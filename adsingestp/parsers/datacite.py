import logging
from collections import OrderedDict

from adsingestp import serializer, utils
from adsingestp.ingest_exceptions import (
    MissingAuthorsException,
    MissingDoiException,
    MissingTitleException,
    WrongSchemaException,
)
from adsingestp.parsers.base import BaseXmlToDictParser

logger = logging.getLogger(__name__)


class DataciteParser(BaseXmlToDictParser):
    """Compatible with DataCite schema versions 3 and 4"""

    DC_SCHEMAS = ["http://datacite.org/schema/kernel-3", "http://datacite.org/schema/kernel-4"]

    author_collaborations_params = {
        "keywords": ["group", "team", "collaboration"],
        "first_author_delimiter": ":",
        "remove_the": False,
        "fix_arXiv_mixed_collaboration_string": False,
    }

    datacite_resourcetype_mapping = {
        "Audiovisual": "misc",
        "Collection": "misc",
        "DataPaper": "misc",
        "Dataset": "misc",
        "Event": "misc",
        "Image": "misc",
        "InteractiveResource": "misc",
        "Model": "misc",
        "PhysicalObject": "misc",
        "Service": "misc",
        "Software": "software",
        "Sound": "misc",
        "Text": "misc",
        "Workflow": "misc",
        "Other": "misc",
    }

    def __init__(self):
        self.base_metadata = {}
        self.input_metadata = None

    def _parse_contrib(self, author=True):
        contribs_out = []
        name_parser = utils.AuthorNames()
        if author:
            contrib_array = self._array(
                self._dict(self.input_metadata.get("creators")).get("creator", [])
            )
        else:
            contrib_array = self._array(
                self._dict(self.input_metadata.get("contributors")).get("contributor", [])
            )
        for c in contrib_array:
            contrib_tmp = {}
            try:
                contrib_tmp["given"] = c["givenName"]
                contrib_tmp["surname"] = c["familyName"]
            except KeyError:
                if author:
                    contrib_name = c.get("creatorName")
                else:
                    contrib_name = c.get("contributorName")
                if type(contrib_name) is OrderedDict:
                    contrib_name = contrib_name.get("#text")
                parsed_name = name_parser.parse(
                    contrib_name, collaborations_params=self.author_collaborations_params
                )
                if len(parsed_name) > 1:
                    logger.warning(
                        "More than one name parsed, can only accept one. Input: %s, output: %s",
                        contrib_name,
                        parsed_name,
                    )
                parsed_name_first = parsed_name[0]
                for key in parsed_name_first.keys():
                    contrib_tmp[key] = parsed_name_first[key]

            contrib_tmp["aff"] = self._array(c.get("affiliation", ""))

            for i in self._array(c.get("nameIdentifier")):
                if "ORCID" == i.get("@nameIdentifierScheme") or "http://orcid.org" == i.get(
                    "@schemeURI"
                ):
                    contrib_tmp["orcid"] = i.get("#text")

            if not author:
                contrib_tmp["role"] = c.get("@contributorType", "")

            contribs_out.append(contrib_tmp)
        if not contribs_out:
            raise MissingAuthorsException("No contributors found for")

        if author:
            self.base_metadata["authors"] = contribs_out
        else:
            self.base_metadata["contributors"] = contribs_out

    def _parse_title_abstract(self):
        titles = {}
        for t in self._array(self._dict(self.input_metadata.get("titles")).get("title", [])):
            title_attr = self._attr(t, "xml:lang", "en")
            # titleType is only present for subtitles and alternate titles, not the primary title
            type_attr = self._attr(t, "titleType", "")
            if not type_attr:
                titles[title_attr.lower()] = self._text(t)
            if type_attr == "Subtitle":
                self.base_metadata["subtitle"] = self._text(t)
        if not titles:
            raise MissingTitleException("No title found")
        # we use the English title as the main one, then add any foreign ones
        # there are several options for "English" in this schema, so check for all of them (lowercase forms)
        en_key = list({"en", "en-us"} & set(titles.keys()))[0]
        self.base_metadata["titleEnglish"] = titles.pop(en_key)
        title_foreign = []
        lang_foreign = []
        for tkey in titles:
            title_foreign.append(titles[tkey])
            lang_foreign.append(tkey)

        # the data model only takes a single foreign-language title; will need to adjust if more are required
        if title_foreign:
            self.base_metadata["titleNative"] = title_foreign[0]
            self.base_metadata["langNative"] = lang_foreign[0]

        # abstract, references are all in the "descriptions" section
        # as of version 3.1 of datacite schema, "References" is not an
        # allowed description type so Lars is shoving the references
        # in a section labeled as "Other" as a json structure
        abstract = None
        for s in self._array(self.input_metadata.get("descriptions", {}).get("description", [])):
            t = s.get("@descriptionType")
            if t == "Abstract":
                abstract = self._text(s)

        self.base_metadata["abstract"] = abstract

    def _parse_publisher(self):
        self.base_metadata["publisher"] = self._text(self.input_metadata.get("publisher", ""))

    def _parse_pubdate(self):
        year = self._text(self.input_metadata.get("publicationYear"))
        if year:
            self.base_metadata["pubdate_electronic"] = year

        dates = []
        for d in self._array(self._dict(self.input_metadata.get("dates")).get("date", [])):
            t = self._attr(d, "dateType")
            dates.append({"type": t, "date": self._text(d)})

        if dates:
            self.base_metadata["pubdate_other"] = dates

    def _parse_keywords(self):
        keywords = []
        for k in self._array(self._dict(self.input_metadata.get("subjects")).get("subject", [])):
            # we are ignoring keyword scheme
            keywords.append({"string": self._text(k), "system": "datacite"})

        self.base_metadata["keywords"] = keywords

    def _parse_ids(self):
        self.base_metadata["ids"] = {}

        if "DOI" != self.input_metadata.get("identifier", {}).get("@identifierType", ""):
            raise MissingDoiException("//identifier['@identifierType'] not DOI!")
        self.base_metadata["ids"]["doi"] = self.input_metadata.get("identifier").get("#text")

        # bibcodes should appear as <alternateIdentifiers>
        pub_ids = []
        for i in self._array(
            self._dict(self.input_metadata.get("alternateIdentifiers")).get(
                "alternateIdentifier", []
            )
        ):
            t = i.get("@alternateIdentifierType")
            pub_ids.append({"attribute": t, "Identifier": self._text(i)})
        self.base_metadata["ids"]["pub-id"] = pub_ids

    def _parse_related_refs(self):
        # related identifiers; bibcodes sometime appear in <relatedIdentifiers>
        related_to = []
        references = []
        for i in self._array(
            self._dict(self.input_metadata.get("relatedIdentifiers")).get("relatedIdentifier", [])
        ):
            rt = i.get("@relationType")
            c = self._text(i)
            if rt == "Cites":
                references.append(c)
            elif rt in utils.related_trans_dict.keys():
                related_to.append({"relationship": utils.related_trans_dict[rt], "id": c})
            else:
                logger.info("RelatedTo type %s not included in translation dictionary", rt)
                related_to.append({"relationship": "related", "id": c})

        self.base_metadata["relatedto"] = related_to
        self.base_metadata["references"] = references

    def _parse_permissions(self):
        is_oa = False
        for i in self._array(self._dict(self.input_metadata.get("rightsList")).get("rights", [])):
            u = self._attr(i, "rightsURI")
            c = self._text(i)
            if u == "info:eu-repo/semantics/openAccess" or c == "Open Access":
                is_oa = True

        self.base_metadata["openAccess"] = is_oa

    def _parse_doctype(self):
        doctype = self.datacite_resourcetype_mapping.get(
            self.input_metadata.get("resourceType", {}).get("@resourceTypeGeneral", None), "misc"
        )
        self.base_metadata["doctype"] = doctype

    def parse(self, text):
        d = self.xmltodict(text)

        # as a convenience, remove the OAI wrapper if it's there
        self.input_metadata = d.get("record", {}).get("metadata", {}).get("resource") or d.get(
            "resource"
        )

        # check for namespace to make sure it's a compatible datacite schema
        schema = self.input_metadata.get("@xmlns")
        if schema not in self.DC_SCHEMAS:
            raise WrongSchemaException('Unexpected XML schema "%s"' % schema)

        self._parse_contrib(author=True)
        self._parse_contrib(author=False)
        self._parse_title_abstract()
        self._parse_publisher()
        self._parse_pubdate()
        self._parse_keywords()
        self._parse_ids()
        self._parse_related_refs()
        self._parse_permissions()
        self._parse_doctype()

        output = serializer.serialize(self.base_metadata, format="OtherXML")

        return output

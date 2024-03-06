import logging
import re

import validators

from adsingestp import utils
from adsingestp.ingest_exceptions import NoSchemaException, XmlLoadException
from adsingestp.parsers.base import BaseBeautifulSoupParser

logger = logging.getLogger(__name__)


class ElsevierParser(BaseBeautifulSoupParser):
    author_collaborations_params = {}

    def __init__(self):
        self.base_metadata = {}
        self.record_header = None
        self.record_meta = None

    def _parse_pub(self):
        if self.record_header.find("prism:publicationName"):
            self.base_metadata["publication"] = self.record_header.find(
                "prism:publicationName"
            ).get_text()

        if self.record_header.find("dct:publisher"):
            self.base_metadata["publisher"] = self.record_header.find("dct:publisher").get_text()

    def _parse_issue(self):
        if self.record_header.find("prism:volume"):
            self.base_metadata["volume"] = self.record_header.find("prism:volume").get_text()
        else:
            # TODO the perl has a branch for books - add that
            pass

        if self.record_header.find("prism:number"):
            self.base_metadata["issue"] = self.record_header.find("prism:number").get_text()

    def _parse_page(self):
        regex_roman = re.compile(r"[ivxIVX]+")
        # TODO the perl has some code for first/last pages that start with L, e, CO, IFC - add that
        # TODO there's also some regex in the perl checking for - or , - check/add that

        if self.record_header.find("prism:startingPage"):
            fpage = self.record_header.find("prism:startingPage").get_text()
            if regex_roman.match(fpage):
                try:
                    fpage = utils.ROMAN_TO_NUMBER[fpage.lower()]
                except KeyError:
                    logger.warning("Can't convert Roman numeral %s to a number", fpage)
            self.base_metadata["page_first"] = fpage

        if self.record_header.find("prism:endingPage"):
            lpage = self.record_header.find("prism:endingPage").get_text()
            if regex_roman.match(lpage):
                try:
                    lpage = utils.ROMAN_TO_NUMBER[lpage.lower()]
                except KeyError:
                    logger.warning("Can't convert Roman numeral %s to a number", lpage)
            self.base_metadata["page_last"] = lpage

        if self.record_meta.find("ce:article-number"):
            self.base_metadata["electronic_id"] = self.record_meta.find(
                "ce:article-number"
            ).get_text()

    def _parse_pubdate(self):
        regex_yyyymmdd = re.compile(
            r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$"
        )  # yyyy-mm-dd
        regex_mmmyy = re.compile(r"^\w+\s\d{4}$")  # month yyyy
        regex_ddmmmyy = re.compile(
            r"^([1-9]|0[1-9]|[12][0-9]|3[01])\s\w+\s\d{4}$"
        )  # d (or dd) month yyyy
        regex_yyyy = re.compile(r"^\d{4}$")  # yyyy

        pubdate = None
        if self.record_header.find("prism:coverDate"):
            pubdate = self.record_header.find("prism:coverDate").get_text()
        elif self.record_header.find("prism:coverDisplayDate"):
            pubdate = self.record_header.find("prism:coverDisplayDate").get_text()

        if pubdate:
            if regex_yyyymmdd.match(pubdate):
                self.base_metadata["pubdate_print"] = pubdate
            elif regex_mmmyy.match(pubdate):
                pubdate_parts = pubdate.split()
                year = pubdate_parts[1]
                month_name = pubdate_parts[0][0:3].lower()
                month = utils.MONTH_TO_NUMBER[month_name]
                self.base_metadata["pubdate_print"] = year + "-" + month + "-00"
            elif regex_ddmmmyy.match(pubdate):
                pubdate_parts = pubdate.split()
                year = pubdate_parts[2]
                month_name = pubdate_parts[1][0:3].lower()
                month = utils.MONTH_TO_NUMBER[month_name]
                day = pubdate_parts[0]
                if len(day) == 1:
                    day = "0" + day
                self.base_metadata["pubdate_print"] = year + "-" + month + "-" + day
            elif regex_yyyy.match(pubdate):
                year = pubdate
                self.base_metadata["pubdate_print"] = year + "-00-00"

    def _parse_edhistory(self):
        # key: xml tag, value: self.base_metadata key
        dates_trans = {
            "ce:date-received": "edhist_rec",
            "ce:date-revised": "edhist_rev",
            "ce:date-accepted": "edhist_acc",
        }

        for date_xml, field in dates_trans.items():
            if self.record_meta.find(date_xml):
                dates = self.record_meta.find_all(date_xml)
                dates_out = []
                for date in dates:
                    # don't assign without at least a year
                    if date.get("year"):
                        month = date.get("month", "00")
                        day = date.get("day", "00")
                        dates_out.append(
                            date.get("year")
                            + "-"
                            + (month if len(month) == 2 else ("0" + month))
                            + "-"
                            + (day if len(day) == 2 else ("0" + day))
                        )
                if date_xml == "ce:date-accepted":
                    # this only accepts a single date, the other two accept a list
                    dates_out = dates_out[0]

                self.base_metadata[field] = dates_out

    def _parse_title_abstract(self):
        if self.record_meta.find("ce:title"):
            self.base_metadata["title"] = self._clean_output(
                self.record_meta.find("ce:title").get_text()
            )
        elif self.record_header.find("dct:title"):
            self.base_metadata["title"] = self._clean_output(
                self.record_header.find("dct:title").get_text()
            )
        elif self.record_meta.find("cd:textfn"):
            self.base_metadata["title"] = self._clean_output(
                self.record_meta.find("cd:textfn").get_text()
            )

        if self.record_meta.find("ce:subtitle"):
            self.base_metadata["subtitle"] = self._clean_output(
                self.record_meta.find("ce:subtitle").get_text()
            )

        if self.record_meta.find("ce:abstract"):
            abstract = ""
            abs_all = self.record_meta.find_all("ce:abstract")
            for abs in abs_all:
                if abs.find("ce:section-title"):
                    if abs.find("ce:section-title").get_text().lower() == "abstract":
                        abs_text_all = abs.find_all("ce:simple-para")
                        abstract = ""  # we've found the real abstract, so reset
                        for abs_text in abs_text_all:
                            abstract = abstract + " " + abs_text.get_text()
                        if abstract:
                            self.base_metadata["abstract"] = abstract
                            break
                    elif abs.find("ce:section-title").get_text().lower() == "highlights":
                        abs_text_all = abs.find_all("ce:para")
                        for abs_text in abs_text_all:
                            abstract = abstract + " " + abs_text.get_text()

            if abstract:
                self.base_metadata["abstract"] = self._clean_output(abstract)

    def _parse_ids(self):
        self.base_metadata["ids"] = {}

        if self.record_header.find("prism:doi"):
            self.base_metadata["ids"]["doi"] = self.record_header.find("prism:doi").get_text()

        if self.record_header.find("prism:issn"):
            # pubtype, issnstring
            self.base_metadata["issn"] = [
                ("not specified", self.record_header.find("prism:issn").get_text())
            ]

        if self.record_header.find("prism:isbn"):
            self.base_metadata["isbn"] = [
                {
                    "type": "not specified",
                    "isbn_str": self.record_header.find("prism:isbn").get_text(),
                }
            ]

        self.base_metadata["ids"]["pub-id"] = []
        if self.record_meta.find("ce:pii"):
            self.base_metadata["ids"]["pub-id"].append(
                {"attribute": "PII", "Identifier": self.record_meta.find("ce:pii").get_text()}
            )

        if self.record_meta.find("prism:issn"):
            # (type, issn) - type is not specified here
            self.base_metadata["issn"] = (
                "default",
                self.record_meta.find("prism:issn").get_text(),
            )

    def _parse_permissions(self):
        self.base_metadata["openAccess"] = {"open": False}

        if (
            self.record_header.find("oa:openAccessStatus")
            and self.record_header.find("oa:openAccessStatus").get_text()
        ):
            if self.record_header.find("oa:openAccessStatus").get_text()[-5:].lower() == "#full":
                self.base_metadata["openAccess"]["open"] = True

        copyright_year = None
        if self.record_meta.find("ce:copyright"):
            copyright_year = self.record_meta.find("ce:copyright").get("year")
        elif self.base_metadata["pubdate_print"]:
            copyright_year = self.base_metadata["pubdate_print"][0:4]
        copyright_stub = "&copy;"
        if copyright_year:
            copyright_stub = copyright_stub + " " + copyright_year

        if self.record_header.find("prism:copyright"):
            self.base_metadata["copyright"] = self.record_header.find("prism:copyright").get_text()
        elif self.record_meta.find("ce:copyright"):
            self.base_metadata["copyright"] = (
                copyright_stub + " " + self.record_meta.find("ce:copyright").get_text()
            )
        else:
            self.base_metadata["copyright"] = (
                copyright_stub + " Elsevier Science B.V. All rights reserved."
            )

    def _parse_authors(self):
        author_list = []

        if self.record_meta.find("ce:author-group"):
            # build affiliations cross-reference dict
            affs_xref_raw = self.record_meta.find("ce:author-group").find_all("ce:affiliation")
            affs_xref = {}
            for aff in affs_xref_raw:
                if aff.find("ce:label"):
                    label = aff.find("ce:label").get_text()
                else:
                    label = "ALLAUTH"

                if aff.find("ce:textfn"):
                    # formatted affiliation string
                    # note: sa:affiliation contains the parsed affiliation - not useful now but may be in the future
                    value = aff.find("ce:textfn").get_text()
                elif aff.find("ce:source-text"):
                    # raw affiliation string
                    value = aff.find("ce:source-text").get_text()
                else:
                    value = ""

                if label == "ALLAUTH":
                    # collect all of the implicit affiliations in a list
                    value_list = affs_xref.get("ALLAUTH", [])
                    value_list.append(value)
                    affs_xref["ALLAUTH"] = value_list
                else:
                    affs_xref[label] = value

            authors_raw = self.record_meta.find("ce:author-group").find_all("ce:author")
            for author in authors_raw:
                author_tmp = {}
                if author.find("ce:surname"):
                    if author.find("ce:given-name"):
                        author_tmp["given"] = author.find("ce:given-name").get_text()
                    author_tmp["surname"] = author.find("ce:surname").get_text()
                elif author.find("ce:given-name") and not author.find("ce:surname"):
                    # In case given-name is present, but no surname is available, put the given name in the surname
                    author_tmp["surname"] = author.find("ce:given-name")

                author_tmp["orcid"] = author.get("orcid", "")

                if (
                    author.find("ce:e-address")
                    and author.find("ce:e-address").get("type", "") == "email"
                ):
                    author_tmp["email"] = author.find("ce:e-address").get_text()

                if author.find("ce:cross-ref") and author.find("ce:cross-ref").find("ce:sup"):
                    affs = []
                    for i in author.find("ce:cross-ref").find_all("ce:sup"):
                        aff_label = i.get_text()
                        # don't append an empty aff
                        if affs_xref.get(aff_label):
                            affs.append(affs_xref[aff_label])
                    author_tmp["aff"] = affs
                elif affs_xref.get("ALLAUTH"):
                    author_tmp["aff"] = affs_xref["ALLAUTH"]

                author_list.append(author_tmp)

        elif self.record_header.find("dct:creator"):
            name_parser = utils.AuthorNames()
            authors_raw = self.record_header.find_all("dct:creator")
            for author in authors_raw:
                author_name_raw = author.get_text()
                parsed_name = name_parser.parse(
                    author_name_raw, collaborations_params=self.author_collaborations_params
                )
                author_list.append(parsed_name)

        if author_list:
            self.base_metadata["authors"] = author_list

    def _parse_keywords(self):
        key_system = "Elsevier"
        if self.record_meta.find("ce:keywords") and self.record_meta.find("ce:keywords").find(
            "ce:section-title"
        ):
            key_system_raw = (
                self.record_meta.find("ce:keywords").find("ce:section-title").get_text()
            )
            if key_system_raw.lower() != "keywords":
                key_system = key_system_raw
        if self.record_meta.find("ce:keyword"):
            keywords = []
            for k in self.record_meta.find_all("ce:keyword"):
                k_text = k.find("ce:text")
                if k_text:
                    keywords.append({"string": k_text.get_text(), "system": key_system})

            self.base_metadata["keywords"] = keywords

    def _parse_references(self):
        if self.record_meta.find("ce:bibliography") and self.record_meta.find(
            "ce:bibliography"
        ).find("sb:reference"):
            references = []
            for ref in self.record_meta.find("ce:bibliography").find_all("sb:reference"):
                # output raw XML for reference service to parse later
                ref_xml = str(ref.extract()).replace("\n", " ")
                references.append(ref_xml)

            self.base_metadata["references"] = references

    def _parse_esources(self):
        links = []
        if self.record_header.find("prism:url"):
            if validators.url(self.record_header.find("prism:url").get_text()):
                links.append(("pub_html", self.record_header.find("prism:url").get_text()))

        self.base_metadata["esources"] = links

    def _find_article_type(self, d):
        article_types = {
            "cja:converted-article": "article",
            "ja:article": "article",
            "ja:simple-article": "article",
            "ja:book-review": "article",
            "ja:exam": "nonarticle",
            "bk:book": "book",
            "bk:chapter": "inbook",
            "bk:simple-chapter": "inbook",
            "bk:examination": "nonarticle",
            "bk:fb-non-chapter": "inbook",
            "bk:glossary": "inbook",
            "bk:index": "inbook",
            "bk:introduction": "inbook",
            "bk:bibliography": "inbook",
        }
        for art_type in article_types.keys():
            if d.find(art_type, None):
                return art_type, article_types[art_type]

    def parse(self, text):
        """
        Parse Elsevier XML into standard JSON format
        :param text: string, contents of XML file
        :return: parsed file contents in JSON format
        """
        try:
            d = self.bsstrtodict(text, parser="lxml-xml")
        except Exception as err:
            raise XmlLoadException(err)

        self.record_header = d.find("rdf:Description")

        article_type, document_enum = self._find_article_type(d)
        self.base_metadata["doctype"] = document_enum
        self.record_meta = d.find(article_type)

        if self.record_meta is None:
            raise NoSchemaException("No Schema Found")

        self._parse_pub()
        self._parse_issue()
        self._parse_page()
        self._parse_title_abstract()
        self._parse_pubdate()
        self._parse_edhistory()
        self._parse_ids()
        self._parse_permissions()
        self._parse_authors()
        self._parse_keywords()
        self._parse_references()
        self._parse_esources()

        self.base_metadata = self._entity_convert(self.base_metadata)

        output = self.format(self.base_metadata, format="OtherXML")

        return output

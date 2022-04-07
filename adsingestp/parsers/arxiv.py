import logging
import re

from adsingestp import serializer, utils
from adsingestp.ingest_exceptions import (
    MissingAuthorsException,
    MissingTitleException,
    NoSchemaException,
    WrongSchemaException,
)
from adsingestp.parsers.base import BaseXmlToDictParser

logger = logging.getLogger(__name__)


class ArxivParser(BaseXmlToDictParser):
    # Dublin Core parser for arXiv

    DUBCORE_SCHEMA = ["http://www.openarchives.org/OAI/2.0/oai_dc/"]

    author_collaborations_params = {
        "keywords": ["group", "team", "collaboration"],
        "first_author_delimiter": ":",
        "remove_the": False,
        "fix_arXiv_mixed_collaboration_string": True,
    }

    def __init__(self):
        self.base_metadata = {}
        self.input_header = None
        self.input_metadata = None

    def get_tag(self, r, tag):
        value = []
        for s in self._array(r.get(tag, [])):
            value.append(self._text(s))

        return value

    def clean_output(self, input):
        input = input.replace("\n", " ")
        output = re.sub(r"\s+", r" ", input)

        return output

    def _parse_ids(self):
        ids = self.input_header.get("identifier", "")
        id_array = ids.split(":")
        arxiv_id = id_array[-1]

        # TODO what should the key on this actually be?
        self.base_metadata["publication"] = "eprint arXiv:" + arxiv_id

        self.base_metadata["ids"] = {"preprint": {}}

        self.base_metadata["ids"]["preprint"]["source"] = "arXiv"
        self.base_metadata["ids"]["preprint"]["id"] = arxiv_id

        dc_ids = self.get_tag(self.input_metadata, "dc:identifier")
        for d in dc_ids:
            if "doi:" in d:
                self.base_metadata["ids"]["doi"] = d.replace("doi:", "")

    def _parse_title(self):
        title_array = self.get_tag(self.input_metadata, "dc:title")
        if title_array:
            if len(title_array) == 1:
                self.base_metadata["title"] = self.clean_output(title_array[0])
            else:
                self.base_metadata["title"] = self.clean_output(": ".join(title_array))
        else:
            raise MissingTitleException("No title found")

    def _parse_author(self):
        authors_out = []
        name_parser = utils.AuthorNames()

        author_array = self.get_tag(self.input_metadata, "dc:creator")
        for a in author_array:
            author_tmp = {}
            parsed_name = name_parser.parse(
                a, collaborations_params=self.author_collaborations_params
            )
            if len(parsed_name) > 1:
                logger.warning(
                    "More than one name parsed, can only accept one. Input: %s, output: %s",
                    a,
                    parsed_name,
                )
            parsed_name_first = parsed_name[0]
            for key in parsed_name_first.keys():
                author_tmp[key] = parsed_name_first[key]

            authors_out.append(author_tmp)

        if not authors_out:
            raise MissingAuthorsException("No contributors found for")

        self.base_metadata["authors"] = authors_out

    def _parse_pubdate(self):
        if self.get_tag(self.input_metadata, "dc:date"):
            self.base_metadata["pubdate_electronic"] = self.get_tag(
                self.input_metadata, "dc:date"
            )[0]

    def _parse_abstract(self):
        desc_array = self.get_tag(self.input_metadata, "dc:description")
        # for arXiv.org, only 'dc:description'[0] is the abstract, the rest are comments
        if desc_array:
            self.base_metadata["abstract"] = self.clean_output(desc_array.pop(0))

        if desc_array:
            comments_out = []
            for d in desc_array:
                comments_out.append({"origin": "arxiv", "text": self.clean_output(d)})

            self.base_metadata["comments"] = comments_out

    def _parse_keywords(self):
        keywords_array = self.get_tag(self.input_metadata, "dc:subject")

        if keywords_array:
            keywords_out = []
            for k in keywords_array:
                keywords_out.append({"system": "arxiv", "string": k})
            self.base_metadata["keywords"] = keywords_out

    def parse(self, text):
        d = self.xmltodict(text)

        self.input_header = d.get("record", {}).get("header", {})
        self.input_metadata = d.get("record", {}).get("metadata", {}).get("oai_dc:dc", {})

        schema_spec = []
        for s in self._array(self.input_metadata["@xmlns:oai_dc"]):
            schema_spec.append(self._text(s))
        if len(schema_spec) == 0:
            raise NoSchemaException("Unknown record schema.")
        elif schema_spec[0] not in self.DUBCORE_SCHEMA:
            raise WrongSchemaException("Wrong schema.")

        self._parse_ids()
        self._parse_title()
        self._parse_author()
        self._parse_pubdate()
        self._parse_abstract()
        self._parse_keywords()

        output = serializer.serialize(self.base_metadata, format="OtherXML")

        return output

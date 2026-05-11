# IEEE parser for metadata-only (not full-text) conference XML files
# /proj/ads_abstracts/sources/IEEE/IEEEcnf/MetadataXML/*

# Parser assumes XML structured per:
# IEEE XML documentation v.5.14, July 2024
# https://www.ieee.org/content/dam/ieee-org/ieee/web/org/pubs/ieee-data-delivery-documentation.pdf

import logging
import re

from adsingestp import utils
from adsingestp.ingest_exceptions import XmlLoadException
from adsingestp.parsers.base import BaseBeautifulSoupParser

logger = logging.getLogger(__name__)

orcid_format = re.compile(r"(\d{4}-){3}\d{3}(\d|X)")


class IEEEParser(BaseBeautifulSoupParser):
    def __init__(self):
        super(BaseBeautifulSoupParser, self).__init__()
        self.base_metadata = {}
        self.publication = None
        self.publicationinfo = None
        self.volumeinfo = None
        self.article = None

    def _parse_ids(self):
        # ISSN
        self.base_metadata["issn"] = []
        for i in self.publicationinfo.find_all("issn"):
            self.base_metadata["issn"].append((i["mediatype"], i.get_text()))

        # IDs
        self.base_metadata["ids"] = {}

        # DOI for article
        if self.article.find("articledoi"):
            self.base_metadata["ids"]["doi"] = self.article.find("articledoi").get_text()

        # DOI for Conference
        self.base_metadata["ids"]["pub-id"] = []
        if self.publicationinfo.find("publicationdoi"):
            self.base_metadata["ids"]["pub-id"].append(
                {
                    "attribute": "doi",
                    "Identifier": self.publicationinfo.find("publicationdoi").get_text(),
                }
            )

        # IEEE unique ID for article
        if self.article.find("articleinfo"):
            articleinfo = self.article.find("articleinfo")
            # Article sequence number
            if articleinfo.find("articleseqnum"):
                articleid = articleinfo.find("articleseqnum").get_text()
                self.base_metadata["electronic_id"] = articleid
                # This next bit probably unnecessary? Unlikely to be >9999 articles in a conf proceedings
                #if len(articleid) > 4:
                #    self.base_metadata["page_first"] = articleid[-4:]  # rightmost 4 chars
                #else:
                #    self.base_metadata["page_first"] = articleid

    def _parse_pub(self):
        # Conference name
        if self.publication.find("title"):
            t = self.publication.find("title")
            title = self._clean_output(
                self._detag(t, self.HTML_TAGSET["title"]).strip()
            )
        self.base_metadata["publication"] = title
        # %X is removed in postprocessing; used for bibstem lookup
        #self.base_metadata["comments"] = []
        #self.base_metadata["comments"].append({"text": title})

        # Conference volume number
        if self.volumeinfo:
            if self.volumeinfo.find("volumenum"):
                self.base_metadata["volume"] = self.volumeinfo.find("volumenum").text
            else:
                self.base_metadata["volume"] = ""

        # Conferences don't have an issue number

        """
        # Conference abbreviation for creating bibstem
        self.base_metadata["comments"] = []
        ieeeabbrev = self.publicationinfo.find("ieeeabbrev").text or ""
        if ieeeabbrev:
            cleanabbrev = re.sub(r"[^A-Za-z]", "", ieeeabbrev)  # delete non-alpha chars
            if len(cleanabbrev) >= 4:  # rightmost 4 chars
                bibstem = cleanabbrev[-4:]
            else:
                bibstem = cleanabbrev.ljust(4, '.')  # pad on the right if <4 chars
            self.base_metadata["comments"].append({"text": bibstem})
        else:
            self.base_metadata["comments"].append({"text": "ieee."})
        """

        # Conference location
        if self.publicationinfo.find("conflocation") is not None:
            confloc = self.publicationinfo.find("conflocation").text
        self.base_metadata["conf_location"] = confloc

        # Conference dates
        confdate = ""
        if self.publicationinfo.find("confdate", {"confdatetype": "End"}) is not None:
            confend = self.publicationinfo.find("confdate", {"confdatetype": "End"}) 
            end_year = confend.find("year").text if confend.find("year") else None
            end_month = confend.find("month").text if confend.find("month") else None
            end_day = confend.find("day").text if confend.find("day") else None
        if self.publicationinfo.find("confdate", {"confdatetype": "Start"}) is not None:
            confstart = self.publicationinfo.find("confdate", {"confdatetype": "Start"})
            start_year = confstart.find("year").text if confstart.find("year") else None
            start_month = confstart.find("month").text if confstart.find("month") else None
            start_day = confstart.find("day").text if confstart.find("day") else None
            confdate = f"{start_day} {start_month} {start_year} - {end_day} {end_month} {end_year}"
            self.base_metadata["conf_date"] = confdate

        # Conference topics
        # These are the same as the browse topics in IEEE Xplore
        # See: https://ieeexplore.ieee.org/browse/conferences/topic 
        # Used to assign %W Collection
        # The ingest data model does not contain a JSON object in which to pass the collection

        # Pass all values of pubtopicalbrowse in comments
        # %X is removed in postprocessing; used for bibstem lookup
        self.base_metadata["comments"] = []
        if self.publicationinfo.find("pubtopicalbrowseset") is not None:
            pubtopicset = self.publicationinfo.find("pubtopicalbrowseset")
            for pubtopic in pubtopicset.find_all("pubtopicalbrowse"):
                topic = pubtopic.get_text(strip=True)
                self.base_metadata["comments"].append({"text": topic})
        '''
        # TO DO: Implement this as a better method
        # Pass topics as keywords and parse out in postprocessing
        keywords = []
        for topicset in self.publicationinfo.find_all("pubtopicalbrowseset"):
            for topic in topicset.find_all("pubtopicalbrowse"):
                if topic.string:
                    keywords.append(
                        {
                            "system": "pubtopicalbrowse",
                            "string": self._clean_output(topic.string.strip()),
                        }
                    )
        '''

        # TO DO: append confDates & confLocation to %J
        if confdate:
            self.base_metadata["publication"] = f"{title}, {confdate}, {confloc}"
        else:
            self.base_metadata["publication"] = f"{title}, {confloc}"

    def _parse_page(self):
        if self.article.find("artpagenums"):
            startpage = self.article.find("artpagenums").get("startpage")
            endpage = self.article.find("artpagenums").get("endpage")
            # Using articleid as page_first, to avoid duplicate bibcodes
            # See IEEE unique ID section above
            # Because multiple papers in conferences use startpage = 1
            #self.base_metadata["page_first"] = self._detag(startpage, []) if startpage else None
            #self.base_metadata["page_last"] = self._detag(endpage, []) if endpage else None

    def _parse_pubdate(self):
        # Look for publication dates in article section
        for date in self.article.find_all("date"):
            date_type = date.get("datetype", "")

            # Get year, month, day values
            if date.find("year"):
                year = date.find("year").get_text()
            else:
                year = "0000"
            self.year = year

            if date.find("month"):
                month_raw = date.find("month").get_text()
                if month_raw.isdigit():
                    month = month_raw
                else:
                    month_name = month_raw[0:3].lower()
                    month = utils.MONTH_TO_NUMBER[month_name]
            else:
                month_raw = "00"
                month = "00"

            if date.find("day"):
                day = date.find("day").get_text()
            else:
                day = "00"

            # Format date string
            pubdate = year + "-" + month + "-" + day

            if date_type == "OriginalPub":
                self.base_metadata["pubdate_print"] = pubdate
            elif date_type == "ePub":
                self.base_metadata["pubdate_electronic"] = pubdate

    def _parse_title_abstract(self):
        # Parse title from article section
        if self.article.find("title"):
            self.base_metadata["title"] = self._clean_output(
                self._detag(self.article.find("title"), self.HTML_TAGSET["title"]).strip()
            )

        # Parse abstract from articleinfo section
        if self.article.find("articleinfo"):
            for abstract in self.article.find("articleinfo").find_all("abstract"):
                if abstract.get("abstracttype") == "Regular":
                    self.base_metadata["abstract"] = self._clean_output(
                        self._detag(abstract, self.HTML_TAGSET["abstract"]).strip()
                    )

    def _parse_permissions(self):
        # Check for open-access and permissions information
        if self.article.find("articleinfo"):
            articleinfo = self.article.find("articleinfo")

            # Copyright holder and year for article
            if articleinfo.find("articlecopyright"):
                copyright = articleinfo.find("articlecopyright")
                copyright_holder = self._clean_output(copyright.get_text())
                if copyright_holder == "":
                    copyright_holder = "IEEE"

                copyright_year = copyright.get("year", "")
                if copyright_year == "0":
                    copyright_year = self.year

                # Sadly <article_copyright_statement> doesn't seem to exist in IEEE conference metadata
                if articleinfo.find("article_copyright_statement"):
                    copyright_statement = self._detag(
                        articleinfo.find("article_copyright_statement").get_text(),
                        self.HTML_TAGSET["license"],
                    )
                else:
                    copyright_statement = ""

            # Copyright holder and year for publication is in <copyrightgroup>

                # Format copyright string
                copyright_text = (
                    copyright_year + " " + copyright_holder + ". " + copyright_statement
                )
                self.base_metadata["copyright"] = copyright_text

            # Check if open access is given as "T" (true)
            if articleinfo.find("articleopenaccess"):
                if articleinfo.find("articleopenaccess").get_text() == "T":
                    self.base_metadata.setdefault("openAccess", {}).setdefault("open", True)

    def _parse_authors(self):
        # Parse authors from articleinfo section
        if self.article.find("articleinfo"):
            articleinfo = self.article.find("articleinfo")
            author_list = []

            # Get all authors from authorgroup
            if articleinfo.find("authorgroup"):
                for author in articleinfo.find("authorgroup").find_all("author"):
                    author_tmp = {}

                    # Get author name components
                    if author.find("firstname"):
                        author_tmp["given"] = self._clean_output(
                            author.find("firstname").get_text()
                        )
                    if author.find("surname"):
                        author_tmp["surname"] = self._clean_output(
                            author.find("surname").get_text()
                        )

                    # Get author affiliation
                    if author.find("affiliation"):
                        author_tmp["aff"] = [
                            self._clean_output(author.find("affiliation").get_text())
                        ]
                        author_tmp["xaff"] = []

                    # Get author email
                    if author.find("email"):
                        author_tmp["email"] = self._clean_output(author.find("email").get_text())

                    # Get author ORCID if present
                    if author.find("orcid"):
                        author_tmp["orcid"] = author.find("orcid").get_text()

                    # Check if author is corresponding author
                    if author.get("role") == "corresponding":
                        author_tmp["corresp"] = True

                    author_list.append(author_tmp)

            if author_list:
                self.base_metadata["authors"] = author_list

    def _parse_keywords(self):
        # Parse IEEE keywords from keywordset elements
        keywords = []

        # Handle all keyword types in <articleinfo>
        # IEEE and IEEEFree keywordtype
        # DOE & PACS don't exist in this collection?
        for keywordset in self.article.find_all("keywordset"):
            keyword_type = keywordset.get("keywordtype", "")

            for keyword in keywordset.find_all("keywordterm"):
                if keyword.string:
                    keywords.append(
                        {
                            "system": keyword_type,
                            "string": self._clean_output(keyword.string.strip()),
                        }
                    )

        '''
        # <pubtopicalbrowse> = topic browse categories in IEEE Xplore
        # NOTE: Some values of <pubtopicalbrowse> contain commas
        # How to deal with this in %K ?

        # Get all pub-level topics in <publicationinfo>
        for pubtopicset in self.publicationinfo.find_all("pubtopicalbrowseset"):
            for pubtopic in pubtopicset.find_all("pubtopicalbrowse"):
                keywords.append(
                    {
                        "system": "XploreTopic",
                        "string": self._clean_output(pubtopic.string.strip()),
                    }
                )
        '''

        if keywords:
            self.base_metadata["keywords"] = keywords

    def _parse_references(self):
        # IEEE conferences do not provide references
        # Check value of <articlereferenceflag>
        references = []
        if self.article.find("references"):
            for ref in self.article.find_all("reference"):
                # output raw XML for reference service to parse later
                ref_xml = str(ref.extract()).replace("\n", " ").replace("\xa0", " ")
                references.append(ref_xml)

            self.base_metadata["references"] = references

    def _parse_funding(self):
        funding = []

        articleinfo = self.article.find("articleinfo")

        if articleinfo.find("fundrefgrp"):
            funding_sections = articleinfo.find("fundrefgrp").find_all("fundref", [])

            for funding_section in funding_sections:
                funder = {}

                # Get funder name
                funder_name = funding_section.find("funder_name")
                if funder_name:
                    funder.setdefault("agencyname", self._clean_output(funder_name.get_text()))

                # Get award/grant number(s)
                award_nums = funding_section.find_all("grant_number")
                if award_nums:
                    # Join multiple award numbers with comma if present
                    awards = [self._clean_output(award.get_text()) for award in award_nums]
                    funder.setdefault("awardnumber", ", ".join(awards))

                if funder:
                    funding.append(funder)

        if funding:
            self.base_metadata["funding"] = funding


    # Parse IEEE XML into standard JSON format
    # :param text: string, contents of XML file
    # :return: parsed file contents in JSON format
    def parse(self, text):
        try:
            d = self.bsstrtodict(text, parser="lxml-xml")
        except Exception as err:
            raise XmlLoadException(err)

        if d.find("publication", None):
            self.publication = d.find("publication")

            if self.publication.find("publicationinfo", None):
                self.publicationinfo = self.publication.find("publicationinfo")

            if self.publication.find("volume", None):
                self.volumeinfo = self.publication.find("volume").find("volumeinfo", None)
                self.article = self.publication.find("volume").find("article", None)

        self._parse_ids()
        self._parse_pub()
        self._parse_page()
        self._parse_pubdate()
        self._parse_title_abstract()
        self._parse_permissions()
        self._parse_authors()
        self._parse_keywords()
        #self._parse_references()
        self._parse_funding()

        output = self.format(self.base_metadata, format="IEEE")

        return output

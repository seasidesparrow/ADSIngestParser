import html
import logging
import re

from adsingestp import utils
from adsingestp.ingest_exceptions import XmlLoadException
from adsingestp.parsers.base import BaseBeautifulSoupParser
from adsingestp.parsers.jats import JATSParser, JATSAffils

logger = logging.getLogger(__name__)

orcid_format = re.compile(r"(\d{4}-){3}\d{3}(\d|X)")


class IEEEJournalParser(JATSParser):
    def __init__(self):
        super(JATSParser, self).__init__()
        self.base_metadata = {}
        self.back_meta = None
        self.article_meta = None
        self.journal_meta = None
        self.isErratum = False

    def parse(self, text, bsparser='lxml'):
        """
        Parse JATS XML into standard JSON format
        :param text: string, contents of XML file
        :return: parsed file contents in JSON format
        """
        # IEEE needs to have HTML entities converted to unicode to
        # deal with Turkish charset translation issues
        try:
            text = html.unescape(text)
            d = self.bsstrtodict(text, parser=bsparser)
        except Exception as err:
            raise XmlLoadException(err)
            
        document = d.article
        # front_meta = document.front
        try:
            front_meta = document.front
        except Exception as err:
            raise XmlLoadException("No front matter found, stopping: %s" % err)
        self.back_meta = document.back

        self.article_meta = front_meta.find("article-meta")
        self.journal_meta = front_meta.find("journal-meta")

        # parse individual pieces
        self._parse_title_abstract()
        self._parse_author()
        self._parse_copyright()
        self._parse_keywords()

        # Volume:
        volume = self.article_meta.volume
        if volume:
            self.base_metadata["volume"] = self._detag(volume, [])

        # Issue:
        issue = self.article_meta.issue
        if issue:
            self.base_metadata["issue"] = self._detag(issue, [])

        if self.article_meta.find("conference"):
            self._parse_conference()

        self._parse_pub()
        self._parse_related()
        self._parse_ids()
        self._parse_pubdate()
        self._parse_edhistory()
        self._parse_permissions()
        self._parse_page()
        self._parse_esources()
        self._parse_funding()

        self._parse_references()

        #self.base_metadata = self._entity_convert(self.base_metadata)

        output = self.format(self.base_metadata, format="IEEE")

        return output


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
            # <amsid> is an IEEE-generated unique ID; also used for article, author, publication, etc.
            #if articleinfo.find("amsid"):
            #    articleid = articleinfo.find("amsid").get_text()
            # article sequence number
            if articleinfo.find("articleseqnum"):
                articleid = articleinfo.find("articleseqnum").get_text()
                self.base_metadata["electronic_id"] = articleid
                #if len(articleid) > 4:
                #    self.base_metadata["page_first"] = articleid[-4:]  # rightmost 4 chars
                #else:
                #    self.base_metadata["page_first"] = articleid

    def _parse_pub(self):
        # Conference name
        if self.publication.find("title"):
            t = self.publication.find("title")
            #self.base_metadata["publication"] = self._clean_output(
            title = self._clean_output(
                self._detag(t, self.HTML_TAGSET["title"]).strip()
            )
            #title = str(title)
        self.base_metadata["publication"] = title

        # Conference volume number
        if self.volumeinfo:
            if self.volumeinfo.find("volumenum"):
                self.base_metadata["volume"] = self.volumeinfo.find("volumenum").text
            else:
                self.base_metadata["volume"] = ""
        # Conferences don't have an issue number

        # Conference abbreviation
        self.base_metadata["comments"] = []
        if self.publicationinfo.find("ieeeabbrev"):
            confabbrev = self.publicationinfo.find("ieeeabbrev").text
            confabbrev = confabbrev[:5].ljust(5, '.')  # leftmost 5 chars, or pad if <5
            #self.base_metadata["comments"]["text"] = confabbrev
            self.base_metadata["comments"].append(
                {"text":confabbrev}
            )

        # Conference location
        confloc = ""
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

        # Shouldn't be necessary to string these together like this?
        # Why are base_metadata["conf_date"] & ["conf_location"] not being picked up by data model?
        if confdate and confloc and title:
            self.base_metadata["publication"] = f"{title}, {confdate}, {confloc}"
        else:
            self.base_metadata["publication"] = f"{title}, {confloc}"

    def _parse_page(self):
        if self.article.find("artpagenums"):
            startpage = self.article.find("artpagenums").get("startpage")
            endpage = self.article.find("artpagenums").get("endpage")
            # Using articleid as page_first, to avoid duplicate bibcodes
            # multiple papers in conferences use startpage = 1
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

            # Assign to appropriate metadata field based on date type
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

            # Get copyright holder and year
            if articleinfo.find("articlecopyright"):
                copyright = articleinfo.find("articlecopyright")
                copyright_holder = self._clean_output(copyright.get_text())
                copyright_year = copyright.get("year", "")
                if articleinfo.find("article_copyright_statement"):
                    copyright_statement = self._detag(
                        articleinfo.find("article_copyright_statement").get_text(),
                        self.HTML_TAGSET["license"],
                    )
                else:
                    copyright_statement = ""

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

        # Handle both IEEE and IEEEFree keyword types
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

                # Get award/grant numbers
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

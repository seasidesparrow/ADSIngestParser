# Springer parser for metadata-only (not full-text) book XML files
# /proj/ads_abstracts/sources/SPRINGER/files/*
# /proj/ads_abstracts/sources/SPRINGER/files.done/*

# See BITS 2.2 tag library:
# https://jats.nlm.nih.gov/extensions/bits/tag-library/2.2/

import sys
import xml.etree.ElementTree as ET
import logging
import re

from adsingestp import utils
from adsingestp.ingest_exceptions import XmlLoadException
from adsingestp.parsers.base import BaseBeautifulSoupParser
from collections import OrderedDict
from ordered_set import OrderedSet

logger = logging.getLogger(__name__)

orcid_format = re.compile(r"(\d{4}-){3}\d{3}(\d|X)")


class SpringerParser(BaseBeautifulSoupParser):
    def __init__(self):
        super(BaseBeautifulSoupParser, self).__init__()
        self.base_metadata = {}
        self.collectionmeta = None
        self.book = None
        self.bookmeta = None
        self.bookpartwrapper = None
        self.bookpart = None
        self.bookpartmeta = None
        self.frontmatter = None
        self.backmatter = None
        self.contenttype = None
        self.back = None  # <body> <book-part> <back>


    def _parse_abstract(self):
    # Springer uses the id attribute for <abstract>, e.g.,
    # <abstract xml:lang="en" id="Abs1" specific-use="web-only">
    # <abstract id="Abs1_5" xml:lang="en">
    # So far I haven't seen >1 abstract per XML file but find_all just in case

    # TO DO: Store multiple abstracts. Currently this overwrites.

        if self.bookpartmeta is None:
            return

        if self.bookpartmeta.find("abstract"):
            for p in self.bookpartmeta.find("abstract").find_all("p"):
                abstract = p.text.strip()
                self.base_metadata["abstract"] = abstract


    def _parse_authors(self):
        # If book is part of a series
        # Ignore Series Editors & affiliations
        # <collection-meta> <contrib-group>

        contrib_affil = JATSAffils()

        # If manuscript, author(s) are for the book
        if self.contenttype == "manuscript":
            author_meta = self.bookmeta
            au_output_dict = contrib_affil.parse(author_meta)
            authors = au_output_dict.get("authors") or []

        # If edited vol, author(s) are for the chapters
        #   <book-part-meta> <contrib-group>
        # and editor(s) are for the book
        #   <book-meta> <contrib-group>
        elif self.contenttype == "edited":
            editor_meta = self.bookmeta
            ed_output_dict = contrib_affil.parse(editor_meta)
            editors = ed_output_dict.get("editors") or []

            if editors:
                for ed in editors:
                    if ed.get("given-names"):
                        ed["given"] = " ".join(ed["given"].split())
                    if ed.get("surname"):
                        ed["surname"] = " ".join(ed["surname"].split())
                self.base_metadata["editors"] = editors

            author_meta = self.bookpartmeta
            au_output_dict = contrib_affil.parse(author_meta)
            authors = au_output_dict.get("authors") or []

            # If BookFrontMatter or BookBackMatter, treat editor(s) as author(s)
            if self.frontmatter or self.backmatter:
                author_meta = self.bookmeta
                au_output_dict = contrib_affil.parse(author_meta)
                authors = au_output_dict.get("authors") or []

        if authors:
            for auth in authors:
                if auth.get("given-names"):
                    auth["given"] = " ".join(auth["given"].split())
                if auth.get("surname"):
                    auth["surname"] = " ".join(auth["surname"].split())
            self.base_metadata["authors"] = authors


    def _parse_collection(self):
        # Pass <book-meta> <custom-meta-group> <custom-meta> <meta-name>book-subject-primary
        # and <meta-name>book-subject-secondary in %X
        # Removed in postprocessing; used to create %W
        # TO DO: Change as needed if Collection is ever added to the ingest data model
        self.base_metadata["comments"] = []
        su_primary = None
        su_secondary = []
        subjects = []

        if self.bookmeta.find("custom-meta-group"):
            cm_group = self.bookmeta.find("custom-meta-group")

        for cm in cm_group.find_all("custom-meta"):
            meta_name = cm.find("meta-name").get_text(strip=True)
            meta_value = cm.find("meta-value").get_text(strip=True)

            # Keeping these separate in case we want to use primary but not secondary
            if meta_name == "book-subject-primary":
                su_primary = meta_value
                subjects.append(meta_value)
            elif meta_name == "book-subject-secondary":
                su_secondary.append(meta_value)
                subjects.append(meta_value)

        if subjects:
            self.base_metadata["comments"].append({"text": "; ".join(subjects)})


    def _parse_ids(self):
        self.base_metadata["ids"] = {}

        # Get book DOI for manuscripts
        if self.contenttype == "manuscript":
            if self.bookmeta.find("book-id", {"book-id-type": "doi"}):
                self.base_metadata["ids"]["doi"] = self.bookmeta.find("book-id", {"book-id-type": "doi"}).get_text(strip=True)

        # Get chapter DOI for edited vols
        # TO DO: Also get book DOI for edited vols?
        if self.contenttype == "edited":
            if self.bookpartmeta.find("book-part-id", {"book-part-id-type": "doi"}):
                self.base_metadata["ids"]["doi"] = self.bookpartmeta.find("book-part-id", {"book-part-id-type": "doi"}).get_text(strip=True)

        # Handle both print & electronic ISBNs
        # <isbn content-type="[ppub or epub]">
        isbn_all = self.bookmeta.find_all("isbn")
        isbns = []
        for i in isbn_all:
            content_type = None
            if i.get("content-type", ""):
                content_type = i.get("content-type")
            isbns.append({"type": content_type, "isbn_str": self._detag(i, [])})
        self.base_metadata["isbn"] = isbns

        # Handle both print & electronic ISSNs
        # <issn publication-format="[print or electronic]">
        # Only series have ISSNs
        if self.collectionmeta:
            issn_all = self.collectionmeta.find_all("issn")
            issns = []
            for i in issn_all:
                if i.get("publication-format", ""):
                    content_type = i.get("publication-format")
                issns.append({content_type, self._detag(i, [])})
            self.base_metadata["issn"] = issns


    def _parse_keywords(self):
        # Not all Springer XML contains keywords
        # Only use keywords in <book-part-meta>
        # BITS allows keywords to be contained in <book-meta> or <collection-meta>
        # but Springer books doesn't seem to do that?
        # BITS also allows <kwd-group> to contain <compound-kwd> & <nested-kwd>
        # but Springer books doesn't seem to do that either?

        keywords = []
        kwd_groups = []

        # BookFrontMatter or BookBackMatter do not contain keywords
        if self.frontmatter or self.backmatter:
            return

        kwd_groups = self.bookpartmeta.find_all("kwd-group")

        # Handle multiple <kwd-group>s
        # BITS allows multiple keyword groups
        # but Springer books doesn't seem to do that either?
        for kwd_group in kwd_groups:
            kwd_type = kwd_group.get("kwd-group-type", "")

            for kwd in kwd_group.find_all("kwd"):
                keyword = kwd.get_text(strip=True)
                if keyword:
                    keywords.append(
                        {
                            "system": kwd_type,
                            "string": self._clean_output(keyword),
                        }
                    )
        if keywords:
            self.base_metadata["keywords"] = keywords


    def _parse_page(self):
        fpage = None
        e_id = None
        lpage = None
        pagerange = None

        if self.contenttype == "edited":
            fpage = self.bookpartmeta.find("fpage")
            e_id = self.bookpartmeta.find("elocation-id")
            lpage = self.bookpartmeta.find("lpage")
            pagerange = self.bookpartmeta.find("page-range")

        # Don't want page number in bibcode for book-level record
        """
        elif self.contenttype == "manuscript":
            fpage = self.bookpartmeta.find("fpage")
            e_id = self.bookpartmeta.find("elocation-id")
            lpage = self.bookpartmeta.find("lpage")
            pagerange = self.bookpartmeta.find("page-range")
        """

        if fpage:
            self.base_metadata["page_first"] = self._detag(fpage, [])

        if e_id:
            self.base_metadata["electronic_id"] = self._detag(e_id, [])

        if lpage == fpage:
            lpage = None
        if lpage:
            self.base_metadata["page_last"] = self._detag(lpage, [])

        if pagerange:
            self.base_metadata["numpages"] = self._detag(pagerange, [])
        elif fpage and lpage:  # Construct page range
            self.base_metadata["page_range"] = (
                self._detag(fpage, []) + "-" + (self._detag(lpage, []))
            )
        else:
            self.base_metadata["page_range"] = fpage

        # <book-page-count> is only for whole book


    def _parse_permissions(self):
        # <permissions> appears in both <book-meta> and <book-part-meta>
        # Use only <book-meta>
        if self.bookmeta.find("permissions"):
            permissions = self.bookmeta.find("permissions")

            copyright_year = permissions.find("copyright-year")
            copyright_holder = permissions.find("copyright-holder")

            # <copyright-statement content-type="compact"> if exists
            # else <copyright-statement>
            compact_cs = permissions.find("copyright-statement", attrs={"content-type": "compact"})
            if compact_cs is not None:
                copyright_statement = compact_cs.get_text(strip=True)
            else:
                cs = permissions.find("copyright-statement")
                copyright_statement = cs.get_text(strip=True) if cs else None

            self.base_metadata["copyright"] = copyright_statement

            # the <license> license-type attribute is only used ="open-access"
            licenses = permissions.find_all("license")
            for lic in licenses:
                if (lic.get("license-type", None) == "open-access"):
                    self.base_metadata.setdefault("openAccess", {}).setdefault("open", True)
                if lic.find("license-p"):
                    license_text = lic.find("license-p")
                    if license_text:
                        self.base_metadata.setdefault("openAccess", {}).setdefault(
                            "license",
                            self._detag(
                                license_text.get_text(), self.HTML_TAGSET["license"]
                            ).strip(),
                        )
                        license_uri = license_text.find("ext-link")
                        if license_uri:
                            if license_uri.get("xlink:href", None):
                                license_uri_value = license_uri.get("xlink:href", None)
                                self.base_metadata.setdefault("openAccess", {}).setdefault(
                                    "licenseURL", self._detag(license_uri_value, [])
                                )


    def _parse_pubdate(self):
        if self.bookmeta.find("permissions").find("copyright-year"):
            self.base_metadata["pubdate_print"] = self.bookmeta.find("permissions").find("copyright-year").get_text()


    # Manuscript: refs are in backmatter
    # Edited volume: refs are in chapters
    def _parse_references(self):
        if self.back is not None:
            ref_list_text = []
            if self.back.find("ref-list"):
                ref_results = self.back.find("ref-list").find_all("ref")
            else:
                ref_results = []
            for r in ref_results:
                # output raw XML for reference service to parse later
                s = str(r.extract()).replace("\n", " ").replace("\xa0", " ")
                ref_list_text.append(s)
            self.base_metadata["references"] = ref_list_text


    def _parse_title(self):
        # 4 possible titles:
        # Series title: <collection-meta collection-type="series"> <title-group> <title>
        # Subseries title: <collection-meta collection-type="subseries"> <title-group> <title>
        # Book title: <book-meta> <book-title-group> <book-title> & <subtitle>
        # Chapter title: <book-part> <book-part-meta> <title-group> <title> (no subtitle)

        # If book is part of a series, get series title
        # Ignore subseries title, if one exists
        if self.collectionmeta:
            for cm in self.collectionmeta.find_all("collection-meta"):
                ctype = cm.get("collection-type")
                if ctype not in ("series", "subseries"):
                    continue

                ti = cm.find("title-group").find("title").get_text(strip=True)

                if ctype == "series":
                    series_title = ti
                    self.base_metadata["series_title"] = series_title
                #elif ctype == "subseries":
                #    subseries_title = ti
                #    self.base_metadata["series_title"] = series_title + ": " + subseries_title

            # Volume number in series is in <book-meta>
            if self.bookmeta.find("book-volume-number"):
                self.base_metadata["volume"] = self.bookmeta.find("book-volume-number").get_text(strip=True)

        # Get book title & subtitle
        if self.bookmeta.find("book-title-group").find("book-title"):
            book_title = self.bookmeta.find("book-title-group").find("book-title").get_text()

        if self.bookmeta.find("book-title-group").find("subtitle"):
            book_subtitle = self.bookmeta.find("book-title-group").find("subtitle").get_text()
        else:
            book_subtitle = None

        # If book type is manuscript, title = book title
        self.base_metadata["title"] = book_title
        self.base_metadata["subtitle"] = book_subtitle

        # BookFrontMatter & BookBackMatter contain title in <book-meta> even if edited vol
        if self.frontmatter or self.backmatter:
            return

        # If book type is edited volume, title = chapter title
        # and publication = book title
        if self.contenttype == "edited":
            if self.bookpartmeta.find("title-group").find("title"):
                chapter_title = self.bookpartmeta.find("title-group").find("title").get_text()

            self.base_metadata["title"] = chapter_title
            self.base_metadata["subtitle"] = ""
            self.base_metadata["publication"] = (
                f"{book_title}: {book_subtitle}" if book_subtitle else book_title
            )


    def parse(self, text):
        try:
            d = self.bsstrtodict(text, parser="lxml-xml")
        except Exception as err:
            raise XmlLoadException(err)

        # If BookFrontMatter or BookBackMatter, top-level element is <book>
        if d.find("book", None):
            self.book = d.find("book")

            if self.book.find("book-meta", None):
                self.bookmeta = self.book.find("book-meta")

            # Only BookFrontMatter files contain <book> <front-matter>
            if self.book.find("front-matter", None):
                self.frontmatter = self.book.find("front-matter")

                if self.frontmatter.find("book-part-meta", None):
                    self.bookpartmeta = self.frontmatter.find("book-part-meta", None)

            # Only BookBackMatter files contain <book> <book-back>
            if self.book.find("book-back", None):
                self.backmatter = self.book.find("book-back")

                if self.backmatter.find("book-part-meta", None):
                    self.bookpartmeta = self.backmatter.find("book-part-meta", None)

                # For edited vols, refs live in <book-back> <book-part> <back>
                if self.backmatter.find("book-part"):
                    self.bookbackpart = self.backmatter.find("book-part")
                    if self.bookbackpart.find("back"):
                        self.back = self.bookbackpart.find("back")

        # If chapter or part frontmatter, top-level element is <book-part-wrapper>
        if d.find("book-part-wrapper", None):  
            self.bookpartwrapper = d.find("book-part-wrapper", None)

            # Book has <collection-meta> only if part of a series
            if self.bookpartwrapper.find("collection-meta", None):  
                self.collectionmeta = self.bookpartwrapper.find("collection-meta", None)
            # <collection-meta> has 2 collection-type="series" ="subseries"

            # All books have <book-meta>
            if self.bookpartwrapper.find("book-meta", None):
                self.bookmeta = self.bookpartwrapper.find("book-meta", None)

            # If chapters are in parts
            #   e.g., .../BOK=.../PRT=1/CHP=1_...
            # XML tree is:
            #   <book-part-wrapper> <book-part book-part-type="part"> <body> <book-part book-part-type="chapter"> <book-part-meta>

            # If book has no parts & chapter is top level
            #   e.g., .../BOK=.../CHP=1_..
            # XML tree is:
            #   <book-part-wrapper> <book-part book-part-type="chapter"> <book-part-meta>

            # Get <book-part book-part-type="chapter"> only
            # Ignore <book-part book-part-type="part"> <book-part-meta> as it only contains part title
            if self.bookpartwrapper.find("book-part", {"book-part-type": "chapter"}):
                self.bookpart = self.bookpartwrapper.find("book-part", {"book-part-type": "chapter"})

            if self.bookpart.find("book-part-meta", None):
                self.bookpartmeta = self.bookpart.find("book-part-meta", None)

        # self.contenttype is used in _parse_ functions above
        contrib_group = self.bookmeta.find("contrib-group")
        if contrib_group is not None:
            content_type = contrib_group.get("content-type", "").lower()
        # If book type is manuscript
        # <contrib-group content-type="book author"> or ="book authors"
        if "author" in content_type:
            self.contenttype = "manuscript"
        # If book type is edited volume
        # <contrib-group content-type="book editor"> or ="book editors"
        elif "editor" in content_type:
            self.contenttype = "edited"
        else:
            raise Exception("XML file is of unknown content type")


        self._parse_abstract()
        self._parse_authors()
        self._parse_collection()
        self._parse_ids()
        self._parse_keywords()
        self._parse_page()
        self._parse_permissions()
        self._parse_pubdate()
        self._parse_references()
        self._parse_title()

        output = self.format(self.base_metadata, format="Springer")

        return output


# Everything below parses authors parses authors & affiliations

class JATSAffils(object):
    regex_email = re.compile(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+")
    regex_auth_xid = re.compile(r"^A[0-9]+$")

    def __init__(self):
        self.contrib_dict = {}
        self.collab = {}
        self.xref_dict = OrderedDict()
        self.xref_xid_dict = OrderedDict()
        self.email_xref = OrderedDict()
        self.output = None

    def _decompose(self, soup=None, tag=None):
        """
        Remove all instances of a tag and its contents from the BeautifulSoup tree
        :param soup: BeautifulSoup object/tree
        :param tag: tag in the BS tree to remove
        :return: BeautifulSoup object/tree
        """
        for element in soup(tag):
            element.decompose()

        return soup

    def _get_inst_identifiers(self, aff):
        """
        Takes a single affiliation in soup form, removes any institution-ids from the text, places them in the aff_id array, and returns the soup sans ids and the aff_id array
        :param aff: BeautifulSoup object/tree
        :return: BeautifulSoup object/tree, aff_ids array
        """
        aff_ids = []
        aff_external_ids = aff.find_all("institution-id", [])
        for ident in aff_external_ids:
            idtype = ident.get("institution-id-type", "")
            idvalue = ident.get_text()
            aff_ids.append({idtype: idvalue})
            ident.decompose()
        if not aff_external_ids:
            aff_ids.append({})
        return aff, aff_ids

    def _remove_unbalanced_parentheses(self, affstr):
        # Stack to track balanced parentheses
        stack = []
        to_remove = set()

        for i, char in enumerate(affstr):
            # Track the index of opening parentheses
            if char == "(":
                stack.append(i)
            elif char == ")":
                if stack:
                    # Pop if there's a matching opening parenthesis
                    stack.pop()
                else:
                    # Mark unbalanced closing parenthesis
                    to_remove.add(i)

        # Mark remaining unbalanced opening parentheses
        to_remove.update(stack)

        # Create a new string without the unbalanced parentheses
        new_affstr = "".join([char for i, char in enumerate(affstr) if i not in to_remove])

        return new_affstr

    def _fix_affil(self, affstring):
        """
        Separate email addresses from affiliations in a given input affiliation string
        :param affstring: Raw affiliation string
        :return: newaffstr: affiliation string with email addresses removed
                 emails: list of email addresses
        """
        aff_list = affstring.split(";")
        new_aff = []
        emails = []
        for a in aff_list:
            a = a.strip()
            # check for empty strings with commas
            check_a = a.replace(",", "")
            if check_a:
                a = re.sub("\\(e-*mail:\\s*,+\\s*\\)", "", a)
                a = a.replace("\\n", ",")
                a = a.replace(" —", "—")
                a = a.replace(" , ", ", ")
                a = a.replace(", .", ".")
                a = re.sub(",+", ",", a)
                a = re.sub("\\s+", " ", a)
                a = re.sub("^(\\s*,+\\s*)+", "", a)
                a = re.sub("(\\s*,\\s+)+", ", ", a)
                a = re.sub("(,\\s*)+$", "", a)
                a = re.sub("\\s+$", "", a)
                if self.regex_email.match(a):
                    emails.append(a)
                else:
                    if a:
                        a = self._remove_unbalanced_parentheses(a)
                        new_aff.append(a)

        newaffstr = "; ".join(new_aff)
        return newaffstr, emails

    def _fix_email(self, email):
        """
        Separate and perform basic validation of email address string
        :param email: List of email address(es)
        :return: list of verified email addresses (those that match the regex)
        """
        email_new = OrderedSet()

        email_format = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
        email_parsed = False
        for em in email:
            if " " in em:
                for e in em.strip().split():
                    try:
                        if email_format.search(e):
                            email_new.add(email_format.search(e).group(0))
                            email_parsed = True
                    except Exception as err:
                        logger.warning("Bad format in _fix_email: %s" % err)
            else:
                try:
                    if type(em) == str:
                        if email_format.search(em):
                            email_new.add(email_format.search(em).group(0))
                            email_parsed = True
                    elif type(em) == list:
                        for e in em:
                            if email_format.search(e):
                                email_new.add(email_format.search(e).group(0))
                                email_parsed = True
                except Exception as err:
                    logger.warning("Bad format in _fix_email: %s" % err)

        if not email_parsed:
            logger.warning("Email not verified as valid. Input email list: %s", str(email))

        return list(email_new)

    def _fix_orcid(self, orcid):
        """
        Standarize ORCID formatting
        :param orcid: string or list of ORCIDs
        :return: uniqued list of ORCIDs, with URL-part removed if necessary
        """
        orcid_new = OrderedSet()
        if isinstance(orcid, str):
            orcid = [orcid]
        elif not isinstance(orcid, list):
            raise TypeError("ORCID must be str or list")

        orcid_format = re.compile(r"(\d{4}-){3}\d{3}(\d|X)")
        for orc in orcid:
            osplit = orc.strip().split()
            for o in osplit:
                # ORCID IDs sometimes have the URL prepended - remove it
                if orcid_format.search(o):
                    orcid_new.add(orcid_format.search(o).group(0))
        return list(orcid_new)

    def _reformat_affids(self):
        for contribs in self.contrib_dict.values():
            for auth in contribs:
                if auth.get("affid", None) == [[{}]]:
                    del auth["affid"]
                # Initialize affid if not present
                if not auth.get("affid"):
                    auth["affid"] = []
                # Process existing affids
                if auth["affid"]:
                    affid_tmp = []
                    for ids in auth.get("affid", None):
                        ids_tmp = []
                        for d in ids:
                            for k, v in d.items():
                                ids_tmp.append({"affIDType": k, "affID": v})
                        affid_tmp.append((ids_tmp))
                    if affid_tmp:
                        auth["affid"] = affid_tmp
                    else:
                        del auth["affid"]

    def _match_xref_clean(self):
        """
        Matches crossreferenced affiliations and emails; cleans emails and ORCIDs
        :return: none (updates class variable auth_list)
        """
        for contrib_type, contribs in self.contrib_dict.items():
            for auth in contribs:
                # contents of xaff field aren't always properly separated - fix that here
                xaff_list = []
                for item in auth.get("xaff", []):
                    xi = re.split("\\s*,\\s*|\\s+", item)
                    for x in xi:
                        xaff_list.append(x)

                    # if you found any emails in an affstring, add them
                    # to the email field
                    if item in self.email_xref:
                        auth["email"].append(self.email_xref[item])

                if auth.get("xref", None):
                    if not auth.get("affid"):
                        auth["affid"] = []

                xaff_xid_tmp = []
                for x in xaff_list:
                    try:
                        if self.xref_dict[x] not in auth["aff"]:
                            auth["aff"].append(self.xref_dict[x])
                    except KeyError as err:
                        logger.info("Key is missing from xaff. Missing key: %s", err)
                        pass
                    try:
                        if self.xref_xid_dict[x]:
                            xaff_xid_tmp.append(self.xref_xid_dict[x])
                        else:
                            xaff_xid_tmp.append([{}])
                    except KeyError as err:
                        logger.info("Key is missing from xaff. Missing key: %s" % err)
                if xaff_xid_tmp:
                    auth["affid"] = xaff_xid_tmp
                if not auth.get("affid", None) or not xaff_xid_tmp:
                    auth["affid"] = []

                # Check for 'ALLAUTH'/'ALLCONTRIB' affils (global affils without a key), and assign them to all authors/contributors
                if contrib_type == "authors" and "ALLAUTH" in self.xref_dict:
                    auth["aff"].append(self.xref_dict["ALLAUTH"])
                if contrib_type == "contributors" and "ALLCONTRIB" in self.xref_dict:
                    auth["aff"].append(self.xref_dict["ALLCONTRIB"])

                for item in auth.get("xemail", []):
                    try:
                        auth["email"].append(self.xref_dict[item])
                    except KeyError as err:
                        logger.info("Missing key in xemail! Error: %s", err)
                        pass

                if auth.get("email", []):
                    auth["email"] = self._fix_email(auth["email"])

                if auth.get("orcid", []):
                    try:
                        auth["orcid"] = self._fix_orcid(auth["orcid"])
                    except TypeError:
                        logger.warning(
                            "ORCID of wrong type (not str or list) passed, removing. ORCID: %s",
                            auth["orcid"],
                        )
                        auth["orcid"] = []

                # note that the ingest schema allows a single email address,
                # but we've extracted all here in case that changes to allow
                #  more than one
                if auth.get("email", []):
                    auth["email"] = auth["email"][0]
                else:
                    auth["email"] = ""

                # same for orcid
                if auth.get("orcid", []):
                    auth["orcid"] = auth["orcid"][0]
                else:
                    auth["orcid"] = ""

    def parse(self, article_metadata):
        """
        Parses author affiliation from BeautifulSoup object
        :param article_metadata: BeautifulSoup object containing author nodes
        :return: auth_list: list of dicts, one per author
        """
        article_metadata = self._decompose(soup=article_metadata, tag="label")

        art_contrib_groups = []
        if article_metadata.find("contrib-group"):
            art_contrib_groups = article_metadata.find_all("contrib-group")

        authors_out = []
        contribs_out = []

        # JATS puts author data in <contrib-group>, giving individual authors in each <contrib>
        for art_group in art_contrib_groups:
            art_contrib_group = art_group.extract()

            contribs_raw = art_contrib_group.find_all("contrib", recursive=False)

            default_key = "ALLAUTH"

            num_contribs = len(contribs_raw)

            # extract <contrib> from each <contrib-group>
            for idx, contrib in enumerate(contribs_raw):
                # note: IOP, APS get affil data within each contrib block,
                #       OUP, AIP, Springer, etc get them via xrefs.
                auth = {}
                # cycle through <contrib> to check if a <collab> is listed in the same level as an author an has multiple authors nested under it;
                # targeted for Springer

                if contrib.find("collab") or contrib.find("collab-name"):
                    # Springer collab info for nested authors is given as <institution>
                    if contrib.find("collab"):
                        if contrib.find("collab").find("institution"):
                            collab = contrib.find("collab").find("institution")
                        else:
                            collab = contrib.find("collab")
                    else:
                        collab = contrib.find("collab-name")

                    # This is checking if a collaboration is listed as an author
                    if collab:
                        if type(collab.contents[0].get_text()) == str:
                            collab_name = collab.contents[0].get_text().strip()
                        else:
                            collab_name = collab.get_text().strip()

                        if collab.find("address"):
                            collab_affil = collab.find("address").get_text()
                        else:
                            collab_affil = []

                        self.collab = {
                            "collab": collab_name,
                            "aff": collab_affil,
                            "affid": [],
                            "xaff": [],
                            "xemail": [],
                            "email": [],
                            "corresp": False,
                            "rid": None,
                            "surname": "",
                            "given": "",
                            "prefix": "",
                            "suffix": "",
                            "native_lang": "",
                            "orcid": "",
                        }

                    if self.collab:
                        # add collab in the correct author position
                        if self.collab not in authors_out:
                            authors_out.append(self.collab)

                    # find nested collab authors and unnest them
                    collab_contribs = collab.find_all("contrib")
                    nested_contribs = []
                    for ncontrib in collab_contribs:
                        if ncontrib:
                            nested_contribs.append(copy(ncontrib))
                            ncontrib.decompose()

                    if not nested_contribs:
                        nested_contribs = contrib.find_all("contrib")

                    nested_idx = idx + 1
                    for nested_contrib in nested_contribs:
                        if "rid" in nested_contrib.attrs:
                            rid_match = next(
                                (
                                    (rid_ndx, author)
                                    for rid_ndx, author in enumerate(authors_out)
                                    if author.get("rid") == nested_contrib["rid"]
                                ),
                                None,
                            )
                            if rid_match:
                                author_tmp = rid_match[1]
                                if contrib.find("collab").find("institution", None):
                                    author_tmp["collab"] = (
                                        contrib.find("collab").find("institution").get_text()
                                    )
                                    authors_out[rid_match[0]] = author_tmp
                        else:
                            # add new collab tag to each unnested author
                            if contrib.find("collab") and contrib.find("collab").find(
                                "institution"
                            ):
                                collab_text = (
                                    contrib.find("collab").find("institution").decode_contents()
                                )
                            elif collab_name:
                                collab_text = collab_name
                            else:
                                collab_text = None
                            if collab_text:
                                collabtag_string = "<collab>" + collab_text + "</collab>"
                                collabtag = bs4.BeautifulSoup(collabtag_string, "xml").collab

                            if not collabtag:
                                collabtag = "ALLAUTH"

                            if collabtag:
                                nested_contrib.insert(0, collabtag)
                                contribs_raw.insert(nested_idx, nested_contrib.extract())
                                nested_idx += 1

                # check if collabtag is present in the author author attributes
                collab = contrib.find("collab")

                if collab:
                    if type(collab.contents[0].get_text()) == str:
                        collab_name = collab.contents[0].get_text().strip()
                    else:
                        collab_name = collab.get_text().strip()

                    if collab.find("address"):
                        collab_affil = collab.find("address").get_text()
                    else:
                        collab_affil = ""

                    if not self.collab:
                        self.collab = {
                            "collab": collab_name,
                            "aff": collab_affil,
                            "affid": [],
                            "xaff": [],
                            "xemail": [],
                            "email": [],
                            "corresp": False,
                            "rid": None,
                            "surname": "",
                            "given": "",
                            "prefix": "",
                            "suffix": "",
                            "native_lang": "",
                            "orcid": "",
                        }

                l_correspondent = False
                if contrib.get("corresp", None) == "yes":
                    l_correspondent = True

                # get author's name
                if contrib.find("name") and contrib.find("name").find("surname"):
                    surname = contrib.find("name").find("surname").get_text()
                elif contrib.find("string-name") and contrib.find("string-name").find("surname"):
                    surname = contrib.find("string-name").find("surname").get_text()
                else:
                    surname = ""

                if contrib.find("name") and contrib.find("name").find("given-names"):
                    given = contrib.find("name").find("given-names").get_text()
                elif contrib.find("string-name") and contrib.find("string-name").find(
                    "given-names"
                ):
                    given = contrib.find("string-name").find("given-names").get_text()
                else:
                    given = ""

                if contrib.find("name") and contrib.find("name").find("suffix"):
                    suffix = contrib.find("name").find("suffix").get_text()
                elif contrib.find("string-name") and contrib.find("string-name").find("suffix"):
                    suffix = contrib.find("string-name").find("suffix").get_text()
                else:
                    suffix = ""

                if contrib.find("name") and contrib.find("name").find("prefix"):
                    prefix = contrib.find("name").find("prefix").get_text()
                elif contrib.find("string-name") and contrib.find("string-name").find("prefix"):
                    prefix = contrib.find("string-name").find("prefix").get_text()
                else:
                    prefix = ""

                # get native language author name
                if contrib.find("name-alternatives"):
                    if contrib.find("name-alternatives").find("string-name"):
                        if (
                            contrib.find("name-alternatives")
                            .find("string-name")
                            .get("name-style", "")
                            != "western"
                        ):
                            native_lang = (
                                contrib.find("name-alternatives")
                                .find("string-name")
                                .get_text()
                                .strip()
                            )
                        else:
                            native_lang = ""
                    else:
                        native_lang = contrib.find("name-alternatives").get_text().strip()
                else:
                    native_lang = ""

                # NOTE: institution-id is actually useful, but at
                # at the moment, strip it
                # contrib = self._decompose(soup=contrib, tag="institution-id")

                # get named affiliations within the contrib block
                affs = contrib.find_all("aff")
                aff_text = []
                email_list = []
                aff_extids = []
                for i in affs:
                    if not i.get("specific-use", None):
                        # special case: some pubs label affils with <sup>label</sup>, strip them
                        i = self._decompose(soup=i, tag="sup")
                        i, aff_extids_tmp = self._get_inst_identifiers(i)
                        affstr = i.get_text(separator=", ").strip()
                        (affstr, email_list) = self._fix_affil(affstr)
                        aff_text.append(affstr)
                        aff_extids.extend(aff_extids_tmp)
                        i.decompose()
                    else:
                        i.decompose()

                # special case (e.g. AIP) - one author per contrib group, aff stored at contrib group level
                if num_contribs == 1 and art_contrib_group.find("aff"):
                    aff_list = art_contrib_group.find_all("aff")
                    if aff_list:
                        for aff in aff_list:
                            aff, aff_extids_tmp = self._get_inst_identifiers(aff)
                            aff_fix = aff.get_text(separator=", ").strip()
                            (affstr, email_fix) = self._fix_affil(aff_fix)
                            email_list.extend(email_fix)
                            aff_text.append(affstr)
                            aff_extids.extend(aff_extids_tmp)
                            aff.decompose()

                # get xrefs...
                xrefs = contrib.find_all("xref")
                xref_aff = []
                xref_email = []
                for x in xrefs:
                    if x.get("ref-type", "") == "aff":
                        xref_aff.append(x["rid"])
                    elif x.get("ref-type", "") == "corresp":
                        xref_email.append(x["rid"])
                    x.decompose()

                # get email(s)...
                # we already have raw emails stripped out of affil strings above, add to this from contrib block
                email_contrib = contrib.find_all("email")
                for e in email_contrib:
                    email_list.append(e.get_text(separator=" ").strip())
                    e.decompose()

                # get orcid
                contrib_id = contrib.find_all("contrib-id")
                orcid = []
                for c in contrib_id:
                    if (c.get("contrib-id-type", "") == "orcid") or ("orcid" in c.get_text()):
                        orcid.append(c.get_text(separator=" ").strip())
                    c.decompose()

                # double-check for orcid in other places...
                extlinks = contrib.find_all("ext-link")
                for e in extlinks:
                    # orcid
                    if e.get("ext-link-type", "") == "orcid":
                        orcid.append(e.get_text(separator=" ").strip())
                    e.decompose()

                # note that the ingest schema allows a single orcid, but we've extracted all
                # here in case that changes to allow more than one
                if orcid:
                    orcid_out = self._fix_orcid(orcid)
                    if orcid_out:
                        orcid_out = orcid_out[0]
                    else:
                        orcid_out = ""
                else:
                    orcid_out = ""

                # create the author dict
                auth["corresp"] = l_correspondent
                auth["surname"] = surname
                auth["given"] = given
                auth["suffix"] = suffix
                auth["prefix"] = prefix
                auth["native_lang"] = native_lang
                auth["aff"] = aff_text
                auth["affid"] = aff_extids
                auth["xaff"] = xref_aff
                auth["xemail"] = xref_email
                auth["orcid"] = orcid_out
                auth["email"] = email_list
                auth["rid"] = contrib.get("id", None)

                # this is a list of author dicts
                if auth:
                    if collab:
                        auth["collab"] = collab_name

                    # Check if author is a duplicate of a collaboration
                    if auth.get("surname", "") == "" and auth.get("collab", ""):
                        # delete email and correspondence info for collabs
                        auth["email"] = []
                        auth["xemail"] = []
                        auth["corresp"] = False
                        # if the collab is already in author list, skip
                        if auth in authors_out:
                            continue

                    if contrib.get("contrib-type", "author") == "author":
                        authors_out.append(auth)
                        default_key = "ALLAUTH"
                    else:
                        if contrib.find("role"):
                            role = contrib.find("role").get_text()
                        else:
                            role = contrib.get("contrib-type", "contributor")
                        auth["role"] = role
                        contribs_out.append(auth)
                        default_key = "ALLCONTRIB"
                contrib.decompose()

            if self.collab:
                if self.collab not in authors_out:
                    authors_out.append(self.collab)

            # special case: affs defined in contrib-group, but not in individual contrib
            if art_contrib_group:
                contrib_aff = art_contrib_group.find_all("aff")
                contrib_aff_new = []
                for a in contrib_aff:
                    if not a.get("specific-use", None):
                        contrib_aff_new.append(a)
                contrib_aff = contrib_aff_new
                for aff in contrib_aff:
                    # check and see if the publisher defined an email tag inside an affil (like IOP does)
                    nested_email_list = aff.find_all("ext-link")
                    key = aff.get("id", default_key)
                    for e in nested_email_list:
                        if e.get("ext-link-type", None) == "email":
                            if e.get("id", None):
                                ekey = e["id"]
                            else:
                                ekey = key
                            value = e.text
                            # build the cross-reference dictionary to be used later
                            self.email_xref[ekey] = value
                            e.decompose()

                    # special case: get rid of <sup>...
                    aff = self._decompose(soup=aff, tag="sup")
                    aff, aff_extids_tmp = self._get_inst_identifiers(aff)

                    # getting rid of ext-link eliminates *all* emails,
                    # so this is not the place to fix the iop thing
                    # a = self._decompose(soup=a, tag='ext-link')

                    affstr = aff.get_text(separator=", ").strip()
                    (affstr, email_list) = self._fix_affil(affstr)
                    if not self.email_xref.get(key, None):
                        if email_list:
                            self.email_xref[key] = email_list
                        else:
                            self.email_xref[key] = ""
                    self.xref_dict[key] = affstr
                    self.xref_xid_dict[key] = aff_extids_tmp

        # special case: publisher defined aff/email xrefs, but the xids aren't
        # assigned to authors; xid is typically of the form "A\d+"
        # publisher example: Geol. Soc. London (gsl)
        count_auth = len(authors_out)
        count_xref = len(self.xref_dict.keys())
        if count_auth == count_xref:
            for auth, xref in zip(authors_out, self.xref_dict.keys()):
                if self.regex_auth_xid.match(xref):
                    if not auth.get("aff", []) and not auth.get("xaff", []):
                        auth["xaff"] = [xref]

        self.contrib_dict = {"authors": authors_out, "contributors": contribs_out}

        # now get the xref keys outside of contrib-group:
        # aff xrefs...
        aff_glob = article_metadata.find_all("aff")
        aff_glob_new = []
        for a in aff_glob:
            if not a.get("specific-use", None):
                aff_glob_new.append(a)
        aff_glob = aff_glob_new
        for aff in aff_glob:
            try:
                key = aff["id"]
            except KeyError:
                logger.info("No aff id key in: %s", aff)
                continue
            # special case: get rid of <sup>...
            aff = self._decompose(soup=aff, tag="sup")

            aff, aff_extids_tmp = self._get_inst_identifiers(aff)
            affstr = aff.get_text(separator=", ").strip()
            (aff_list, email_list) = self._fix_affil(affstr)
            self.xref_dict[key] = aff_list
            if self.xref_xid_dict.get(key, None):
                self.xref_xid_dict[key].extend(aff_extids_tmp)
            else:
                self.xref_xid_dict[key] = aff_extids_tmp
            aff.decompose()

        # author-notes xrefs...
        authnote_glob = article_metadata.find_all("author-notes")
        for aff in authnote_glob:
            # emails...
            cor = aff.find_all("corresp")
            for c in cor:
                try:
                    key = c["id"]
                except KeyError:
                    logger.info("No authnote id key in: %s", aff)
                    continue
                c = self._decompose(soup=c, tag="sup")
                val = c.get_text(separator=" ").strip()
                self.xref_dict[key] = val
                c.decompose()

        # finishing up
        self._match_xref_clean()
        self._reformat_affids()

        return self.contrib_dict


import logging
import re
from collections import OrderedDict

from bs4 import BeautifulSoup

from adsingestp import utils
from adsingestp.parsers.base import BaseBeautifulSoupParser

logger = logging.getLogger(__name__)


class JATSAffils(object):
    regex_email = re.compile(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+")

    def __init__(self):
        self.auth_list = []
        self.collab = {}
        self.xref_dict = OrderedDict()
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
            if self.regex_email.match(a):
                emails.append(a)
            else:
                # check for empty strings with commas
                check_a = a.replace(",", "")
                if check_a:
                    a = a.replace(" , ", ", ")
                    a = re.sub("\\s+", " ", a)
                    new_aff.append(a)

        newaffstr = "; ".join(new_aff)
        return newaffstr, emails

    def _fix_email(self, email):
        """
        Separate and perform basic validation of email address string
        :param email: String of email address(es)
        :return: list of verified email addresses (those with an @ sign)
        """
        email_new = set()
        for em in email:
            if " " in em:
                for e in em.strip().split():
                    if "@" in e:
                        email_new.add(e.strip())
            else:
                if "@" in em:
                    email_new.add(em.strip())
        return list(email_new)

    def _fix_orcid(self, orcid):
        """
        Standarize ORCID formatting
        :param orcid: string or list of ORCIDs
        :return: uniqued list of ORCIDs, with URL-part removed if necessary
        """
        orcid_new = set()
        if isinstance(orcid, str):
            orcid = [orcid]
        elif not isinstance(orcid, list):
            raise TypeError("ORCID must be str or list")

        for orc in orcid:
            osplit = orc.strip().split()
            for o in osplit:
                o = o.rstrip("/").split("/")[-1]
                orcid_new.add(o)
        return list(orcid_new)

    def _match_xref_clean(self):
        """
        Matches crossreferenced affiliations and emails; cleans emails and ORCIDs
        :return: none (updates class variable auth_list)
        """
        for auth in self.auth_list:
            for item in auth.get("xaff", []):
                xi = re.split("\\s*,\\s*|\\s+", item)
                for x in xi:
                    try:
                        auth["aff"].append(self.xref_dict[x])
                    except KeyError as err:
                        logger.info("Key is missing from xaff. Missing key: %s", err)
                        pass

                # if you found any emails in an affstring, add them
                # to the email field
                if item in self.email_xref:
                    auth["email"].append(self.email_xref[item])

            # Check for 'ALLAUTH' affils (global affils without a key), and assign them to all authors
            if "ALLAUTH" in self.xref_dict:
                auth["aff"].append(self.xref_dict["ALLAUTH"])

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

            # note that the ingest schema allows a single email address, but we've extracted all
            # here in case that changes to allow more than one
            if auth["email"]:
                auth["email"] = auth["email"][0]
            else:
                auth["email"] = ""

            # same for orcid
            if auth["orcid"]:
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

        art_contrib_group = None
        if article_metadata.find("contrib-group"):
            art_contrib_group = article_metadata.find("contrib-group").extract()

        # JATS puts author data in <contrib-group>, giving individual authors in each <contrib>
        if art_contrib_group:
            contribs_raw = art_contrib_group.find_all("contrib")
            for contrib in contribs_raw:
                # note: IOP, APS get affil data within each contrib block,
                #       OUP, AIP, Springer, etc get them via xrefs.

                auth = {}

                collab = contrib.find("collab")
                if collab:
                    collab_name = collab.get_text()
                    if collab.find("address"):
                        collab_affil = collab.find("address").get_text()
                    else:
                        collab_affil = ""
                    self.collab = {
                        "name": collab_name,
                        "aff": collab_affil,
                        "xaff": [],
                        "xemail": [],
                        "email": [],
                        "corresp": False,
                    }

                l_correspondent = False
                if contrib.get("corresp", "") == "yes":
                    l_correspondent = True

                # get author's name
                if contrib.find("name") and contrib.find("name").find("surname"):
                    surname = contrib.find("name").find("surname").get_text()
                else:
                    surname = ""
                if contrib.find("name") and contrib.find("name").find("given-names"):
                    given = contrib.find("name").find("given-names").get_text()
                else:
                    given = ""

                # NOTE: institution-id is actually useful, but at
                # at the moment, strip it
                contrib = self._decompose(soup=contrib, tag="institution-id")

                # get named affiliations within the contrib block
                affs = contrib.find_all("aff")
                aff_text = []
                email_list = []
                for i in affs:
                    # special case: some pubs label affils with <sup>label</sup>, strip them
                    i = self._decompose(soup=i, tag="sup")
                    affstr = i.get_text(separator=" ").strip()
                    (affstr, email_list) = self._fix_affil(affstr)
                    aff_text.append(affstr)
                    i.decompose()

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
                    if c.get("contrib-id-type", "") == "orcid":
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
                    orcid_out = orcid_out[0]
                else:
                    orcid_out = ""

                # create the author dict
                auth.update(corresp=l_correspondent)
                auth.update(surname=surname)
                auth.update(given=given)
                auth.update(aff=aff_text)
                auth.update(xaff=xref_aff, xemail=xref_email)
                auth.update(orcid=orcid_out)
                auth.update(email=email_list)
                contrib.decompose()

                # this is a list of author dicts
                if auth:
                    self.auth_list.append(auth)

        if self.collab:
            self.auth_list.append(self.collab)

        # special case: affs defined in contrib-group, but not in individual contrib
        if art_contrib_group:
            contrib_aff = art_contrib_group.find_all("aff")
            for aff in contrib_aff:
                # check and see if the publisher defined an email tag inside an affil (like IOP does)
                nested_email_list = aff.find_all("ext-link")
                for e in nested_email_list:
                    key = e["id"]
                    value = e.text
                    # build the cross-reference dictionary to be used later
                    self.email_xref[key] = value
                    e.decompose()

                key = aff.get("id", "ALLAUTH")
                # special case: get rid of <sup>...
                aff = self._decompose(soup=aff, tag="sup")
                aff = self._decompose(soup=aff, tag="institution-id")
                # getting rid of ext-link eliminates *all* emails,
                # so this is not the place to fix the iop thing
                # a = self._decompose(soup=a, tag='ext-link')

                affstr = aff.get_text().strip()
                (affstr, email_list) = self._fix_affil(affstr)
                if email_list:
                    self.email_xref[key] = email_list
                self.xref_dict[key] = affstr

        # now get the xref keys outside of contrib-group:
        # aff xrefs...
        aff_glob = article_metadata.find_all("aff")
        for aff in aff_glob:
            try:
                key = aff["id"]
            except KeyError:
                logger.info("No aff id key in: %s", aff)
                continue
            # special case: get rid of <sup>...
            aff = self._decompose(soup=aff, tag="sup")
            # NOTE: institution-id is actually useful, but at
            # at the moment, strip it
            aff = self._decompose(soup=aff, tag="institution-id")
            affstr = aff.get_text(separator=" ").strip()
            (aff_list, email_list) = self._fix_affil(affstr)
            self.xref_dict[key] = aff_list
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

        return self.auth_list


class JATSParser(BaseBeautifulSoupParser):
    fix_ampersand = re.compile(r"(&amp;)(.*?)(;)")

    JATS_TAGS_MATH = [
        "inline-formula",
        "tex-math",
        "mml:math",
        "mml:semantics",
        "mml:mrow",
        "mml:munder",
        "mml:mo",
        "mml:mi",
        "mml:msub",
        "mml:mover",
        "mml:mn",
        "mml:annotation",
    ]

    JATS_TAGS_HTML = ["sub", "sup", "a", "astrobj"]

    JATS_TAGSET = {
        "title": JATS_TAGS_MATH + JATS_TAGS_HTML,
        "abstract": JATS_TAGS_MATH + JATS_TAGS_HTML + ["pre", "br"],
        "comments": JATS_TAGS_MATH + JATS_TAGS_HTML + ["pre", "br"],
        "affiliations": ["email", "orcid"],
        "keywords": ["astrobj"],
    }

    JATS_TAGS_DANGER = ["php", "script", "css"]

    def __init__(self):
        self.base_metadata = {}
        self.back_meta = None
        self.article_meta = None
        self.journal_meta = None
        self.isErratum = False

    def _detag(self, r, tags_keep):
        """
        Removes tags from input BeautifulSoup object
        :param r: BeautifulSoup object (not string)
        :param tags_keep: this function will remove all tags except those passed here
        :return: newr: striing with cleaned text
        """
        # note that parser=lxml is recommended here - if the more stringent lxml-xml is used,
        # the output is slightly different and the code will need to be modified
        newr = BeautifulSoup(str(r), "lxml")
        if newr.find_all():
            tag_list = list(set([x.name for x in newr.find_all()]))
        else:
            tag_list = []
        for t in tag_list:
            elements = newr.find_all(t)
            for e in elements:
                if t in self.JATS_TAGS_DANGER:
                    e.decompose()
                elif t in tags_keep:
                    continue
                else:
                    if t.lower() == "sc":
                        e.string = e.string.upper()
                    e.unwrap()

        # Note: newr is converted from a bs4 object to a string here.
        # Everything after this point is string manipulation.
        newr = str(newr)

        amp_fix = self.fix_ampersand.findall(newr)
        for s in amp_fix:
            s_old = "".join(s)
            s_new = "&" + s[1] + ";"
            newr = newr.replace(s_old, s_new)

        newr = re.sub("\\s+|\n+|\r+", " ", newr)
        newr = newr.replace("&nbsp;", " ")
        newr = newr.strip()

        return newr

    def _get_date(self, d):
        """
        Extract and standarize date from input BeautifulSoup date object
        :param d: BeautifulSoup date object
        :return: formatted date string (yyyy-mm-dd)
        """
        if d.find("year"):
            pubdate = self._detag(d.year, [])
        else:
            pubdate = "0000"

        if d.find("month"):
            month_raw = self._detag(d.month, [])
            if month_raw.isdigit():
                month = month_raw
            else:
                month_name = month_raw[0:3].lower()
                month = utils.MONTH_TO_NUMBER[month_name]

            if int(month) < 10 and len(month) < 2:
                month = "0" + str(int(month))
            else:
                month = str(month)
            pubdate = pubdate + "-" + month
        else:
            pubdate = pubdate + "-" + "00"

        if d.find("day"):
            day_raw = self._detag(d.day, [])
            if day_raw.isdigit():
                day = day_raw
            else:
                day = "00"

            if int(day) < 10 and len(day) < 2:
                day = "0" + str(int(day))
            else:
                day = str(day)
            pubdate = pubdate + "-" + day
        else:
            pubdate = pubdate + "-" + "00"

        return pubdate

    def _parse_title_abstract(self):
        title_xref_list = []
        title_fn_list = []
        self.titledoi = None
        if self.article_meta.find("title-group") and self.article_meta.find("title-group").find(
            "article-title"
        ):
            title = self.article_meta.find("title-group").find("article-title")
            for dx in title.find_all("ext-link"):
                self.titledoi = dx.find("xlink:href")
            for dx in title.find_all("xref"):
                title_xref_list.append(self._detag(dx, self.JATS_TAGSET["abstract"]).strip())
                dx.decompose()
            for df in title.find_all("fn"):
                title_fn_list.append(self._detag(df, self.JATS_TAGSET["abstract"]).strip())
                df.decompose()

            self.base_metadata["title"] = self._detag(title, self.JATS_TAGSET["title"]).strip()

        if self.article_meta.find("abstract") and self.article_meta.find("abstract").find("p"):
            abstract = self._detag(
                self.article_meta.find("abstract").find("p"), self.JATS_TAGSET["abstract"]
            )
            self.base_metadata["abstract"] = abstract
            if title_fn_list:
                self.base_metadata["abstract"] += "  " + " ".join(title_fn_list)

    def _parse_author(self):
        auth_affil = JATSAffils()
        aa_output = auth_affil.parse(article_metadata=self.article_meta)
        if aa_output:
            self.base_metadata["authors"] = aa_output

    def _parse_copyright(self):
        copyright = self.article_meta.find("copyright-statement")
        if copyright:
            self.base_metadata["copyright"] = self._detag(copyright, [])

    def _parse_edhistory(self):
        # received and revised dates can be arrays, but accepted can just be a single value
        received = []
        revised = []

        dates = self.article_meta.find("history").find_all("date")
        for d in dates:
            date_type = d.get("date-type", "")
            eddate = self._get_date(d)
            if date_type == "received":
                received.append(eddate)
            elif date_type == "rev-recd":
                revised.append(eddate)
            elif date_type == "accepted":
                self.base_metadata["edhist_acc"] = eddate
            else:
                logger.info("Editorial history date type (%s) not recognized.", date_type)

        self.base_metadata["edhist_rec"] = received
        self.base_metadata["edhist_rev"] = revised

    def _parse_keywords(self):
        keys_uat = []
        keys_misc = []
        keys_aas = []
        keys_out = []
        keyword_groups = self.article_meta.find_all("kwd-group")
        for kg in keyword_groups:
            # Check for UAT first:
            if kg.get("kwd-group-type", "") == "author":
                keys_uat_test = kg.find_all("compound-kwd-part")
                for kk in keys_uat_test:
                    if kk["content-type"] == "uat-code":
                        keys_uat.append(self._detag(kk, self.JATS_TAGSET["keywords"]))
                if not keys_uat:
                    keys_misc_test = kg.find_all("kwd")
                    for kk in keys_misc_test:
                        keys_misc.append(self._detag(kk, self.JATS_TAGSET["keywords"]))
            # Then check for AAS:
            elif kg.get("kwd-group-type", "") == "AAS":
                keys_aas_test = kg.find_all("kwd")
                for kk in keys_aas_test:
                    keys_aas.append(self._detag(kk, self.JATS_TAGSET["keywords"]))
            # If all else fails, just search for 'kwd'
            else:
                keys_misc_test = kg.find_all("kwd")
                for kk in keys_misc_test:
                    keys_misc.append(self._detag(kk, self.JATS_TAGSET["keywords"]))

        if keys_uat:
            for k in keys_uat:
                keys_out.append({"system": "UAT", "string": k})

        if keys_aas:
            for k in keys_aas:
                keys_out.append({"system": "AAS", "string": k})

        if keys_misc:
            for k in keys_misc:
                keys_out.append({"system": "misc", "string": k})

        if keys_out:
            self.base_metadata["keywords"] = keys_out

        if "keywords" not in self.base_metadata:
            if self.article_meta.find("article-categories"):
                keywords = self.article_meta.find("article-categories").find_all("subj-group")
            else:
                keywords = []
            for c in keywords:
                if c.get("subj-group-type", "") == "toc-minor":
                    for k in c.find_all("subject"):
                        keys_out.append(
                            {
                                "system": "subject",
                                "string": self._detag(k, self.JATS_TAGSET["keywords"]),
                            }
                        )

                    self.base_metadata["keywords"] = keys_out
                else:
                    for k in c.find_all("subject"):
                        if k.string == "Errata" or k.string == "Corrigendum":
                            self.isErratum = True

    def _parse_pub(self):
        journal = None
        if self.journal_meta.find("journal-title-group") and self.journal_meta.find(
            "journal-title-group"
        ).find("journal-title"):
            journal = self.journal_meta.find("journal-title-group").find("journal-title")
        elif self.journal_meta.find("journal-title"):
            journal = self.journal_meta.find("journal-title")

        if journal:
            self.base_metadata["publication"] = self._detag(journal, [])

        if self.journal_meta.find("publisher") and self.journal_meta.find("publisher").find(
            "publisher-name"
        ):
            self.base_metadata["publisher"] = self._detag(
                self.journal_meta.find("publisher").find("publisher-name"), []
            )

        issn_all = self.journal_meta.find_all("issn")
        issns = []
        for i in issn_all:
            issns.append((i["pub-type"], self._detag(i, [])))
        self.base_metadata["issn"] = issns

    def _parse_related(self):
        # Related article data, especially corrections and errata

        related = self.article_meta.find_all("related-article")
        relateddoi = ""
        for r in related:
            # TODO are there other types of related articles to track? need an example
            if r.get("related-article-type", "") == "corrected-article":
                self.isErratum = True
                relateddoi = r.get("xlink:href", "")

        if self.isErratum:
            doiurl_pat = r"(.*?)(doi.org\/)"
            if self.titledoi:
                self.base_metadata["relatedto"] = {
                    "relationship": "errata",
                    "id": re.sub(doiurl_pat, "", self.titledoi),
                }
            elif relateddoi:
                self.base_metadata["relatedto"] = {
                    "relationship": "errata",
                    "id": re.sub(doiurl_pat, "", relateddoi),
                }
            else:
                logger.warning("No DOI for erratum: %s", related)

    def _parse_ids(self):

        self.base_metadata["ids"] = {}

        ids = self.article_meta.find_all("article-id")

        self.base_metadata["ids"]["pub-id"] = []
        for d in ids:
            id_type = d.get("pub-id-type", "")
            # DOI
            if id_type == "doi":
                self.base_metadata["ids"]["doi"] = self._detag(d, [])

            # publisher ID
            if id_type == "publisher-id":
                self.base_metadata["ids"]["pub-id"].append(
                    {"attribute": "publisher-id", "Identifier": self._detag(d, [])}
                )
            elif id_type == "manuscript":
                self.base_metadata["ids"]["pub-id"].append(
                    {"attribute": "manuscript", "Identifier": self._detag(d, [])}
                )
            elif id_type == "other":
                self.base_metadata["ids"]["pub-id"].append(
                    {"attribute": "other", "Identifier": self._detag(d, [])}
                )

        # Arxiv Preprint
        arxiv = self.article_meta.find_all("custom-meta")
        # ax_pref = "https://arxiv.org/abs/"
        self.base_metadata["ids"]["preprint"] = {}
        for ax in arxiv:
            x_name = self._detag(ax.find("meta-name"), [])
            x_value = self._detag(ax.find("meta-value"), [])
            if x_name and x_name == "arxivppt":
                self.base_metadata["ids"]["preprint"].append({"source": "arxiv", "id": x_value})

    def _parse_pubdate(self):
        pub_dates = self.article_meta.find_all("pub-date")
        for d in pub_dates:
            pub_format = d.get("publication-format", "")
            pub_type = d.get("pub-type", "")
            pubdate = self._get_date(d)
            if pub_format == "print" or pub_type == "ppub" or pub_type == "cover":
                self.base_metadata["pubdate_print"] = pubdate
            elif pub_format == "electronic" or pub_type == "epub":
                self.base_metadata["pubdate_electronic"] = pubdate

            if pub_type == "open-access":
                self.base_metadata.setdefault("openAccess", {}).setdefault("open", True)

    def _parse_permissions(self):
        # Check for open-access / "Permissions" field
        permissions = self.article_meta.find("permissions").find_all("license")
        for p in permissions:
            if p.find("license-type") == "open":
                self.base_metadata.setdefault("openAccess", {}).setdefault("open", True)

    def _parse_page(self):
        fpage = self.article_meta.find("fpage")
        if not fpage:
            fpage = self.article_meta.find("pageStart")
        if fpage:
            self.base_metadata["page_first"] = self._detag(fpage, [])

        e_id = self.article_meta.find("elocation-id")
        if e_id:
            self.base_metadata["electronic_id"] = self._detag(e_id, [])

        lpage = self.article_meta.find("lpage")
        if not lpage:
            lpage = self.article_meta.find("pageEnd")
        if lpage == fpage:
            lpage = None
        if lpage:
            self.base_metadata["page_last"] = self._detag(lpage, [])

        if fpage and lpage:
            self.base_metadata["page_range"] = (
                self._detag(fpage, []) + "-" + (self._detag(lpage, []))
            )

        # Number of Pages:
        if self.article_meta.find("counts") and self.article_meta.find("counts").find(
            "page-count"
        ):
            self.base_metadata["numpages"] = (
                self.article_meta.find("counts").find("page-count").get("count", "")
            )

    def _parse_references(self):
        if self.back_meta is not None:
            ref_list_text = []
            ref_results = self.back_meta.find("ref-list").find_all("ref")
            for r in ref_results:
                # output raw XML for reference service to parse later
                s = str(r.extract()).replace("\n", " ")
                ref_list_text.append(s)
            self.base_metadata["references"] = ref_list_text

    def parse(self, text):
        """
        Parse JATS XML into standard JSON format
        :param text: string, contents of XML file
        :return: parsed file contents in JSON format
        """
        d = self.bsstrtodict(text, parser="lxml-xml")
        document = d.article

        front_meta = document.front
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
        self.base_metadata["volume"] = self._detag(volume, [])

        # Issue:
        issue = self.article_meta.issue
        self.base_metadata["issue"] = self._detag(issue, [])

        self._parse_pub()
        self._parse_related()
        self._parse_ids()
        self._parse_pubdate()
        self._parse_edhistory()
        self._parse_permissions()
        self._parse_page()

        self._parse_references()

        self.base_metadata = self._entity_convert(self.base_metadata)

        output = self.format(self.base_metadata, format="JATS")

        return output

    def add_fulltext(self):
        pass

import logging
import re
from collections import OrderedDict

from bs4 import BeautifulSoup

from adsingestp import serializer, utils
from adsingestp.ingest_exceptions import JATSContribException
from adsingestp.parsers.base import BaseBeautifulSoupParser

logger = logging.getLogger(__name__)


class JATSAffils(object):
    regex_spcom = re.compile(r"\s+,")
    regex_multisp = re.compile(r"\s+")
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
        try:
            for element in soup(tag):
                element.decompose()
        except Exception:
            pass
        return soup

    def _fix_affil(self, affstring):
        aff_list = affstring.split(";")
        new_aff = []
        emails = []
        for a in aff_list:
            a = a.strip()
            if self.regex_email.match(a):
                emails.append(a)
            else:
                # check for empty strings with commas
                checka = a.replace(",", " ")
                if checka.replace(" ", ""):
                    a = a.replace(" , ", ", ").replace("  ", " ")
                    new_aff.append(a)

        newaffstr = "; ".join(new_aff)
        return newaffstr, emails

    def _fix_email(self, email):
        email_new = []
        for em in email:
            if " " in em:
                for e in em.strip().split():
                    if "@" in e:
                        email_new.append(e.strip())
            else:
                if "@" in em:
                    email_new.append(em.strip())
        return list(dict.fromkeys(email_new))

    def _fix_orcid(self, orcid):
        orcid_new = []
        for orc in orcid:
            osplit = orc.strip().split()
            for o in osplit:
                o = o.rstrip("/").split("/")[-1]
                orcid_new.append(o)
        return list(dict.fromkeys(orcid_new))

    def _match_xref(self):
        for a in self.auth_list:
            try:
                for item in a["xaff"]:
                    item = re.sub(r"\s+", ",", item)
                    xi = re.split(",", item)
                    for x in xi:
                        try:
                            a["aff"].append(self.xref_dict[x])
                        except Exception as err:
                            # import pdb
                            # pdb.set_trace()
                            logger.info("Key is missing from xaff. Missing key: %s", err)
                            pass

                    # if you found any emails in an affstring, add them
                    # to the email field
                    if item in self.email_xref:
                        a["email"].append(self.email_xref[item])

                # Check for 'ALLAUTH' affils (global affils without a key),
                # and assign them to all authors
                if "ALLAUTH" in self.xref_dict:
                    a["aff"].append(self.xref_dict["ALLAUTH"])
            except Exception:
                pass

            try:
                for item in a["xemail"]:
                    try:
                        a["email"].append(self.xref_dict[item])
                    except Exception as err:
                        logger.info("Missing key in xemail! Error: %s", err)
                        pass
            except Exception:
                pass

            # note that the ingest schema allows a single email address, but we've extracted all
            # here in case that changes to allow more than one
            if a["email"]:
                a["email"] = a["email"][0]
            else:
                a["email"] = ""

    def parse(self, article_metadata):
        """

        :param article_metadata: BeautifulSoup object
        :return:
        """
        try:
            try:
                article_metadata = self._decompose(soup=article_metadata, tag="label")
            except Exception:
                pass

            # JATS puts author data in <contrib-group>, giving individual
            # authors in each <contrib>
            try:
                art_contrib_group = article_metadata.find("contrib-group").extract()
            except Exception:
                pass
            else:
                contribs_raw = art_contrib_group.find_all("contrib")

                for contrib in contribs_raw:
                    auth = {}
                    l_correspondent = False

                    # note: IOP, APS get affil data within each contrib block,
                    #       OUP, AIP, Springer, etc get them via xrefs.
                    try:
                        if contrib["contrib-type"] == "collab":
                            collab = contrib.find("collab")
                            try:
                                collab_name = collab.text
                            except Exception:
                                pass
                            else:
                                self.collab = {
                                    "name": collab_name,
                                    "aff": [],
                                    "xaff": [],
                                    "xemail": [],
                                    "email": [],
                                    "corresp": False,
                                }
                        elif contrib["contrib-type"] == "author":
                            if contrib.find("collab") is not None:
                                collab = contrib.find("collab")
                                try:
                                    collab_name = collab.contents[0]
                                except Exception:
                                    pass
                                else:
                                    self.collab = {
                                        "name": collab_name,
                                        "aff": [],
                                        "xaff": [],
                                        "xemail": [],
                                        "email": [],
                                        "corresp": False,
                                    }
                                    try:
                                        collab_affil = collab.find("address").text
                                    except Exception:
                                        pass
                                    else:
                                        self.collab["aff"] = collab_affil
                            else:
                                # corresponding author?
                                try:
                                    if contrib["corresp"] == "yes":
                                        l_correspondent = True
                                except Exception:
                                    pass

                                # get author's name
                                try:
                                    surname = contrib.find("name").find("surname").text
                                except Exception:
                                    surname = ""
                                try:
                                    given = contrib.find("name").find("given-names").text
                                except Exception:
                                    given = ""

                                # get named affiliations within the contrib block
                                affs = contrib.find_all("aff")
                                aff_text = []
                                for i in affs:
                                    # special case: some pubs label affils with
                                    # <sup>label</sup>, strip them
                                    try:
                                        # NOTE: institution-id is actually useful, but at
                                        # at the moment, strip it
                                        # TODO get an example from MT - none in the test data
                                        contrib = self._decompose(
                                            soup=contrib, tag="institution-id"
                                        )
                                        i = self._decompose(soup=i, tag="sup")
                                    except Exception:
                                        pass
                                    affstr = i.get_text(separator=" ").strip()
                                    (affstr, email_list) = self._fix_affil(affstr)
                                    aff_text.append(affstr)
                                    i.decompose()

                                # get xrefs...
                                xrefs = contrib.find_all("xref")
                                xref_aff = []
                                xref_email = []
                                for x in xrefs:
                                    try:
                                        if x["ref-type"] == "aff":
                                            xref_aff.append(x["rid"])
                                        elif x["ref-type"] == "corresp":
                                            xref_email.append(x["rid"])
                                    except Exception:
                                        pass
                                    x.decompose()

                                # get orcid
                                contrib_id = contrib.find_all("contrib-id")
                                orcid = []
                                for c in contrib_id:
                                    try:
                                        if c["contrib-id-type"] == "orcid":
                                            orcid.append(c.get_text(separator=" ").strip())
                                    except Exception:
                                        pass
                                    c.decompose()

                                # get email(s)...
                                emails = []
                                # first, add any emails found by stripping raw emails out of affil strings above...
                                try:
                                    for e in email_list:
                                        emails.append(e)
                                except Exception:
                                    pass
                                else:
                                    email_list = []
                                try:
                                    email = contrib.find_all("email")
                                    for e in email:
                                        try:
                                            emails.append(e.get_text(separator=" ").strip())
                                        except Exception:
                                            pass
                                        e.decompose()
                                except Exception:
                                    pass

                                # double-check for other things...
                                extlinks = contrib.find_all("ext-link")
                                for e in extlinks:
                                    # orcid
                                    try:
                                        if e["ext-link-type"] == "orcid":
                                            orcid.append(e.get_text(separator=" ").strip())
                                    except Exception:
                                        pass
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
                                auth.update(email=emails)
                                contrib.decompose()

                        # this is a list of author dicts
                        if auth:
                            self.auth_list.append(auth)
                    except Exception:
                        pass

                if self.collab:
                    self.auth_list.append(self.collab)

                # special case: affs defined in contrib-group, but
                #               not in individual contrib
                contrib_aff = art_contrib_group.find_all("aff")
                for a in contrib_aff:
                    # check and see if the publisher defined an email tag
                    # inside an affil (like IOP does...)
                    nested_email_list = a.find_all("ext-link")
                    if nested_email_list:
                        for e in nested_email_list:
                            key = e["id"]
                            value = e.text
                            self.email_xref[key] = value
                            e.decompose()
                    try:
                        key = a["id"]
                    except Exception:
                        key = "ALLAUTH"
                    try:
                        # special case: get rid of <sup>...
                        try:
                            a = self._decompose(soup=a, tag="sup")
                            a = self._decompose(soup=a, tag="institution-id")
                            # getting rid of ext-link eliminates *all* emails,
                            # so this is not the place to fix the iop thing
                            # a = self._decompose(soup=a, tag='ext-link')
                        except Exception:
                            pass
                        affstr = a.get_text().strip()
                        (affstr, email_list) = self._fix_affil(affstr)
                        if email_list:
                            self.email_xref[key] = email_list
                            email_list = []
                        self.xref_dict[key] = affstr
                    except Exception:
                        pass

            # now get the xref keys outside of contrib-group:
            # aff xrefs...
            try:
                aff_glob = article_metadata.find_all("aff")
            except Exception:
                aff_glob = None
            else:
                for a in aff_glob:
                    try:
                        key = a["id"]
                        # special case: get rid of <sup>...
                        try:
                            a = self._decompose(soup=a, tag="sup")
                        except Exception:
                            pass
                        try:
                            # NOTE: institution-id is actually useful, but at
                            # at the moment, strip it
                            a = self._decompose(soup=a, tag="institution-id")
                        except Exception:
                            pass
                        affstr = a.get_text(separator=" ").strip()
                        (aff_list, email_list) = self._fix_affil(affstr)
                        self.xref_dict[key] = aff_list
                        a.decompose()
                    except Exception:
                        logger.info("No aff id key in: %s", a)
                        pass

            # author-notes xrefs...
            try:
                authnote_glob = article_metadata.find_all("author-notes")
            except Exception:
                pass
            else:
                for a in authnote_glob:
                    try:
                        # emails...
                        cor = a.find_all("corresp")
                        for c in cor:
                            key = c["id"]
                            try:
                                c = self._decompose(soup=c, tag="sup")
                            except Exception:
                                pass
                            val = c.get_text(separator=" ").strip()
                            self.xref_dict[key] = val
                            c.decompose()
                    except Exception:
                        logger.info("No authnote id key in: %s", a)
                        pass

            # finishing up
            self._match_xref()
            return self.auth_list
        except Exception as err:
            raise JATSContribException(err)


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

    def _detag(self, r, tags_keep, **kwargs):
        """

        :param r: BeautifulSoup object (not string)
        :param tags_keep:
        :param kwargs:
        :return:
        """
        # note that parser=lxml is recommended here - if the more stringent lxml-xml is used,
        # the output is slightly different and the code will need to be modified
        newr = BeautifulSoup(str(r), "lxml")
        try:
            tag_list = list(set([x.name for x in newr.find_all()]))
        except Exception:
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

        newr = newr.replace(u"\n", u" ").replace(u"  ", u" ")
        newr = newr.replace("&nbsp;", " ")
        newr = newr.strip()

        return newr

    def _get_date(self, d):
        pubdate = self._detag(d.year, [])
        try:
            d.month
        except Exception:
            pubdate = pubdate + "-" + "00"
        else:
            try:
                int(self._detag(d.month, []))
            except Exception:
                month_name = self._detag(d.month, [])[0:3].lower()
                month = utils.MONTH_TO_NUMBER[month_name]
            else:
                month = self._detag(d.month, [])
            if int(month) < 10:
                month = "0" + str(int(month))
            else:
                month = str(month)
            pubdate = pubdate + "-" + month
        try:
            d.day
        except Exception:
            pubdate = pubdate + "-" + "00"
        else:
            try:
                int(self._detag(d.day, []))
            except Exception:
                day = "0"
            else:
                day = self._detag(d.day, [])
            if int(day) < 10:
                day = "0" + str(int(day))
            else:
                day = str(day)
            pubdate = pubdate + "-" + day

        return pubdate

    def _parse_title_abstract(self):
        title_xref_list = []
        title_fn_list = []
        try:
            title = self.article_meta.find("title-group").find("article-title")
        except Exception:
            pass
        else:
            try:
                for dx in title.find_all("ext-link"):
                    ## TODO in the original code, this seems to only be used for errata (later in the code) - is that right?
                    self.titledoi = dx["xlink:href"]
            except Exception:
                pass
            try:
                for dx in title.find_all("xref"):
                    title_xref_list.append(self._detag(dx, self.JATS_TAGSET["abstract"]).strip())
                    dx.decompose()
            except Exception:
                pass
            try:
                for df in title.find_all("fn"):
                    title_fn_list.append(self._detag(df, self.JATS_TAGSET["abstract"]).strip())
                    df.decompose()
            except Exception:
                pass
            self.base_metadata["title"] = self._detag(title, self.JATS_TAGSET["title"]).strip()

        try:
            abstract = self.article_meta.abstract.p
        except Exception:
            pass
        else:
            abstract = self._detag(abstract, self.JATS_TAGSET["abstract"])
            self.base_metadata["abstract"] = abstract
            if title_fn_list:
                self.base_metadata["abstract"] += "  " + " ".join(title_fn_list)

    def _parse_author(self):
        try:
            auth_affil = JATSAffils()
            aa_output = auth_affil.parse(article_metadata=self.article_meta)
        except Exception:
            pass
        else:
            self.base_metadata["authors"] = aa_output

    def _parse_copyright(self):
        try:
            copyright = self.article_meta.find("copyright-statement")
        except Exception:
            pass
        else:
            self.base_metadata["copyright"] = self._detag(copyright, [])

    def _parse_edhistory(self):
        # received and revised dates can be arrays, but accepted can just be a single value
        received = []
        revised = []
        try:
            dates = self.article_meta.find("history").find_all("date")
        except Exception:
            pass
        else:
            for d in dates:
                try:
                    date_type = d["date-type"]
                except Exception:
                    date_type = ""
                try:
                    eddate = self._get_date(d)
                except Exception:
                    pass
                else:
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
        try:
            keys_uat = []
            keys_misc = []
            keys_aas = []
            keys_out = []
            keyword_groups = self.article_meta.find_all("kwd-group")
            for kg in keyword_groups:
                # Check for UAT first:
                if kg["kwd-group-type"] == "author":
                    keys_uat_test = kg.find_all("compound-kwd-part")
                    for kk in keys_uat_test:
                        if kk["content-type"] == "uat-code":
                            keys_uat.append(self._detag(kk, self.JATS_TAGSET["keywords"]))
                    if not keys_uat:
                        keys_misc_test = kg.find_all("kwd")
                        for kk in keys_misc_test:
                            keys_misc.append(self._detag(kk, self.JATS_TAGSET["keywords"]))
                # Then check for AAS:
                elif kg["kwd-group-type"] == "AAS":
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
        except Exception:
            pass

        if "keywords" not in self.base_metadata:
            try:
                keywords = self.article_meta.find("article-categories").find_all("subj-group")
            except Exception:
                keywords = []
            for c in keywords:
                try:
                    if c["subj-group-type"] == "toc-minor":
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
                except Exception:
                    pass

    def _parse_pub(self):
        try:
            journal = self.journal_meta.find("journal-title-group").find("journal-title")
            self.base_metadata["publication"] = self._detag(journal, [])
        except Exception:
            try:
                journal = self.journal_meta.find("journal-title")
                self.base_metadata["publication"] = self._detag(journal, [])
            except Exception:
                pass

        try:
            publisher = self.journal_meta.find("publisher").find("publisher-name")
            self.base_metadata["publisher"] = self._detag(publisher, [])
        except Exception:
            pass

        try:
            issn_all = self.journal_meta.find_all("issn")
        except Exception:
            issn_all = []
        issns = []
        for i in issn_all:
            issns.append((i["pub-type"], self._detag(i, [])))
        self.base_metadata["issn"] = issns

    def _parse_related(self):
        # Related article data, especially corrections and errata

        try:
            related = self.article_meta.find_all("related-article")
            for r in related:
                # TODO are there other types of related articles to track? need an example
                if r["related-article-type"] == "corrected-article":
                    self.isErratum = True
                    relateddoi = r["xlink:href"]
        except Exception:
            pass

        if self.isErratum:
            try:
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
                    # TODO need to figure out an ID for this log statement
                    logger.warning("No DOI for erratum")
                    # pass
            except Exception as err:
                # TODO figure out an ID for this log statement
                logger.warning("Problem making erratum: %s", err)
                # pass

    def _parse_ids(self):

        self.base_metadata["ids"] = {}

        try:
            ids = self.article_meta.find_all("article-id")
        except Exception:
            ids = []

        self.base_metadata["ids"]["pub-id"] = []
        for d in ids:
            # DOI
            if d["pub-id-type"] == "doi":
                self.base_metadata["ids"]["doi"] = self._detag(d, [])

            # publisher ID
            if d["pub-id-type"] == "publisher-id":
                self.base_metadata["ids"]["pub-id"].append(
                    {"attribute": "publisher-id", "Identifier": self._detag(d, [])}
                )
            elif d["pub-id-type"] == "manuscript":
                self.base_metadata["ids"]["pub-id"].append(
                    {"attribute": "manuscript", "Identifier": self._detag(d, [])}
                )
            elif d["pub-id-type"] == "other":
                self.base_metadata["ids"]["pub-id"].append(
                    {"attribute": "other", "Identifier": self._detag(d, [])}
                )

        # Arxiv Preprint
        try:
            arxiv = self.article_meta.find_all("custom-meta")
        except Exception:
            pass
        else:
            # ax_pref = "https://arxiv.org/abs/"
            self.base_metadata["ids"]["preprint"] = {}
            for ax in arxiv:
                try:
                    x_name = self._detag(ax.find("meta-name"), [])
                    x_value = self._detag(ax.find("meta-value"), [])
                    if x_name == "arxivppt":
                        self.base_metadata["ids"]["preprint"].append(
                            {"source": "arxiv", "id": x_value}
                        )
                except Exception:
                    pass

    def _parse_pubdate(self):
        try:
            pub_dates = self.article_meta.find_all("pub-date")
        except Exception:
            pub_dates = []
        for d in pub_dates:
            try:
                a = d["publication-format"]
            except KeyError:
                a = ""
            try:
                b = d["pub-type"]
            except KeyError:
                b = ""
            try:
                pubdate = self._get_date(d)
            except Exception:
                pass
            else:
                if a == "print" or b == "ppub" or b == "cover":
                    self.base_metadata["pubdate_print"] = pubdate
                elif a == "electronic" or b == "epub":
                    self.base_metadata["pubdate_electronic"] = pubdate
            try:
                if b == "open-access":
                    self.base_metadata.setdefault("openAccess", {}).setdefault("open", True)
            except Exception:
                pass

    def _parse_permissions(self):
        # Check for open-access / "Permissions" field
        try:
            permissions = self.article_meta.find("permissions").find_all("license")
        except Exception:
            pass
        else:
            for p in permissions:
                try:
                    if p["license-type"] == "open":
                        self.base_metadata.setdefault("openAccess", {}).setdefault("open", True)
                except Exception:
                    pass

    def _parse_page(self):
        fpage = self.article_meta.fpage
        if fpage is None:
            fpage = self.article_meta.pageStart
        if fpage:
            self.base_metadata["page_first"] = self._detag(fpage, [])

        try:
            tmp = self.article_meta.find("elocation-id")
            self.base_metadata["electronic_id"] = self._detag(tmp, [])
        except Exception:
            pass

        lpage = self.article_meta.lpage
        if lpage is None:
            lpage = self.article_meta.pageEnd

        else:
            if lpage == fpage:
                lpage = None
        if lpage:
            self.base_metadata["page_last"] = self._detag(lpage, [])

        if fpage and lpage:
            self.base_metadata["page_range"] = (
                self._detag(fpage, []) + "-" + (self._detag(lpage, []))
            )

        # Number of Pages:
        try:
            counts = self.article_meta.counts
            pagecount = counts.find("page-count")
            self.base_metadata["numpages"] = pagecount["count"]
        except Exception:
            pass

    def _parse_references(self):
        if self.back_meta is not None:

            ref_list_text = []
            try:
                ref_results = self.back_meta.find("ref-list").find_all("ref")
                for r in ref_results:
                    # s = r.extract().get_text().replace("\n", " ")
                    # s = re.sub(r"\s+", r" ", s)
                    # s = namedentities.named_entities(s.strip())
                    # output raw XML for reference service to parse later
                    s = str(r.extract()).replace("\n", " ")
                    ref_list_text.append(s)
            except Exception:
                pass
            else:
                self.base_metadata["references"] = ref_list_text

    def entity_convert(self):
        econv = utils.EntityConverter()
        for k, v in self.base_metadata.items():
            if isinstance(v, str):
                econv.input_text = v
                econv.convert()
                v = econv.output_text
            elif isinstance(v, list):
                newv = []
                for i in v:
                    if isinstance(i, str):
                        econv.input_text = i
                        econv.convert()
                        i = econv.output_text
                    newv.append(i)
                v = newv
            else:
                pass
            self.base_metadata[k] = v

    def parse(self, text):

        d = self.bsstrtodict(text, parser="lxml-xml")
        document = d.article

        front_meta = document.front
        self.back_meta = document.back

        try:
            self.article_meta = front_meta.find("article-meta")
            self.journal_meta = front_meta.find("journal-meta")
        except Exception:
            return {}

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

        self.entity_convert()

        output = serializer.serialize(self.base_metadata, format="JATS")

        return output

    def add_fulltext(self):
        pass

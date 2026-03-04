import logging
import re
from collections import OrderedDict
from copy import copy

import bs4
import validators
from ordered_set import OrderedSet

from adsingestp import utils
from adsingestp.ingest_exceptions import XmlLoadException
from adsingestp.parsers.base import BaseBeautifulSoupParser

logger = logging.getLogger(__name__)


class JATSNew(object):
    regex_email = re.compile(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+")
    regex_auth_xid = re.compile(r"^A[0-9]+$")

    def __init__(self, soup=None, raw=None):
       
        self.raw = raw
        self.soup = soup
        self.xref_dict = {}
        self.affil_dict = {}
        if self.raw and not self.soup:
            self.soup = bs4.BeautifulSoup(self.raw, "lxml-xml")
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

    def _extract_all_xref(self):
        xref_map = {"bibr": "ref"}
        self.xref_dict = {}
        self.affil_dict = {}
        
        try:
            # generates a dict of every xref you're going to *need*
            all_xref = self.soup.find_all("xref")
            xref_dict = {}
            for x in all_xref:
                xtype = x.get("ref-type", "")
                xid = x.get("rid", "").split()
                for i in xid:
                    if xref_dict.get(xtype, None):
                        if i not in xref_dict.get(xtype):
                            xref_dict[xtype].append(i)
                    else:
                        xref_dict[xtype] = [i]

            affil_dict = {}
            for xtype in xref_dict.keys():
                xmap = xref_map.get(xtype, xtype)
                mumu = self.soup.find_all(xmap)
                if xmap == "aff":
                    for aff in mumu:
                        affil_dict.update(self._process_aff_tag(aff))

            self.xref_dict = xref_dict
            self.affil_dict = affil_dict

        except Exception as err:
            print("well, shit... %s" % err)

    def _process_aff_tag(self, afftag):
        try:
            affid = afftag.get("id", "none")

            instid_dict = {}
            #does the affil have ids like ROR?
            instid = afftag.find_all("institution-id")
            for iid in instid:
                itype = iid.get("institution-id-type","")
                ivalu = iid.get_text()
                instid_dict[itype] = ivalu
                iid.decompose()
            # get string after removing the institution-id(s)
            affstring = afftag.get_text()
            return {affid: {"affstring": affstring, "instid": instid_dict}}
        except Exception as err:
            print("Like OMG, %s" % err)

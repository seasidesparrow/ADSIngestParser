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

    def __init__(self, soup=None, raw= None):
       
        self.raw = raw
        self.soup = soup
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
            for xtype in xref_dict.keys():
                xmap = xref_map.get(xtype, xtype)
                mumu = self.soup.find_all(xmap)
                print("Yay!  %s tags of type %s" % (len(mumu), xmap))
                print("ID: %s" % ", ".join(xref_dict[xtype]))
        except Exception as err:
            print("well, shit... %s" % err)

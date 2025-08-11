from adsingestp.parsers.base import BaseBeautifulSoupParser
from adsingestp.ingest_exceptions import *

class JAFFParser(BaseBeautifulSoupParser):

    def __init__(self):
        self.soup = None
        self.xref_dict = {}
        pass


    def _extract_xref(self):
        try:
            all_xrefs = self.soup.find_all("xref")
            print(all_xrefs)
        except:
            pass
        return


    def parse(self, data, bsparser="lxml-xml"):
        try:
            self.soup = self.bsstrtodict(data, parser=bsparser)
        except Exception as err:
            raise XmlLoadException(err)

        self._extract_xref()

        print(self.xref_dict)


        

        

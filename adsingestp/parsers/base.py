import bs4
import xmltodict as xmltodict_parser


class BaseXmlToDictParser(object):
    """
    An XML parser which uses xmltodict to create a dictionary
    out of the input XML stream
    """

    def xmltodict(self, input_xml):
        """returns a dict as created by xmltodict
        :param input_xml: XML text blob
        """
        return xmltodict_parser.parse(input_xml)

    def _array(self, e):
        """Ensures that e is an array"""
        if not e:
            return []
        elif isinstance(e, list):
            return e
        else:
            return [e]

    def _dict(self, e):
        """Ensures that e is a dictionary"""
        if not e:
            return {}
        elif isinstance(e, dict):
            return e
        else:
            return {}

    def _text(self, e):
        """Returns text node of element e, or an empty string"""
        if not e:
            return ""
        elif isinstance(e, dict):
            return e.get("#text", "")
        elif isinstance(e, str):
            return e

    def _attr(self, e, a, d=""):
        """Returns attribute a from element e, or an empty string"""
        if not e:
            return d
        elif isinstance(e, dict):
            return e.get("@" + a, d)
        elif isinstance(e, str):
            return d
        else:
            return d


class BaseBeautifulSoupParser(object):
    """
    An XML parser which uses BeautifulSoup to create a dictionary
    out of the input XML stream.
    """

    def bsstrtodict(self, input_xml, parser="lxml-xml"):
        """
        Returns a BeautifulSoup tree given an XML text
        :param input_xml: XML text blob
        :param parser: e.g. 'html.parser', 'html5lib', 'lxml-xml' (default)
        :return: BeautifulSoup object/tree
        """

        return bs4.BeautifulSoup(input_xml, parser)

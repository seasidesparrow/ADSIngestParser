import bs4

class BaseBeautifulSoupParser(object):
    """
    An XML parser which uses BeautifulSoup to create a dictionary
    out of the input XML stream.
    """

    def bsstrtodict(self, r, parser='lxml-xml', **kwargs):
        """
        Returns a BeautifulSoup tree given an XML text
        :param r:
        :param parser: e.g. 'html.parser', 'html5lib', 'lxml' (default)
        :param kwargs:
        :return:
        """
        """"""

        return bs4.BeautifulSoup(r, parser, **kwargs)
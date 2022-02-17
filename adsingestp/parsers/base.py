import bs4

class BaseBeautifulSoupParser(object):
    """
    An XML parser which uses BeautifulSoup to create a dictionary
    out of the input XML stream.
    """

    def bsfiletodict(self, fp, parser='lxml', **kwargs):
        """
        Returns a BeautifulSoup tree

        :param fp:
        :param parser: e.g. 'html.parser', 'html5lib', 'lxml' (default)
        :param kwargs:
        :return:
        """
        ## TODO what is fp? I think it's the file path - should modify because we're not reading files here, just input text blobs
        return bs4.BeautifulSoup(fp.read(), parser, **kwargs)

    def bsstrtodict(self, r, parser='lxml', **kwargs):
        """
        Returns a BeautifulSoup tree given an XML text
        :param r:
        :param parser: e.g. 'html.parser', 'html5lib', 'lxml' (default)
        :param kwargs:
        :return:
        """
        """"""

        return bs4.BeautifulSoup(r, parser, **kwargs)
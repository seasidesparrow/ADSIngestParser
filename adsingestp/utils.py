import logging
import os
import re

import nameparser
import unidecode
from namedentities import named_entities, unicode_entities

from adsingestp.ingest_exceptions import UnicodeHandlerError

re_ents = re.compile(r"&[a-z0-9]+;|&#[0-9]{1,6};|&#x[0-9a-fA-F]{1,6};")

MONTH_TO_NUMBER = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# HTML_ENTITY_TABLE
HTML_ENTITY_TABLE = os.path.dirname(os.path.abspath(__file__)) + "/html5.dat"
ENTITY_DICTIONARY = dict()
try:
    with open(HTML_ENTITY_TABLE, "r") as fent:
        for line in fent.readlines():
            carr = line.rstrip().split("\t")

            UNI_ENTITY = None
            NAME_ENTITY = None
            HEX_ENTITY = None
            DEC_ENTITY = None
            if len(carr) >= 4:
                UNI_ENTITY = carr[0]
                NAME_ENTITY = carr[1]
                HEX_ENTITY = carr[2].lower()
                DEC_ENTITY = carr[3].lower()
                for c in NAME_ENTITY.strip().split():
                    # preserve greek letters, convert all other high-bit chars
                    eValue = int(DEC_ENTITY.lstrip("&#").rstrip(";"))
                    if (eValue >= 913 and eValue <= 969) or (eValue >= 192 and eValue <= 382):
                        ENTITY_DICTIONARY[UNI_ENTITY.strip()] = c.strip()
                        ENTITY_DICTIONARY[HEX_ENTITY.strip()] = c.strip()
                        ENTITY_DICTIONARY[DEC_ENTITY.strip()] = c.strip()
                    else:
                        ENTITY_DICTIONARY[UNI_ENTITY.strip()] = DEC_ENTITY.strip()
                        ENTITY_DICTIONARY[HEX_ENTITY.strip()] = DEC_ENTITY.strip()
                        ENTITY_DICTIONARY[c.strip()] = DEC_ENTITY.strip()
            else:
                print("broken HTML entity:", line.rstrip())
                NAME_ENTITY = "xxxxx"

except Exception as e:
    print("Problem in config:", e)

# ADS-specific translations
# have been added to html5.txt
ENTITY_DICTIONARY["&sim;"] = "~"
ENTITY_DICTIONARY["&#8764;"] = "~"
ENTITY_DICTIONARY["&Tilde;"] = "~"
ENTITY_DICTIONARY["&rsquo;"] = "'"
ENTITY_DICTIONARY["&#8217;"] = "'"
ENTITY_DICTIONARY["&lsquo;"] = "'"
ENTITY_DICTIONARY["&#8216;"] = "'"
ENTITY_DICTIONARY["&nbsp;"] = " "
ENTITY_DICTIONARY["&mdash;"] = "-"
ENTITY_DICTIONARY["&#8212;"] = "-"
ENTITY_DICTIONARY["&ndash;"] = "-"
ENTITY_DICTIONARY["&#8211;"] = "-"
ENTITY_DICTIONARY["&rdquo;"] = '"'
ENTITY_DICTIONARY["&#8221;"] = '"'
ENTITY_DICTIONARY["&ldquo;"] = '"'
ENTITY_DICTIONARY["&#8220;"] = '"'
ENTITY_DICTIONARY["&minus;"] = "-"
ENTITY_DICTIONARY["&#8722;"] = "-"
ENTITY_DICTIONARY["&plus;"] = "+"
ENTITY_DICTIONARY["&#43;"] = "+"
ENTITY_DICTIONARY["&thinsp;"] = " "
ENTITY_DICTIONARY["&#8201;"] = " "
ENTITY_DICTIONARY["&hairsp;"] = " "
ENTITY_DICTIONARY["&#8202;"] = " "
ENTITY_DICTIONARY["&ensp;"] = " "
ENTITY_DICTIONARY["&#8194;"] = " "
ENTITY_DICTIONARY["&emsp;"] = " "
ENTITY_DICTIONARY["&#8195;"] = " "

# TODO have MT review this list
related_trans_dict = {
    "IsCitedBy": "related",
    "IsSupplementTo": "supplement",
    "IsContinuedBy": "series",
    "IsDescribedBy": "related",
    "HasMetadata": "related",
    "HasVersion": "related",
    "IsNewVersionOf": "updated",
    "IsPartOf": "related",
    "IsReferencedBy": "related",
    "IsDocumentedBy": "related",
    "IsCompiledBy": "related",
    "IsVariantFormOf": "related",
    "IsIdenticalTo": "related",
    "IsReviewedBy": "related",
    "IsDerivedFrom": "related",
    "Requires": "related",
    "IsObsoletedBy": "related",
    "Cites": "related",
    "IsSupplementedBy": "related",
    "Continues": "series",
    "Describes": "related",
    "IsMetadataFor": "related",
    "IsVersionOf": "related",
    "PreviousVersionOf": "related",
    "HasPart": "related",
    "References": "related",
    "Documents": "related",
    "Compiles": "related",
    "IsOriginalFormOf": "related",
    "*": "related",
    "Reviews": "related",
    "IsSourceOf": "related",
    "IsRequiredBy": "related",
    "Obsoletes": "related",
    "Erratum": "errata",
}


def u2asc(input):
    """
    Converts/transliterates unicode characters to ASCII, using the unidecode package.
    Functionality is similar to the legacy code in adspy.Unicode, but may treat some characters differently
    (e.g. umlauts). Standard unidecode package only handles Latin-based characters.
    :param input: string to be transliterated. Can be either unicode or encoded in utf-8
    :return output: transliterated string, in either unicode or encoded (to match input)
    """

    # TODO If used on anything but author names, add special handling for math symbols and other special chars
    if not isinstance(input, str):
        try:
            input = input.decode("utf-8")
        except UnicodeDecodeError:
            raise UnicodeHandlerError("Input must be either unicode or encoded in utf8.")

    try:
        output = unidecode.unidecode(input)
    except UnicodeDecodeError:
        raise UnicodeHandlerError("Transliteration failed, check input.")

    if not isinstance(input, str):
        output = output.encode("utf-8")

    return output


class EntityConverter(object):
    def __init__(self):
        self.input_text = ""
        self.output_text = ""
        self.ent_dict = ENTITY_DICTIONARY

    def convert(self):
        o = named_entities(self.input_text)
        oents = list(dict.fromkeys(re.findall(re_ents, o)))

        for e in oents:
            try:
                enew = self.ent_dict[e]
            except Exception:
                pass
            else:
                o = re.sub(e, enew, o)
        self.output_text = o


class AuthorNames(object):
    """
    Author names parser
    """

    default_collaborations_params = {
        "keywords": ["group", "team", "collaboration", "consortium"],
        "first_author_delimiter": ":",
        "remove_the": True,
        "fix_arXiv_mixed_collaboration_string": False,
    }

    def __init__(self):
        self.max_first_name_initials = 6
        self.dutch_last_names = ["van", "von", "'t", "den", "der", "van't"]

        data_dirname = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "data_files", "author_names"
        )
        logging.info("Loading ADS author names from: %s", data_dirname)
        self.first_names = self._read_datfile(os.path.join(data_dirname, "first.dat"))
        self.last_names = self._read_datfile(os.path.join(data_dirname, "last.dat"))
        prefix_names = self._read_datfile(os.path.join(data_dirname, "prefixes.dat"))
        suffix_names = self._read_datfile(os.path.join(data_dirname, "suffixes.dat"))

        try:
            for s in prefix_names:
                nameparser.config.CONSTANTS.titles.add(s)
            for s in suffix_names:
                nameparser.config.CONSTANTS.suffix_acronyms.add(s)
                nameparser.config.CONSTANTS.suffix_not_acronyms.add(s)
        except Exception as e:
            logging.exception("Unexpected error setting up the nameparser")
            raise BaseException(e)

        # Setup the nameparser package and remove *all* of the preset titles:
        nameparser.config.CONSTANTS.titles.remove(*nameparser.config.CONSTANTS.titles)
        nameparser.config.CONSTANTS.suffix_acronyms.remove(
            *nameparser.config.CONSTANTS.suffix_acronyms
        )
        nameparser.config.CONSTANTS.suffix_not_acronyms.remove(
            *nameparser.config.CONSTANTS.suffix_not_acronyms
        )
        nameparser.config.CONSTANTS.string_format = (
            '{last}, {first} "{nickname}", {suffix}, {title}'
        )

        # Compile regular expressions
        self.regex_initial = re.compile(r"\. *(?!,)")
        self.regex_etal = re.compile(r",? et ?al\.?")
        self.regex_and = re.compile(r" and ")
        self.regex_dash = re.compile(r"^-")
        self.regex_quote = re.compile(r"^'")
        self.regex_the = re.compile(r"^[Tt]he ")
        self.regex_author = re.compile(
            r"^(?P<last_name>[^,]+),\s*(?P<initial0>\S)\w*"
            + "".join(
                [
                    r"(?:\s*(?P<initial{}>\S)\S*)?".format(i + 1)
                    for i in range(self.max_first_name_initials - 1)
                ]
            )
        )

        # Default unknown author
        self.unknown_author_str = ""

    def _read_datfile(self, filename):
        output_list = []

        try:
            fp = open(filename, "r")
        except Exception as err:
            # TODO fix logging
            logging.exception("Error reading file: %s. Error: %s", filename, err)
        else:
            with fp:
                for line in fp.readlines():
                    if line.strip() != "" and line[0] != "#":
                        output_list.append(line.strip())
        return output_list

    # def normalize(self, authors_str, delimiter=';', collaborations_params=default_collaborations_params):
    #     """
    #     Normalizes a string of author names separated by some delimiter
    #     :param authors_str: Input string of one or more author names
    #     :param delimiter: Delimiter that separates two author names
    #     :param collaborations_params: Dict of collaborations params/config; should include 'keywords' (list),
    #     'first_author_delimiter' (string), 'remove_the' (boolean), 'fix_arXiv_mixed_collaboration_string' (boolean)
    #
    #     :return author names, normalized, separated by the given delimiter plus a space
    #     """
    #     normalized_authors_list = []
    #
    #     for author_str in authors_str.split(delimiter):
    #         normalized_authors_list.append(self._normalize_author(author_str, collaborations_params))
    #     return (delimiter + ' ').join(normalized_authors_list)

    # def _normalize_author(self, author_str, collaborations_params=default_collaborations_params):
    #     """
    #     Normalizes an author name string ensuring capitalization and
    #     transforming first name to only initials
    #     :param author_str
    #     :param collaborations_params
    #     """
    #     try:
    #         # TODO do we still need to convert to ASCII??
    #         # Transliterates unicode characters to ASCII
    #         author_str = u2asc(author_str.strip())
    #     except Exception:
    #         logging.exception("Unexpected error transliterating author name\
    #                            unicode string to ASCII")
    #         # TODO: Implement better error control
    #         return self._normalize_author(self.unknown_author_str,
    #                                       collaborations_params)
    #
    #     # Check first if it is a collaboration, given that collaboration strings
    #     # may have commas and it may be wrongly interpreted as a name
    #     collaboration = False
    #     for keyword in collaborations_params['keywords']:
    #         if keyword in author_str.lower():
    #             collaboration = True
    #             break
    #     if collaboration:
    #         # Make sure there are no commas to avoid interpreting this name as 'last, first name'
    #         normalized_author_str = author_str.replace(",", "")
    #     else:
    #         match = self.regex_author.search(author_str)
    #         if match:
    #             # Last name detected
    #             ## Using .title() breaks dutch last names!
    #             # last_name = match.group('last_name').strip().title()
    #             last_name = match.group('last_name').strip()
    #             initials_list = []
    #             # Collect initials from first name if it is present
    #             for i in range(self.max_first_name_initials):
    #                 key = 'initial' + str(i)
    #                 if match.group(key):
    #                     initials_list.append(match.group(key).strip().upper())
    #             initials_str = " ".join(initials_list)
    #             # Form normalized author string where capitalization is guaranteed
    #             normalized_author_str = "{}, {}".format(last_name, initials_str)
    #             # Make sure there are no dots
    #             normalized_author_str = normalized_author_str.replace(".", "")
    #         else:
    #             # Make sure there are no commas to avoid interpreting this
    #             # name as 'last, first name'
    #             normalized_author_str = author_str.replace(",", "")
    #             # Make sure there are no dots or commas
    #             normalized_author_str = normalized_author_str.replace(".", "")
    #
    #     normalized_author_str = normalized_author_str.strip()
    #     if len(normalized_author_str) == 0:
    #         normalized_author_str = self.normalized_unknown_author_str
    #     return normalized_author_str

    def _extract_collaboration(
        self, collaboration_str, default_to_last_name, delimiter, collaborations_params
    ):
        """
        Verifies if the author name string contains a collaboration string
        The collaboration extraction can be controlled by the dictionary
        'collaborations_params'.
        """
        corrected_collaboration_str = ""  # Default
        try:
            for keyword in collaborations_params["keywords"]:
                if keyword in collaboration_str.lower():
                    collaboration_str = re.sub(keyword, keyword.capitalize(), collaboration_str)
                    if collaborations_params["remove_the"]:
                        corrected_collaboration_str = self.regex_the.sub("", collaboration_str)
                    else:
                        corrected_collaboration_str = collaboration_str

                    if collaborations_params["fix_arXiv_mixed_collaboration_string"]:
                        # TODO: Think a better way to account for this
                        # specific cases if there's a ',' in the string,
                        # it probably includes the 1st author
                        string_list = corrected_collaboration_str.split(",")
                        if len(string_list) == 2:
                            # Based on an arXiv author case: "collaboration,
                            # Gaia"
                            string_list.reverse()
                            corrected_collaboration_str = " ".join(string_list)

                    if collaborations_params["first_author_delimiter"]:
                        # Based on an arXiv author case: "<tag>Collaboration:
                        # Name, Author</tag>"
                        authors_list = corrected_collaboration_str.split(
                            collaborations_params["first_author_delimiter"]
                        )
                        corrected_authors_list = []
                        for author in authors_list:
                            if keyword in author.lower():
                                corrected_authors_list.append(author.strip())
                            else:
                                corrected_authors_list.append(
                                    self._parse_author_name(author.strip(), default_to_last_name)
                                )
                        corrected_collaboration_str = (
                            (delimiter + " ").join(corrected_authors_list).strip()
                        )
                    break
        except Exception:
            logging.exception("Unexpected error in collaboration checks")
        is_collaboration_str = corrected_collaboration_str != ""
        return is_collaboration_str, corrected_collaboration_str

    def _clean_author_name(self, author_str):
        """
        Remove useless characters in author name string
        """
        author_str = self.regex_initial.sub(". ", author_str)
        author_str = self.regex_etal.sub("", author_str)
        author_str = self.regex_and.sub(" ", author_str)
        author_str = author_str.replace(" .", ".").replace("  ", " ").replace(" ,", ",")
        author_str = author_str.strip()
        return author_str

    def _parse_author_name(self, author_str, default_to_last_name):
        """
        Automatically detect first and last names in an author name string and parse into parts
        :param author_str: raw author name string
        :param default_to_last_name: boolean; if true, ambiguous middle names added to last name, if false, kept as middle name

        :return dict of parsed author name parts
        """
        author = nameparser.HumanName(author_str)
        if author.first == "Jr." and author.suffix != "":
            author.first = author.suffix
            author.suffix = "Jr."

        if author.middle:
            # Move middle names to first name if detected as so,
            # or move to last name if detected as so
            # or move to the default
            keep_as_middle = []
            add_to_last = []
            last_name_found = False

            middle_name_list = author.middle.split()

            try:
                for middle_name in middle_name_list:
                    middle_name_length = len(
                        unicode_entities(middle_name).strip(".").strip("-")
                    )  # Ignore '.' or '-' at the beginning/end of the string
                    middle_name_upper = middle_name.upper()
                    if (
                        (
                            middle_name_length <= 2
                            and middle_name_upper not in self.last_names
                            and "'" not in middle_name
                        )
                        or (
                            middle_name_upper in self.first_names
                            and middle_name_upper not in self.last_names
                        )
                        or (
                            self.regex_dash.sub("", middle_name_upper) in self.first_names
                            and self.regex_dash.sub("", middle_name_upper) not in self.last_names
                        )
                        or (
                            self.regex_quote.sub("", middle_name_upper) in self.first_names
                            and self.regex_quote.sub("", middle_name_upper) not in self.last_names
                        )
                    ):
                        # Case: First name found
                        # Middle name is found in the first names ADS
                        # list and not in the last names ADS list
                        if last_name_found:
                            # Move all previously detected first names to
                            # last name since we are in a situation where
                            # we detected:
                            # middle name: L F
                            # hence we correct it to:
                            # middle name: F F
                            # where F is first name and L is last name
                            keep_as_middle += add_to_last
                            add_to_last = []
                            last_name_found = False
                        keep_as_middle.append(middle_name)
                    elif last_name_found or middle_name.upper() in self.last_names:
                        # Case: Last name found
                        add_to_last.append(middle_name)
                        last_name_found = True
                    else:
                        # Case: Unknown
                        # Middle name not found in the first or last names ADS list
                        if default_to_last_name:
                            add_to_last.append(middle_name)
                            last_name_found = True
                        else:
                            keep_as_middle.append(middle_name)
            except Exception:
                logging.exception("Unexpected error in middle name parsing")
            author.middle = " ".join(keep_as_middle)
            # [MT 2020 Oct 07, can't reproduce where .reverse() is necessary?]
            # add_to_last.reverse()
            author.last = " ".join(add_to_last) + author.last

        # Verify that no first names appear in the detected last name
        if author.last:
            if isinstance(author.last, str):
                last_name_list = [author.last]
            else:
                last_name_list = author.last.split()
            # At this point we already know it has at least 1 last name and
            # we will not question that one (in the last position)
            verified_last_name_list = [last_name_list.pop()]
            last_name_list.reverse()
            try:
                for last_name in last_name_list:
                    last_name_upper = last_name.upper()
                    if (
                        last_name_upper in self.first_names
                        and last_name_upper not in self.last_names
                    ):
                        author.middle = [author.middle, last_name]
                    else:
                        verified_last_name_list.append(last_name)
            except Exception:
                logging.exception("Unexpected error in last name parsing")
            else:
                verified_last_name_list.reverse()
                author.last = verified_last_name_list

        parsed_author = {}
        try:
            parsed_author["given"] = unicode_entities(author.first).replace("  ", " ")
            parsed_author["middle"] = unicode_entities(author.middle).replace("  ", " ")
            parsed_author["surname"] = unicode_entities(author.last).replace("  ", " ")
            parsed_author["suffix"] = unicode_entities(author.suffix).replace("  ", " ")
            parsed_author["prefix"] = unicode_entities(author.title).replace("  ", " ")
            parsed_author["nameraw"] = unicode_entities(author_str).replace("  ", " ")
        except Exception:
            logging.exception("Unexpected error converting detected name into a string")
            # TODO: Implement better error control
            parsed_author["nameraw"] = unicode_entities(author_str).replace("  ", " ")
        return parsed_author

    def parse(
        self,
        authors_str,
        delimiter=";",
        default_to_last_name=True,
        collaborations_params=default_collaborations_params,
    ):
        """
        Receives an authors string with individual author names separated by a
        delimiter and returns re-formatted authors string where all author
        names follow the structure: last name, first name

        It also verifies if an author name string contains a collaboration
        string.  The collaboration extraction can be controlled by the
        dictionary 'collaborations_params' which can have the following keys:

        - keywords [list of strings]: Keywords that appear in strings that
          should be identifier as collaboration strings. Default: 'group',
          'team', 'collaboration'
        - remove_the [boolean]: Remove the article 'The' from collaboration
          strings (e.g., 'The collaboration'). Default: False.
        - first_author_delimiter [string]: Some collaboration strings include
          the first author separated by a delimiter (e.g., The collaboration:
          First author), the delimiter can be specified in this variable,
          otherwise None or False values can be provided to avoid trying to
          extract first authors from collaboration strings. Default: ':'
        - fix_arXiv_mixed_collaboration_string [boolean]: Some arXiv entries
          mix the collaboration string with the collaboration string.
          (e.g. 'collaboration, Gaia'). Default: False
        """

        # Split and convert unicode characters and numerical HTML
        # (e.g. 'u'both em\u2014and&#x2013;dashes&hellip;' -> 'both em&mdash;and&ndash;dashes&hellip;')
        authors_list = [str(named_entities(n.strip())) for n in authors_str.split(delimiter)]

        corrected_authors_list = []
        for author_str in authors_list:
            author_str = self._clean_author_name(author_str)
            # Check for collaboration strings
            is_collaboration, collaboration_str = self._extract_collaboration(
                author_str, default_to_last_name, delimiter, collaborations_params
            )
            if is_collaboration:
                # Collaboration strings can contain the first author, which we need to split
                for corrected_author_str in collaboration_str.split(delimiter):
                    corrected_authors_list.append(corrected_author_str.strip())
            else:
                corrected_authors_list.append(
                    self._parse_author_name(author_str, default_to_last_name)
                )

        # Last minute global corrections due to manually detected problems in
        # our processing corrected_authors_str =
        # corrected_authors_str.replace(' ,', ',').replace('  ', ' ').
        # replace('. -', '.-')
        # TODO do I need these? make a cleaning func if so
        # corrected_authors_str = corrected_authors_str.replace(', , ', ', ')
        # corrected_authors_str = corrected_authors_str.replace(' -', '-').replace(' ~', '~')

        return corrected_authors_list

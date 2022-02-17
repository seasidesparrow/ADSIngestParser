import re
import os
from namedentities import named_entities

re_ents = re.compile(r'&[a-z0-9]+;|&#[0-9]{1,6};|&#x[0-9a-fA-F]{1,6};')

MONTH_TO_NUMBER = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                   'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11,
                   'dec': 12}

# HTML_ENTITY_TABLE
HTML_ENTITY_TABLE = os.path.dirname(os.path.abspath(__file__)) + '/html5.dat'
ENTITY_DICTIONARY = dict()
try:
    with open(HTML_ENTITY_TABLE, 'r') as fent:
        for l in fent.readlines():
            carr = l.rstrip().split('\t')

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
                    eValue = int(DEC_ENTITY.lstrip('&#').rstrip(';'))
                    if (eValue >= 913 and eValue <= 969) or (eValue >= 192 and eValue <= 382):
                        ENTITY_DICTIONARY[UNI_ENTITY.strip()] = c.strip()
                        ENTITY_DICTIONARY[HEX_ENTITY.strip()] = c.strip()
                        ENTITY_DICTIONARY[DEC_ENTITY.strip()] = c.strip()
                    else:
                        ENTITY_DICTIONARY[UNI_ENTITY.strip()] = DEC_ENTITY.strip()
                        ENTITY_DICTIONARY[HEX_ENTITY.strip()] = DEC_ENTITY.strip()
                        ENTITY_DICTIONARY[c.strip()] = DEC_ENTITY.strip()
            else:
                print("broken HTML entity:", l.rstrip())
                NAME_ENTITY = "xxxxx"

except Exception as e:
    print("Problem in config:", e)

# ADS-specific translations
# have been added to html5.txt
ENTITY_DICTIONARY['&sim;'] = "~"
ENTITY_DICTIONARY['&#8764;'] = "~"
ENTITY_DICTIONARY['&Tilde;'] = "~"
ENTITY_DICTIONARY['&rsquo;'] = "'"
ENTITY_DICTIONARY['&#8217;'] = "'"
ENTITY_DICTIONARY['&lsquo;'] = "'"
ENTITY_DICTIONARY['&#8216;'] = "'"
ENTITY_DICTIONARY['&nbsp;'] = " "
ENTITY_DICTIONARY['&mdash;'] = "-"
ENTITY_DICTIONARY['&#8212;'] = "-"
ENTITY_DICTIONARY['&ndash;'] = "-"
ENTITY_DICTIONARY['&#8211;'] = "-"
ENTITY_DICTIONARY['&rdquo;'] = '"'
ENTITY_DICTIONARY['&#8221;'] = '"'
ENTITY_DICTIONARY['&ldquo;'] = '"'
ENTITY_DICTIONARY['&#8220;'] = '"'
ENTITY_DICTIONARY['&minus;'] = "-"
ENTITY_DICTIONARY['&#8722;'] = "-"
ENTITY_DICTIONARY['&plus;'] = "+"
ENTITY_DICTIONARY['&#43;'] = "+"
ENTITY_DICTIONARY['&thinsp;'] = " "
ENTITY_DICTIONARY['&#8201;'] = " "
ENTITY_DICTIONARY['&hairsp;'] = " "
ENTITY_DICTIONARY['&#8202;'] = " "
ENTITY_DICTIONARY['&ensp;'] = " "
ENTITY_DICTIONARY['&#8194;'] = " "
ENTITY_DICTIONARY['&emsp;'] = " "
ENTITY_DICTIONARY['&#8195;'] = " "

class EntityConverter(object):

    def __init__(self):
        self.input_text = ''
        self.output_text = ''
        self.ent_dict = ENTITY_DICTIONARY

    def convert(self):
        o = named_entities(self.input_text)
        oents = list(dict.fromkeys(re.findall(re_ents, o)))

        for e in oents:
            try:
                enew = self.ent_dict[e]
            except:
                pass
            else:
                o = re.sub(e, enew, o)
        self.output_text = o
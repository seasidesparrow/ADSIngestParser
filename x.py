import json
from adsingestp.parsers.jats import JATSParser
#infile = "./tests/stubdata/input/jats_springer_EPJC_s10052-023-11699-1.xml"
#infile = "/Users/mtemple/jft/chinese/11_kjkxxb-45-1-135.xml"
infile = "/Users/mtemple/jft/french/Non_OA_cjfr-2024-0035.xml"


with open(infile, "r") as fd:
    data = fd.read()


p = JATSParser()
q = p.parse(data)
with open("piffol.json", "w") as fp:
     fp.write("%s\n" % json.dumps(q, indent=2, sort_keys=True))

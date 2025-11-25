import json
from adsingestp.parsers.ieee import IEEEJournalParser, IEEEParser
infile = "tests/stubdata/input/ieee_example_1.xml"


with open(infile, "r") as fx:
    raw = fx.read()

#try:
p = IEEEParser()
output = p.parse(raw)
with open("woo.json", "w") as fj:
    fj.write("%s\n" % json.dumps(output, indent=2, sort_keys=True))
#except Exception as err:
    #print("Well, shit: %s" % err)

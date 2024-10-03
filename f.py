import json
from adsingestp.parsers.jats import JATSParser

with open("aa49385-24.xml", "r") as fi:
    raw = fi.read()


jp = JATSParser()
output = jp.parse(raw)

with open("out.json", "w") as fo:
    fo.write("%s\n" % json.dumps(output, indent=2, sort_keys=True))

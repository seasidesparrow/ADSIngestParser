import json
from adsingestp.parsers.jats_current import JATSParser


with open("tests/stubdata/input/jats_edp_jnwpu_40_96.xml", "r") as fd:
    data = fd.read()

parser = JATSParser()
output = parser.parse(data)
with open("./jats_edp_jnwpu_40_96_current.json", "w") as fw:
    fw.write("%s\n" % json.dumps(output, sort_keys=True, indent=2))

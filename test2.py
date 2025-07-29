import json
from adsingestp.parsers.bits import BITSParser
from glob import glob


infiles = glob("/Users/mtemple/work/bits_files/*/978-981-96-0191-2_BookFrontMatter_nlm.xml.Meta")

output = []
for f in infiles:
    with open(f, "r") as fd:
        data = fd.read()
        p = BITSParser()
        parsed = p.parse(data)
        output.append(parsed)

with open("out2.json", "w") as fw:
    fw.write("%s\n" % json.dumps(output, indent=2, sort_keys=True))

import json
from glob import glob
from adsingestp.parsers.bits import BITSParser

searchfile = "/Users/mtemple/work/bits_files/appendix_b/*Meta"
files = glob(searchfile)


for f in files:
    with open(f, "r") as fr:
        raw = fr.read()
#   try:
    parser = BITSParser()
    output = parser.parse(raw)
    with open("woo.json", "w") as fw:
        fw.write(json.dumps(output, sort_keys=True, indent=2))
#   except Exception as err:
#       print("Well, shit: %s" % err)

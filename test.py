import json
from adsingestp.parsers.bits import BITSParser
from glob import iglob


infiles = iglob("/Users/mtemple/work/bits_files/**/*Meta")

for f in infiles:
    with open(f, "r") as fd:
        data = fd.read()
    try:
        p = BITSParser()
        parsed = p.parse(data)
    except Exception as err:
        pf = f.split("/")[-1]
        print("File %s failed: %s" % (pf, err))
    else:
        outfile = f + "_6770d1.json"
        with open(outfile, "w") as fw:
            fw.write("%s\n" % json.dumps(parsed, indent=2, sort_keys=True))

import json
from adsingestp.parsers.nj import NewJATSParser
from glob import glob

#infile = 'tests/stubdata/input/jats_a+a_collab_affils.xml'
#infile = 'tests/stubdata/input/jats_edp_jnwpu_40_96.xml'
#infile = 'tests/stubdata/input/jats_springer_ZaMP_s00033-023-02064-z.xml'
#infile = '/Users/mtemple/Projects/Github_repos/JATSFullTextParser/chinese/17_kjkxxb-45-1-215.xml'
#infile = 'tests/stubdata/input/nlm_tf_molph_120_2000057.xml'

files = glob("./tests/stubdata/input/*jats*xml")

for f in files:
    with open(f, 'r') as fd:
        raw = fd.read()


    print(f)
    parser = NewJATSParser()
    outf = "./test_output/"+f.split('/')[-1]+".json"
    try:
        output = parser.parse(raw)
    except Exception as err:
        print("Failed: %s" % err)
    else:
        with open(outf, "w") as fj:
            fj.write("%s\n" % json.dumps(output, indent=2, sort_keys=True))


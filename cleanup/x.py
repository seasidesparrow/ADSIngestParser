import json
from adsingestp.parsers.nj import NewJATSParser

#infile = 'tests/stubdata/input/jats_a+a_collab_affils.xml'
#infile = 'tests/stubdata/input/jats_edp_jnwpu_40_96.xml'
#infile = 'tests/stubdata/input/jats_springer_ZaMP_s00033-023-02064-z.xml'
infile = '/Users/mtemple/Projects/Github_repos/JATSFullTextParser/chinese/17_kjkxxb-45-1-215.xml'
#infile = 'tests/stubdata/input/nlm_tf_molph_120_2000057.xml'

with open(infile, 'r') as fd:
    raw = fd.read()


parser = NewJATSParser()
output = parser.parse(raw)

with open("woot.json", "w") as fj:
    fj.write("%s\n" % json.dumps(output, indent=2, sort_keys=True))

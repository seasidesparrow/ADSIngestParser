from adsingestp.parsers.jats_new import JATSNew

infile = "jats_aps_id_overrun.xml"

with open(infile, "r") as fj:
    raw = fj.read()

p = JATSNew(raw=raw)
p._extract_all_xref()

#with open("buh.tsv", "w") as fb:
#    for k, v in p.xref_dict.items():
#        fb.write("%s\t%s\n" % (k, v))
print(p.xref_dict)
print(p.affil_dict)

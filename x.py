from adsingestp.parsers.jats_new import JATSNew

infile = "jats_aps_id_overrun.xml"

with open(infile, "r") as fj:
    raw = fj.read()

p = JATSNew(raw=raw)
p._extract_all_xref()

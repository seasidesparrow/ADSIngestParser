import json
from adsingestp.parsers.crossref import CrossrefParser


infile = "./lol.xml"
with open(infile, "r") as fx:
    raw = fx.read()

parser = CrossrefParser()
output = parser.parse(raw)
print(json.dumps(output, indent=2, sort_keys=True))

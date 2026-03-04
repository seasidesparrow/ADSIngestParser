import bs4
import json
from glob import glob

target_dir = "./tests/stubdata/input/*jats*xml"

for f in glob(target_dir):
    tagcount = {}
    try:
        with open(f, "r") as fx:
            raw = fx.read()
        soup = bs4.BeautifulSoup(raw, "lxml-xml")
        aff_list = soup.find_all("aff", id=True)
        if aff_list:
            for tag in aff_list:
                key = tag.parent.name
                if tagcount.get(key, None):
                    tagcount[key] += 1
                else:
                    tagcount[key] = 1
                #if key == "article-meta":
                #if key == "contrib":
                #    print("Input file: %s" % f)
                #    print(tag)
            print(f, tagcount)
         
        else:
            pass
            #print("File %s has no aff tags..." % f)
    except Exception as err:
        print("Well, shit.  %s" % err)
    

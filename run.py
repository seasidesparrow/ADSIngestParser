import argparse
import json
import os
import re
from datetime import datetime, timedelta
from glob import iglob

from adsenrich.references import ReferenceWriter
from adsmanparse import classic_serializer, counter, doiharvest, translator, utils
from adsputils import load_config, setup_logging

from adsingestp.parsers.adsfeedback import ADSFeedbackParser
from adsingestp.parsers.bits import BITSParser
from adsingestp.parsers.copernicus import CopernicusParser
from adsingestp.parsers.crossref import CrossrefParser
from adsingestp.parsers.datacite import DataciteParser
from adsingestp.parsers.dubcore import DublinCoreParser
from adsingestp.parsers.elsevier import ElsevierParser
from adsingestp.parsers.jats import JATSParser
from adsingestp.parsers.springer import SpringerParser
from adsingestp.parsers.wiley import WileyParser

PARSER_TYPES = {
    "jats": JATSParser(),
    "dc": DataciteParser(),
    "cr": CrossrefParser(),
    "nlm": JATSParser(),
    "elsevier": ElsevierParser(),
    "feedback": ADSFeedbackParser(),
    "copernicus": CopernicusParser(),
    "wiley": WileyParser(),
    "dubcore": DublinCoreParser(),
    "bits": BITSParser(),
    "springer": SpringerParser(),
}

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "./"))
conf = load_config(proj_home=proj_home)
logger = setup_logging(
    "run.py",
    proj_home=proj_home,
    level=conf.get("LOGGING_LEVEL", "INFO"),
    attach_stdout=conf.get("LOG_STDOUT", False),
)

doi_bibcode_dict = utils.load_doi_bibcode(conf.get("DOI_BIBCODE_MAP", "./all.links"))

counter_datafile = conf.get("COUNTER_DATAFILE", "./counter.json")


def get_args():
    parser = argparse.ArgumentParser("Create an ADS record from a DOI")

    parser.add_argument(
        "-b",
        "--bibstem",
        dest="bibstem",
        action="store",
        default=None,
        help="Bibstem for special handling and/or bibcode(s)",
    )

    parser.add_argument(
        "-v",
        "--volume",
        dest="volume",
        action="store",
        default=None,
        help="Volume for special handling and/or bibcodes(s)",
    )

    parser.add_argument(
        "-d", "--doi", dest="fetch_doi", action="store", default=None, help="DOI to fetch"
    )

    parser.add_argument(
        "-l",
        "--doi-list",
        dest="fetch_doi_list",
        action="store",
        default=None,
        help="Path to a file containing a list of DOIs, one per line",
    )

    parser.add_argument(
        "-f",
        "--outfile",
        dest="output_file",
        action="store",
        default="./doi.tag",
        help="File that tagged format will be written to",
    )

    parser.add_argument(
        "-p",
        "--proc_path",
        dest="proc_path",
        action="store",
        default=None,
        help="Path to files or list of files",
    )

    parser.add_argument(
        "-a",
        "--age",
        dest="proc_since",
        action="store",
        default=None,
        help="Age (in days) of oldest files in --proc_path to process",
    )

    parser.add_argument(
        "-t",
        "--file_type",
        dest="file_type",
        action="store",
        default=None,
        help="Type of input file: jats, dc, cr, nlm, elsevier, feedback, copernicus, wiley",
    )

    parser.add_argument(
        "-w",
        "--write_refs",
        dest="write_refs",
        action="store_true",
        default=False,
        help="Export references from records along with bibdata",
    )

    parser.add_argument(
        "-r",
        "--ref_dir",
        dest="ref_dir",
        action="store",
        default="/proj/ads/references/sources/",
        help="Base path to reference output directory",
    )

    parser.add_argument(
        "-s",
        "--source",
        dest="source",
        action="store",
        default=None,
        help="Origin/publisher of record/reference data",
    )

    parser.add_argument(
        "-z",
        "--parsedfile",
        dest="parsedfile",
        action="store_true",
        default=False,
        help="Output parsed filename in properties tag",
    )

    parser.add_argument(
        "-x",
        "--write_xref",
        dest="write_xref",
        action="store_true",
        default=False,
        help="Write doi-harvested records to xml file",
    )

    parser.add_argument(
        "-Z",
        "--tagged_refs",
        dest="tagged_refs",
        action="store_true",
        default=False,
        help="Output refs in tagged file (%%Z)",
    )

    parser.add_argument(
        "-I",
        "--id_page",
        dest="id_page",
        action="store_true",
        default=False,
        help="Use id in place of page",
    )

    parser.add_argument(
        "-D",
        "--doi_page",
        dest="doi_page",
        action="store_true",
        default=False,
        help="Use DOI in place of page",
    )

    parser.add_argument(
        "-C",
        "--counter_page",
        dest="counter_page",
        action="store_true",
        default=False,
        help="Use a running counter in place of page",
    )

    args = parser.parse_args()
    return args


def use_counter_page(output, bibstem):
    try:
        bibcode = output.get("bibcode", None)
        if bibcode:
            if not bibstem:
                bibstem = bibcode[4:9]
            year = str(bibcode[0:4])
            page = bibcode[14:18]
            if page == "....":
                page = str(counter.Counter().get_page(bibstem, year, counter_datafile))
                page = page.rjust(4, ".")
            bibcode_new = bibcode[0:14] + page + bibcode[18]
            if bibcode_new != bibcode:
                output["bibcode"] = bibcode_new
        return output
    except Exception as err:
        logger.warning("Failed to add counter page to bibcode: %s" % err)


def move_pubid(record):
    try:
        pubids = record.get("publisherIDs", [])
        pid = None
        for p in pubids:
            if p.get("attribute", "") == "publisher-id":
                pid = p.get("Identifier", "")
        if pid:
            pagination = record.get("pagination", {})
            # split on hyphen
            lp = pid.split("-")[-1]
            if lp:
                try:
                    del pagination["firstPage"]
                    del pagination["lastPage"]
                    del pagination["pageRange"]
                except Exception:
                    pass
                pagination["electronicID"] = lp
                record["pagination"] = pagination
        else:
            raise Exception("Didn't find a publisher id for page.id")

    except Exception as err:
        logger.warning("Failed to convert pubid to valid idno: %s" % err)
    return record


def move_doiid(record):
    try:
        persistentids = record.get("persistentIDs", [])
        doi = None
        for p in persistentids:
            if p.get("DOI", None):
                doi = p["DOI"]
                break
        if doi:
            pagination = record.get("pagination", {})
            first = pagination.get("firstPage", "")
            rangefirst = pagination.get("pageRange", "").split("-")[0]
            if first == "1" or rangefirst == "1":
                doiid = doi.split("/")[-1]
                try:
                    del pagination["firstPage"]
                    del pagination["lastPage"]
                    del pagination["pageRange"]
                except Exception:
                    pass
                pagination["electronicID"] = doiid
                record["pagination"] = pagination
        else:
            raise Exception("Didn't find a DOI for page.id")

    except Exception as err:
        logger.warning("Failed to convert doi to valid idno: %s" % err)
    return record


def create_tagged(rec=None, args=None):
    try:
        xlator = translator.Translator(
            doibib=doi_bibcode_dict, idpage=args.id_page, doipage=args.doi_page
        )
    except Exception as err:
        raise Exception("translator instantiation failed: %s" % err)
    try:
        seri = classic_serializer.ClassicSerializer(tag_refs=args.tagged_refs)
    except Exception as err:
        raise Exception("serializer instantiation failed: %s" % err)
    try:
        xlator.translate(
            data=rec, bibstem=args.bibstem, volume=args.volume, parsedfile=args.parsedfile
        )
        if args.counter_page and xlator.output.get("bibcode", None):
            use_counter_page(xlator.output, args.bibstem)
        output = seri.output(xlator.output)
        return output
    except Exception as err:
        raise Exception("TRANSLATE failed: %s" % err)


def write_xml(inputRecord):
    try:
        if inputRecord.get("type", None) == "cr":
            doi = inputRecord.get("name", None)
            raw_data = inputRecord.get("data", "")
            output_dir = conf.get("XML_OUTPUT_BASEDIR", "./doi/")
            doi_to_path = doi.split("/")
            path = []
            if "http" in doi_to_path[0].lower():
                doi_to_path = doi_to_path[3:]
            for d in doi_to_path:
                path.append(re.sub(r"[^\w_. -]+", "_", d))
            filepath = "/".join(path) + ".xml"
            output_path = output_dir + filepath
            dirname = os.path.dirname(output_path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(output_path, "w") as fx:
                fx.write("%s\n" % raw_data)
        else:
            raise Exception("This is not a crossref file.")
    except Exception as err:
        logger.warning("Export of doi (%s) to xml failed: %s" % (doi, err))


def create_refs(rec=None, args=None, bibcode=None):
    try:
        rw = ReferenceWriter(
            reference_directory=args.ref_dir,
            reference_source=args.source,
            bibcode=bibcode,
            data=rec,
        )
        rw.write_references_to_file()
    except Exception as err:
        logger.warning("Unable to write references: %s" % err)


def write_record(record, args):
    if args.output_file:
        tagged = None
        try:
            tagged = create_tagged(rec=record, args=args)
        except Exception as err:
            logger.warning("Failed to create_tagged record: %s" % err)
        if tagged:
            with open(args.output_file, "a") as fout:
                fout.write("%s\n" % tagged)
            tagged_list = tagged.split("\n")
            bibcode = None
            for l in tagged_list:
                try:
                    (tag, value) = l.strip().split()
                    if tag == "%R":
                        bibcode = value
                        break
                except Exception as noop:
                    pass
            if args.write_refs:
                create_refs(rec=record, bibcode=bibcode, args=args)
        else:
            raise Exception("Tagged record not generated.")
    else:
        raise Exception("Output_file not defined, no place to write records to!")


def parse_record(rec):
    pdata = rec.get("data", None)
    ptype = rec.get("type", None)
    filename = rec.get("name", "")
    parser = PARSER_TYPES.get(ptype, None)
    write_file = utils.has_body(pdata)
    parsedrecord = None
    if not parser:
        logger.error("No parser available for file_type '%s'." % ptype)
    else:
        try:
            parser.__init__()
            if ptype == "nlm":
                parsedrecord = parser.parse(pdata, bsparser="lxml-xml")
            else:
                parsedrecord = parser.parse(pdata)
            if parsedrecord:
                if utils.suppress_title(parsedrecord, conf.get("DEPRECATED_TITLES", [])):
                    parsedrecord = None
                    raise Exception("Warning: article matches a suppressed title.")
                if filename:
                    if not parsedrecord.get("recordData", {}).get("loadLocation", None):
                        parsedrecord["recordData"]["loadLocation"] = filename
                    if not write_file:
                        parsedrecord["recordData"]["loadLocation"] = None
            else:
                raise Exception("Null body returned by parser!")
        except Exception as err:
            logger.warning("Error parsing record (%s): %s" % (filename, err))
    return parsedrecord


def process_record(rec, args):
    try:
        parsedRecord = parse_record(rec)
        if not parsedRecord:
            logger.error("Parsing yielded no data for %s" % rec.get("name", None))
        else:
            if args.id_page:
                parsedRecord = move_pubid(parsedRecord)
            elif args.doi_page:
                parsedRecord = move_doiid(parsedRecord)
            try:
                write_record(parsedRecord, args)
            except Exception as err:
                logger.error("Classic tagger did not generate a tagged record for %s" % err)
            else:
                # logger.debug("Successfully processed %s with %s" % (rec.get("name", None), str(args)))
                pass
    except Exception as err:
        logger.error("Error parsing and processing record %s: %s" % (rec.get("name", ""), err))


def process_filepath(args):
    if args.proc_path:
        logger.info("Finding files in path %s ..." % args.proc_path)
        infiles = [x for x in iglob(args.proc_path, recursive=True)]
        if not infiles:
            logger.warning("No files found in path %s." % args.proc_path)
        else:
            logger.info("Found %s files." % len(infiles))
            if args.proc_since:
                logger.info("Checking file ages...")
                dtime = timedelta(days=int(args.proc_since))
                today = datetime.today()
                infiles_since = [
                    x
                    for x in infiles
                    if ((today - datetime.fromtimestamp(os.path.getmtime(x))) <= dtime)
                ]
                infiles = infiles_since
            if not infiles:
                logger.error("No files more recent than %s days old!" % str(args.proc_since))
            else:
                nfiles = len(infiles)
                logger.info("There were %s files found to process" % str(nfiles))
                for f in infiles:
                    inputRecord = {}
                    try:
                        with open(f, "r") as fin:
                            inputRecord = {"data": fin.read(), "name": f, "type": args.file_type}
                    except Exception as err:
                        logger.warning("Failed to read input file %s: %s" % (f, err))
                    else:
                        try:
                            process_record(inputRecord, args)
                        except Exception as err:
                            logger.warning("Process record failed: %s" % err)
    else:
        logger.warning("Null processing path given, nothing processed.")


def process_doilist(doilist, args):
    if doilist:
        ptype = args.file_type
        if not ptype:
            ptype = "cr"
        for d in doilist:
            try:
                getdoi = doiharvest.DoiHarvester(doi=d)
                doi_record = getdoi.get_record()
                inputRecord = {"data": doi_record, "name": d, "type": ptype}
                if args.write_xref:
                    write_xml(inputRecord)
            except Exception as err:
                logger.warning("Failed to fetch doi %s: %s" % (d, err))
            else:
                process_record(inputRecord, args)
    else:
        logger.warning("No DOIs provided, nothing processed.")


def main():
    args = get_args()
    rawDataList = []
    ingestDocList = []

    logger.debug("Initiating parsing with the following arguments: %s" % str(args))

    if args.proc_path and not args.file_type:
        fileTypeList = PARSER_TYPES.keys()
        logger.error("You need to provide a filetype from this list: %s" % str(fileTypeList))
    else:
        # This route processes data from user-input files
        if args.proc_path:
            process_filepath(args)

        # This route fetches data from Crossref via the Habanero module
        elif args.fetch_doi or args.fetch_doi_list:
            doiList = None
            if args.fetch_doi:
                doiList = [args.fetch_doi]
            elif args.fetch_doi_list:
                doiList = []
                with open(args.fetch_doi_list, "r") as fin:
                    for l in fin.readlines():
                        doiList.append(l.strip())
            process_doilist(doiList, args)


if __name__ == "__main__":
    main()

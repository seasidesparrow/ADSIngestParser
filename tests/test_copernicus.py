"""
Created on Wed Aug 30 17:03:57 2023

@author: mugdhapolimera

Unittest for Copernicus parser
"""

import datetime
import json
import os
import unittest

from adsingestschema import ads_schema_validator

from adsingestp.parsers import copernicus

TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


class TestCopernicus(unittest.TestCase):
    def setUp(self):
        stubdata_dir = os.path.join(os.path.dirname(__file__), "stubdata/")
        self.inputdir = os.path.join(stubdata_dir, "input")
        self.outputdir = os.path.join(stubdata_dir, "output")

    def test_copernicus(self):
        filenames = [
            "copernicus_ESSD_essd-15-3075-2023",
            "copernicus_ISPAn_isprs-annals-X-M-1-2023-237-2023",
            "copernicus_GeChr_gchron-5-323-2023",
            "copernicus_ISPAr_isprs-archives-XLVIII-M-2-2023-721-2023",
        ]
        for f in filenames:
            test_infile = os.path.join(self.inputdir, f + ".xml")
            test_outfile = os.path.join(self.outputdir, f + ".json")
            parser = copernicus.CopernicusParser()

            with open(test_infile, "rb") as fp:
                input_data = fp.read()

            parsed = parser.parse(input_data)

            with open(test_outfile, "rb") as fp:
                output_text = fp.read()
                output_data = json.loads(output_text)

            # make sure this is valid schema
            try:
                ads_schema_validator().validate(parsed)
            except Exception:
                self.fail("Schema validation failed")
                pass

            # this field won't match the test data, so check and then discard
            time_difference = (
                datetime.datetime.strptime(parsed["recordData"]["parsedTime"], TIMESTAMP_FMT)
                - datetime.datetime.utcnow()
            )
            self.assertTrue(abs(time_difference) < datetime.timedelta(seconds=10))
            parsed["recordData"]["parsedTime"] = ""

            self.assertEqual(parsed, output_data)

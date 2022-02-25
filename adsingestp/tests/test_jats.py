
import unittest
import os
import json
import datetime

from adsingestp.parsers import jats

TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

class TestIOP(unittest.TestCase):

    def setUp(self):
        stubdata_dir = os.path.join(os.path.dirname(__file__), 'stubdata/')
        self.inputdir = os.path.join(stubdata_dir, 'input')
        self.outputdir = os.path.join(stubdata_dir, 'output')
        self.maxDiff = None

    def test_jats(self):
        filenames = ['jats_apj_859_2_101']
        for f in filenames:
            test_infile = os.path.join(self.inputdir, f + '.xml')
            test_outfile = os.path.join(self.outputdir, f + '.json')
            parser = jats.JATSParser()

            with open(test_infile, 'rb') as fp:
                input_data = fp.read()

            with open(test_outfile, 'rb') as fp:
                output_text = fp.read()
                output_data = json.loads(output_text)

            parsed = parser.parse(input_data)

            # this field won't match the test data, so check and then discard
            time_difference = datetime.datetime.strptime(parsed['recordData']['parsedTime'], TIMESTAMP_FMT) - datetime.datetime.utcnow()
            self.assertTrue(abs(time_difference) < datetime.timedelta(seconds=10))
            parsed['recordData']['parsedTime'] = None

            self.assertEqual(parsed, output_data)

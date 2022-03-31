import logging
import os
import unittest

from adsingestp import utils

proj_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "adsingestp"))
logging.basicConfig(
    format="%(levelname)s %(asctime)s %(message)s",
    filename=os.path.join(proj_dir, "logs", "parser.log"),
    level=logging.INFO,
    force=True,
)


class TestAuthorNames(unittest.TestCase):
    def setUp(self):
        self.name_parser = utils.AuthorNames()

    def test_parse(self):
        input_authors = [
            "Miller, Elizabeth",
            "Elizabeth Miller",
            "Robert White Smith",
            "M. Power",
            "maria antonia de la paz",
            "bla. bli.",
            "John",
        ]

        expected_authors = [
            {
                "given": "Elizabeth",
                "middle": "",
                "surname": "Miller",
                "suffix": "",
                "prefix": "",
                "nameraw": "Miller, Elizabeth",
            },
            {
                "given": "Elizabeth",
                "middle": "",
                "surname": "Miller",
                "suffix": "",
                "prefix": "",
                "nameraw": "Elizabeth Miller",
            },
            {
                "given": "Robert",
                "middle": "",
                "surname": "White Smith",
                "suffix": "",
                "prefix": "",
                "nameraw": "Robert White Smith",
            },
            {
                "given": "M.",
                "middle": "",
                "surname": "Power",
                "suffix": "",
                "prefix": "",
                "nameraw": "M. Power",
            },
            {
                "given": "maria",
                "middle": "antonia",
                "surname": "de la paz",
                "suffix": "",
                "prefix": "",
                "nameraw": "maria antonia de la paz",
            },
            {
                "given": "bla.",
                "middle": "",
                "surname": "bli.",
                "suffix": "",
                "prefix": "",
                "nameraw": "bla. bli.",
            },
            {
                "given": "John",
                "middle": "",
                "surname": "",
                "suffix": "",
                "prefix": "",
                "nameraw": "John",
            },
        ]

        for idx, i in enumerate(input_authors):
            parsed = self.name_parser.parse(i)

            self.assertEqual(parsed[0], expected_authors[idx])

    def test_parse_collaboration(self):
        input_authors = [
            "The Collaboration: John Stuart",
            "Gaia Collaboration",
        ]

        # first round, test default collaboration params
        expected_authors = [
            [
                {"nameraw": "The Collaboration", "collab": "Collaboration"},
                {
                    "given": "John",
                    "middle": "",
                    "surname": "Stuart",
                    "suffix": "",
                    "prefix": "",
                    "nameraw": "John Stuart",
                },
            ],
            [{"nameraw": "Gaia Collaboration", "collab": "Gaia Collaboration"}],
        ]

        for idx, i in enumerate(input_authors):
            parsed = self.name_parser.parse(i)

            self.assertEqual(parsed, expected_authors[idx])

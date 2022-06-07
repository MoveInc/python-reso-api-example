import os
import unittest

from get_listings_data import _ensure_arg


class TestArgs(unittest.TestCase):
    def test_parse_arg(self):
        args = {"one": "1", "two": "2"}
        parse1 = _ensure_arg(args, "one")
        parse2 = _ensure_arg(args, "two")
        os.environ["AOIJOWEIUOIOIJ"] = "3"
        parse3 = _ensure_arg(args, "AOIJOWEIUOIOIJ")
        del os.environ["AOIJOWEIUOIOIJ"]
        self.assertTrue(parse1 == "1")
        self.assertTrue(parse2 == "2")
        self.assertTrue(parse3 == "3")
        with self.assertRaises(EnvironmentError):
            _ensure_arg(args, "four")


if __name__ == "__main__":
    unittest.main()

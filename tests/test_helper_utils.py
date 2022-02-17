import os
import unittest
import src.helper_utils as helper_utils


class TestHelperUtils(unittest.TestCase):
    def test_authenticate(self):
        auth_key = helper_utils.authenticate("public_sandbox", "public_sandbox")
        self.assertTrue(auth_key != "")

    def test_get_properties(self):
        auth_key = helper_utils.authenticate("public_sandbox", "public_sandbox")
        props = helper_utils._get_data("https://api.listhub.com/odata/Property?$top=1&$select=ListingKey", {}, auth_key)
        self.assertTrue(props != tuple())
        prop: dict = list(props[1].values())[0]
        self.assertTrue(prop.get("ListingKey") is not None)

    def test_parse_arg(self):
        args = {"one": "1", "two": "2"}
        parse1 = helper_utils.parse_arg(args, "one")
        parse2 = helper_utils.parse_arg(args, "two")
        os.environ["AOIJOWEIUOIOIJ"] = "3"
        parse3 = helper_utils.parse_arg(args, "AOIJOWEIUOIOIJ")
        del os.environ["AOIJOWEIUOIOIJ"]
        self.assertTrue(parse1 == "1")
        self.assertTrue(parse2 == "2")
        self.assertTrue(parse3 == "3")
        with self.assertRaises(EnvironmentError):
            helper_utils.parse_arg(args, "four")


if __name__ == '__main__':
    unittest.main()

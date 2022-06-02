import os
import responses
import unittest

import helper_utils


class TestHelperUtils(unittest.TestCase):
    @responses.activate
    def test_authenticate(self):
        responses.add(responses.POST, 'https://api.listhub.com/oauth2/token', json={'access_token': 'ImAKey'},
                      status=200)
        auth_key = helper_utils.authenticate("public_sandbox", "public_sandbox")
        self.assertTrue(auth_key != "")

    @responses.activate
    def test_get_properties(self):
        responses.add(responses.POST, 'https://api.listhub.com/oauth2/token',
                      json={'access_token': 'ImAKey'}, status=200)
        responses.add(responses.GET, 'https://api.listhub.com/odata/Property?$top=1&$select=ListingKey',
                      json={"value": [{"ListingKey": "FAKE"}]}, status=200)

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

import responses
import unittest

import helper_utils


class TestHelperUtils(unittest.TestCase):
    @responses.activate
    def test_authenticate(self):
        responses.add(
            responses.POST, "https://api.listhub.com/oauth2/token", json={"access_token": "ImAKey"}, status=200
        )

        access_token = helper_utils.authenticate("public_sandbox", "public_sandbox")
        self.assertTrue(access_token != "")

    @responses.activate
    def test_get_properties(self):
        responses.add(
            responses.POST, "https://api.listhub.com/oauth2/token", json={"access_token": "ImAKey"}, status=200
        )
        responses.add(
            responses.GET,
            "https://api.listhub.com/odata/Property?$top=1&$select=ListingKey",
            json={"value": [{"ListingKey": "FAKE"}]},
            status=200,
        )

        access_token = helper_utils.authenticate("public_sandbox", "public_sandbox")
        props = helper_utils._get_properties_data(
            "https://api.listhub.com/odata/Property?$top=1&$select=ListingKey", access_token, {}
        )

        self.assertTrue(props != tuple())

        prop: dict = list(props[1].values())[0]
        self.assertTrue(prop.get("ListingKey") is not None)


if __name__ == "__main__":
    unittest.main()

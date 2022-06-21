import responses
import unittest

import media_utils
from data_utils import ListingState


class TestMediaUtils(unittest.TestCase):
    @responses.activate
    def test_parse_media(self):
        responses.add(responses.GET, "https://google.com/test.jpg", body=b"{'photo': 'ImAPic'}", status=200)

        db_medias = {
            "fake1": [["key1", "", "", "2021-11-06T04:03:44.000Z", int(ListingState.NO_CHANGE)]],
            "fake2": [["key1", "", "", "2021-11-06T04:03:44.000Z", int(ListingState.NO_CHANGE)]],
        }
        api_properties = {
            "fake1": {
                "Media": [
                    {
                        "MediaKey": "key2",
                        "MediaURL": "https://google.com/test.jpg",
                        "MediaModificationTimestamp": "2021-10-06T04:03:44.000Z",
                    }
                ]
            }
        }
        parsed_db_medias = media_utils.parse_media(api_properties, db_medias, True)

        should_be = {
            "fake1": [
                # Should be "no change"
                ["key1", "", "", "2021-11-06T04:03:44.000Z", 4],
                # Should be "new"
                ["key2", "fake1", "https://google.com/test.jpg", "2021-10-06T04:03:44.000Z", 1],
            ],
            "fake2": [
                # Should be "no change"
                ["key1", "", "", "2021-11-06T04:03:44.000Z", 4]
            ],
        }

        self.assertTrue(parsed_db_medias == should_be)


if __name__ == "__main__":
    unittest.main()

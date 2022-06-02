import os
import responses
import unittest

import media_utils
import db_utils


class TestHelperUtils(unittest.TestCase):
    @responses.activate
    def test_download_and_delete_photo(self):
        responses.add(responses.GET, 'https://google.com/test', body=b"{'photo': 'ImAPic'}", status=200)
        media = ["test", "test", "https://google.com/test"]
        media_utils._download_photo(media)
        self.assertTrue(os.path.exists("./media/test/test.jpeg"))
        media_utils._delete_photo(media)
        self.assertFalse(os.path.exists("./media/test/"))

    @responses.activate
    def test_parse_media(self):
        responses.add(responses.GET, 'https://google.com/test.jpg', body=b"{'photo': 'ImAPic'}", status=200)
        media_db = {
            "fake1": [["key1", "", "", "2021-11-06T04:03:44.000Z", int(db_utils.ListingState.NO_CHANGE)]],
            "fake2": [["key1", "", "", "2021-11-06T04:03:44.000Z", int(db_utils.ListingState.NO_CHANGE)]]
        }
        properties = {"fake1": {"Media": [{
            "MediaKey": "key2",
            "MediaURL": "https://google.com/test.jpg",
            "MediaModificationTimestamp": "2021-10-06T04:03:44.000Z"
        }]}}
        new_media_db = media_utils.parse_media(properties, media_db, True)
        should_be = {
            'fake1': [
                # Should be "no change"
                ['key1', '', '', '2021-11-06T04:03:44.000Z', 4],
                # Should be "new"
                ['key2', 'fake1', 'https://google.com/test.jpg', '2021-10-06T04:03:44.000Z', 1]
            ],
            'fake2': [
                # Should be "delete"
                ['key1', '', '', '2021-11-06T04:03:44.000Z', 3]
            ]
        }
        self.assertTrue(new_media_db == should_be)


if __name__ == '__main__':
    unittest.main()

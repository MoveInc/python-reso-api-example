import os

import requests
import media_utils as media_utils
import db_utils as db_utils
from datetime import datetime, timezone


# Get current UTC time and convert to the ISO format.
def get_current_time_iso() -> str:
    time = datetime.now(tz=timezone.utc)
    iso_time = time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return iso_time


# Authenticate to the api with the client credentials.
def authenticate(client_id: str, client_secret: str) -> str:
    url: str = "https://api.listhub.com/oauth2/token"
    data: dict = {"grant_type": "client_credentials", "client_id": client_id,
                  "client_secret": client_secret}
    request: requests.Response = requests.post(url, data=data)
    response: dict = request.json()
    access_token = response["access_token"]
    return access_token


# For all the listings, check the listing status and update the database
def _update_db(properties, listings_db, media_db, db_conn, db_cursor) -> None:
    for key in listings_db:
        if listings_db[key] == int(db_utils.ListingState.NEW):
            db_utils.insert_table_data(key, properties, db_conn, db_cursor)
            _update_media(key, media_db, db_conn, db_cursor)
        elif listings_db[key] == int(db_utils.ListingState.UPDATE):
            db_utils.update_table_data(key, properties, db_conn, db_cursor)
            _update_media(key, media_db, db_conn, db_cursor)
        elif listings_db[key] == int(db_utils.ListingState.DELETE):
            _update_media(key, media_db, db_conn, db_cursor)
            db_utils.delete_table_data(key, db_conn, db_cursor)
        elif listings_db[key] == int(db_utils.ListingState.DEFAULT):
            print("DEFAULT Case not handled")


# For all the media to corresponding listingKey, update the database.
def _update_media(listingkey: str, media_db: dict, db_conn, db_cursor) -> None:
    if listingkey in media_db:
        for media in media_db[listingkey]:
            if media[4] == int(db_utils.ListingState.NEW):
                db_utils.insert_media(media, db_conn, db_cursor)
            elif media[4] == int(db_utils.ListingState.UPDATE):
                db_utils.update_media(media, db_conn, db_cursor)
            elif media[4] == int(db_utils.ListingState.DELETE):
                db_utils.delete_media(media, db_conn, db_cursor)
            elif media[4] == int(db_utils.ListingState.DEFAULT):
                db_utils.delete_media(media, db_conn, db_cursor)


# Get properties in a results page and return the next link and properties.
def _get_data(url: str, properties: dict, access_token: str) -> tuple:
    headers = {"Authorization": "Bearer " + access_token}
    request = requests.get(url, headers=headers)
    # If request fails for some reason try a couple more times before giving up.
    if request is None:
        tries = 1
        while request is None and tries < 5:
            print("Tries: " + str(tries))
            request = requests.get(url, headers)
            tries += 1
    if request is not None:
        response = request.json()
    else:
        raise ConnectionError("Could not get data from API")

    for reso_property in response["value"]:
        properties.update({reso_property["ListingKey"]: reso_property})
    try:
        next_link = response["@odata.nextLink"]
    except KeyError:
        next_link = ""
    return next_link, properties


# Iterate through the next links till the last one.
def get_properties(url: str, access_token: str, listingkey_db, opts: dict, db_conn, db_cursor, full: bool) -> None:
    count = 0
    properties = {}
    while True:
        print("Get Listings: " + url)
        url, properties = _get_data(url, properties, access_token)
        count += len(properties)
        parse_listings(properties, listingkey_db, full, db_conn, db_cursor, opts)
        print("Total properties processed: " + str(count))
        properties = {}
        if url == "":
            break


# Iterate through the properties and listings from the database. Compare
# them and create, append or update the listings_db with the new or updated
# properties. Create media and call handle photos to update photos.
def parse_listings(properties: dict, listings_db, is_full_pull: bool, db_conn, db_cursor, opts: dict) -> None:
    media_db = db_utils.get_media_db(db_cursor)
    for listingKey in listings_db:
        if listingKey in properties:
            listings_db[listingKey] = int(db_utils.ListingState.UPDATE)
        else:
            if is_full_pull is True:
                listings_db[listingKey] = int(db_utils.ListingState.DELETE)
            else:
                listings_db[listingKey] = int(db_utils.ListingState.NO_CHANGE)
    for key in properties:
        if listings_db.get(key) is None:
            listings_db[key] = int(db_utils.ListingState.NEW)
    media_db = media_utils.parse_media(properties, media_db, is_full_pull)
    print("Updating Database")
    _update_db(properties, listings_db, media_db, db_conn, db_cursor)

    if opts.get("download_photos"):
        media_utils.handle_photos(media_db)


# Parses a single argument from the args; if it cannot be found
# it will try to find it in the environment variables else raise error.
def parse_arg(args: dict, arg: str) -> str:
    value: str = args.get(arg)
    if value is None:
        value = os.environ.get(arg)

    if value is None:
        raise EnvironmentError(f"{arg} is not present in options or the environment.")

    return value

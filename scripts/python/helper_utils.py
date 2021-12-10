import requests
import media_utils
import db_utils
from datetime import datetime, timezone


# Get current UTC time and convert to the ISO format.
def get_current_time_iso():
    time = datetime.now(tz=timezone.utc)
    iso_time = time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return iso_time


# Authenticate to the api with the client credentials.
def authenticate(client_id, client_secret):
    url = "https://api.listhub.com/oauth2/token"
    data = {"grant_type": "client_credentials", "client_id": client_id,
            "client_secret": client_secret}
    request = requests.post(url, data=data)
    response = request.json()
    access_token = response["access_token"]
    return access_token


# For all the listings, check the listing status and update the database
def _update_db(properties, listings_db, media_db, db_conn, db_cursor):
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
def _update_media(listingKey, media_db, db_conn, db_cursor):
    if listingKey in media_db:
        for media in media_db[listingKey]:
            if media[4] == int(db_utils.ListingState.NEW):
                db_utils.insert_media(media, db_conn, db_cursor)
            elif media[4] == int(db_utils.ListingState.UPDATE):
                db_utils.update_media(media, db_conn, db_cursor)
            elif media[4] == int(db_utils.ListingState.DELETE):
                db_utils.delete_media(media, db_conn, db_cursor)
            elif media[4] == int(db_utils.ListingState.DEFAULT):
                db_utils.delete_media(media, db_conn, db_cursor)


# Get properties in a results page and return the next link and properties.
def _get_data(url, properties, access_token):
    headers = {"Authorization": "Bearer " + access_token}
    request = requests.get(url, headers=headers)
    response = request.json()
    for property in response["value"]:
        properties.update({property["ListingKey"]: property})
    try:
        next_link = response["@odata.nextLink"]
    except KeyError:
        next_link = ""
    return next_link, properties


# Iterate through the next links till the last one.
def get_properties(url, access_token):
    properties = {}
    while True:
        print("Get Listings: " + url)
        url, properties = _get_data(url, properties, access_token)
        if url == "":
            break
    return properties


# Iterate through the properties and listings from the database. Compare
# them and create, append or update the listings_db with the new or updated
# properties. Create media and call handle photos to update photos.
def parse_listings(properties, listings_db, is_full_pull, last_access_time,
                   db_conn, db_cursor):
    media_db = {}
    media_db = db_utils.get_media_db(db_conn, db_cursor)
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
    # Uncomment the below line to download the photos.
    # media_utils.handle_photos(media_db)

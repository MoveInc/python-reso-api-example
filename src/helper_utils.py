from datetime import datetime, timezone
from time import sleep
from typing import Any, Optional
from retry import retry

import requests
import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor

import media_utils
import data_utils
from data_utils import ListingState


def get_current_time_iso() -> str:
    """Get current UTC time and convert to the ISO format."""
    time = datetime.now(tz=timezone.utc)
    return time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def authenticate(client_id: str, client_secret: str) -> str:
    """Authenticate to the api with the client credentials."""
    url: str = "https://api.listhub.com/oauth2/token"
    data: dict[str, str] = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    response: requests.Response = requests.post(url, data=data)
    data = response.json()

    return data["access_token"]


def _update_db(
    api_properties: dict[str, dict[str, Any]],
    listing_states: dict[str, int],
    is_full_pull: bool,
    cursor: Cursor
) -> None:
    """For all the listings, check the listing status and update the database."""
    print("Updating database")

    db_medias: dict[str, list[Any]] = data_utils.get_medias(cursor)
    db_medias = media_utils.parse_media(api_properties, db_medias, is_full_pull)

    for listing_key in api_properties:
        if listing_states[listing_key] == int(ListingState.NEW):
            data_utils.insert_property(listing_key, api_properties, cursor)
            _update_media(listing_key, db_medias, cursor)
        elif listing_states[listing_key] == int(ListingState.UPDATE):
            data_utils.update_property(listing_key, api_properties, cursor)
            _update_media(listing_key, db_medias, cursor)


def _update_media(listing_key: str, medias: dict[str, list[Any]], cursor: Cursor) -> None:
    """For all the media with matching listing key, update the database."""
    if listing_key not in medias:
        return

    for media in medias[listing_key]:
        if media[4] == int(ListingState.NEW):
            data_utils.insert_media(media, cursor)
        elif media[4] == int(ListingState.UPDATE):
            data_utils.update_media(media, cursor)


@retry(ConnectionError, tries=5, delay=2)
def _attempt_request(url: str, headers: dict) -> Optional[requests.Response]:
    """Attempts request with retries on exception."""
    try:
        response = requests.get(url, headers=headers)
        if response:
            return response
    except ConnectionError as err:
        print(f"Connection error: {err}")
        raise err

    return None


def _get_properties_data(url: str, access_token: str, properties: dict) -> tuple[str, dict]:
    """Get properties in a results page and return the next link and properties."""
    print(f"Getting property listings: {url}")

    headers = {"Authorization": "Bearer " + access_token}
    response = _attempt_request(url, headers)
    if not response:
        raise ConnectionError("Could not get data from API")

    data = response.json()
    for reso_property in data["value"]:
        properties.update({reso_property["ListingKey"]: reso_property})

    next_link = data.get("@odata.nextLink") or ""

    return next_link, properties


def _process_listings(
    api_properties: dict[str, dict[str, Any]],
    listing_states: dict[str, int],
    is_full_pull: bool,
    cursor: Cursor,
    options: dict[str, str]
) -> None:
    """
    Iterate through the API properties to update listing states then update the database.
    """
    for listing_key in listing_states:
        if listing_key in api_properties:
            listing_states[listing_key] = int(ListingState.UPDATE)

    for listing_key in api_properties:
        if not listing_states.get(listing_key):
            listing_states[listing_key] = int(ListingState.NEW)

    _update_db(api_properties, listing_states, is_full_pull, cursor)


def get_properties(
    url: str,
    access_token: str,
    connection: Connection,
    options: dict[str, str],
    is_full_pull: Optional[bool] = False
) -> bool:
    """Get the properties from the API."""
    success: bool = False
    count: int = 0
    api_properties: dict[str, dict[str, Any]] = {}

    with connection.cursor() as cursor:
        listing_states: dict[str, int] = data_utils.get_listing_states(cursor)

        try:
            while True:
                success = False
                url, api_properties = _get_properties_data(url, access_token, api_properties)

                _process_listings(api_properties, listing_states, is_full_pull, cursor, options)
                connection.commit()
                success = True

                count += len(api_properties)
                print("Total properties processed: " + str(count))

                api_properties = {}
                if not success or url == "":
                    break

            if success and is_full_pull:
                delete_listing_keys =(key for key in listing_states.keys() if listing_states[key] == int(ListingState.DEFAULT))
                for listing_key in delete_listing_keys:
                    data_utils.delete_media(listing_key, cursor)
                    data_utils.delete_property(listing_key, cursor)

                connection.commit()
        except (pymysql.IntegrityError, pymysql.DataError) as err:
            print("DataError or IntegrityError")
            print(err)
            connection.rollback()
        except pymysql.ProgrammingError as err:
            print("Programming Error")
            print(err)
            connection.rollback()
        except pymysql.Error as err:
            print(err)
            connection.rollback()

    return success

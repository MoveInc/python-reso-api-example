from collections import defaultdict
from datetime import datetime
from typing import Any

import dateutil.parser as dateutil_parser

from data_utils import ListingState

PATH_TO_DOWNLOAD = "./media/"


def _iso_to_time(time_str: str) -> datetime:
    """Convert iso format to datetime."""
    timestamp: str = time_str[:-1]
    return dateutil_parser.parse(timestamp)


def _update_if_new(api_media: dict[str, Any], db_media: list[Any]) -> None:
    """Update existing media from database if API media is newer."""
    new_media_modified_time = _iso_to_time(api_media["MediaModificationTimestamp"])
    db_media_modified_time = _iso_to_time(db_media[3])
    if new_media_modified_time > db_media_modified_time:
        db_media[2] = api_media["MediaURL"]
        db_media[3] = api_media["MediaModificationTimestamp"]
        db_media[4] = int(ListingState.UPDATE)
    else:
        db_media[4] = int(ListingState.NO_CHANGE)


def _create_media(media: dict[str, list[Any]], listing_key: str) -> list[Any]:
    return [
        media["MediaKey"],
        listing_key,
        media["MediaURL"],
        media["MediaModificationTimestamp"],
        int(ListingState.NEW)
    ]


def _merge_media(db_medias: dict[str, list[Any]], new_media: dict[str, Any]) -> None:
    """Copy the new media in the existing medias."""
    for listing_key in new_media:
        for media in new_media[listing_key]:
            db_medias[listing_key].append(media)


def parse_media(
    api_properties: dict[str, dict[str, Any]],
    db_medias: dict[str, list[Any]],
    is_full_pull: bool
) -> dict[str, list[Any]]:
    """
    Parse the API properties and update the medias and status.
    """
    new_media: dict[str, list[Any]] = defaultdict(list)

    # Check existing media from the database
    for listing_key in db_medias:
        medias_length: int = len(db_medias[listing_key])

        if listing_key not in api_properties:
            continue

        for api_media in api_properties[listing_key]["Media"]:
            media_exists = False

            for i in range(medias_length):
                # If media key from property matches the database entry
                if api_media["MediaKey"] == db_medias[listing_key][i][0]:
                    media_exists = True
                    _update_if_new(api_media, db_medias[listing_key][i])

                if (i == medias_length - 1) and not media_exists:
                    new_media[listing_key].append(_create_media(api_media, listing_key))

    _merge_media(db_medias, new_media)

    # Check for new media from the API
    for listing_key in api_properties:
        if db_medias.get(listing_key):
            continue

        media_values: list[dict[str, Any]] = api_properties[listing_key]["Media"]
        if not media_values:
            continue

        db_medias[listing_key] = []
        for api_media in media_values:
            db_medias[listing_key].append(_create_media(api_media, listing_key))

    return db_medias

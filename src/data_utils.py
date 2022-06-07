from collections import defaultdict
from enum import IntEnum
from typing import Any

from pymysql.cursors import Cursor

PROPERTY_COLUMNS = (
    "ListingKey",
    "ListingId",
    "ModificationTimestamp",
    "PropertyType",
    "PropertySubType",
    "UnparsedAddress",
    "PostalCity",
    "StateOrProvince",
    "Country",
    "ListPrice",
    "PostalCode",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "StandardStatus",
    "PhotosCount",
    "Cooling",
    "Heating",
    "Latitude",
    "Longitude",
    "LeadRoutingEmail",
    "FireplaceYN",
    "WaterfrontYN"
)

MEDIA_COLUMNS = (
    "MediaKey", "MediaURL", "MediaModificationTimestamp"
)


class ListingState(IntEnum):
    """Enum class for listing states."""
    DEFAULT = 0
    NEW = 1
    UPDATE = 2
    DELETE = 3
    NO_CHANGE = 4


def _create_data(property: dict[str, Any]) -> list[Any]:
    """Create a list of data."""
    data: list[Any] = []

    for column in PROPERTY_COLUMNS:
        if column not in property:
            data.append(None)
        elif type(property[column]) is list:
            data.append(",".join(str(item) for item in property[column]))
        else:
            data.append(property[column])

    return data


def insert_property(listing_key: str, properties: dict[str, dict[str, Any]], cursor: Cursor) -> None:
    """Insert property values into the property table."""
    sql: str = """
        INSERT INTO property (
            ListingKey,
            ListingId,
            ModificationTimestamp,
            PropertyType,
            PropertySubType,
            UnparsedAddress,
            PostalCity,
            StateOrProvince,
            Country,
            ListPrice,
            PostalCode,
            BedroomsTotal,
            BathroomsTotalInteger,
            StandardStatus,
            PhotosCount,
            Cooling,
            Heating,
            Latitude,
            Longitude,
            LeadRoutingEmail,
            FireplaceYN,
            WaterfrontYN
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    data: list[Any] = _create_data(properties[listing_key])

    cursor.execute(sql, data)


def update_property(listing_key: str, properties: dict[str, dict[str, Any]], cursor: Cursor) -> None:
    """Update property table."""
    sql: str = """
        UPDATE property
        SET ListingId = %s,
            ModificationTimestamp = %s,
            PropertyType = %s,
            PropertySubType = %s,
            UnparsedAddress = %s,
            PostalCity = %s,
            StateOrProvince = %s,
            Country = %s,
            ListPrice = %s,
            PostalCode = %s,
            BedroomsTotal = %s,
            BathroomsTotalInteger = %s,
            StandardStatus = %s,
            PhotosCount = %s,
            Cooling = %s,
            Heating = %s,
            Latitude = %s,
            Longitude = %s,
            LeadRoutingEmail = %s,
            FireplaceYN = %s,
            WaterfrontYN = %s
        WHERE ListingKey= %s
    """
    data: list[Any] = _create_data(properties[listing_key])

    listing_key = data.pop(0)
    data.append(listing_key)

    cursor.execute(sql, data)


def delete_property(listing_key: str, cursor: Cursor) -> None:
    """Delete the property table entry with given ListingKey."""

    sql: str = """
        DELETE FROM property
        WHERE ListingKey = %s
    """
    data: list[str] = [listing_key]

    cursor.execute(sql, data)


def get_listing_states(cursor: Cursor) -> dict[str, int]:
    """Get all listing keys from database and return the keys."""
    sql: str = """
        SELECT ListingKey
        FROM property
    """

    listing_keys: dict[str, int] = {}

    cursor.execute(sql)
    results: list = cursor.fetchall()
    if results:
        listing_keys = {i[0]: int(ListingState.DEFAULT) for i in results}

    return listing_keys


def insert_media(media_values: list[str], cursor: Cursor) -> None:
    """Insert media entry to media table."""
    sql = """
        INSERT INTO media (
            MediaKey,
            MediaURL,
            MediaModificationTimestamp,
            ListingKey
        ) VALUES (
            %s, %s, %s, %s
        )
    """
    media: list[str] = media_values.copy()
    media.pop(-1)
    listing_key = media.pop(1)
    media.append(listing_key)

    cursor.execute(sql, media)


def update_media(media_values: list[str], cursor: Cursor) -> None:
    """Update media table."""
    sql = """
        UPDATE media
        SET MediaURL = %s,
            MediaModificationTimestamp = %s
        WHERE ListingKey=%s
            AND MediaKey=%s
    """
    media: list[str] = media_values.copy()
    media.pop(-1)
    listing_key: str = media.pop(1)
    media_key: str = media.pop(0)
    media.append(listing_key)
    media.append(media_key)

    cursor.execute(sql, media)


def delete_media(listing_key: str, cursor: Cursor) -> None:
    """Delete the media from media table."""
    sql = """
        DELETE FROM media
        WHERE ListingKey = %s
    """
    data: list[str] = [listing_key]

    cursor.execute(sql, data)


def get_medias(cursor: Cursor) -> dict[str, list[Any]]:
    """Get all media data from database and return the dictionary."""
    sql: str = """
        SELECT *
        FROM media
    """

    media: dict[str, list[Any]] = defaultdict(list)

    cursor.execute(sql)
    results = cursor.fetchall()
    if results:
        for item in results:
            media[item[1]].append(list(item) + [int(ListingState.DEFAULT), ])

    return media

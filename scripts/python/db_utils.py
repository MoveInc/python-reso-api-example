import mysql.connector
from enum import IntEnum

keys = ("ListingKey", "ListingId", "ModificationTimestamp", "PropertyType",
        "PropertySubType", "UnparsedAddress", "PostalCity", "StateOrProvince",
        "Country", "ListPrice", "PostalCode", "BedroomsTotal",
        "BathroomsTotalInteger", "StandardStatus", "PhotosCount",
        "Cooling", "Heating", "Latitude", "Longitude",
        "LeadRoutingEmail", "FireplaceYN", "WaterfrontYN")

media_keys = ("MediaKey", "MediaURL", "MediaModificationTimestamp")


# Enum class for listing states.
class ListingState(IntEnum):
    DEFAULT = 0
    NEW = 1
    UPDATE = 2
    DELETE = 3
    NO_CHANGE = 4


# Create a list of data.
def _create_data(property):
    data = []
    for key in keys:
        if key not in property:
            data.append(None)
        elif type(property[key]) is list:
            data.append(str(property[key]).strip('[]'))
        else:
            data.append(property[key])
    return data


# Insert property values to the property table
def insert_table_data(key, properties, db_conn, db_cursor):
    sql_query = '''INSERT INTO property (ListingKey, ListingId,
                    ModificationTimestamp, PropertyType, PropertySubType,
                    UnparsedAddress, PostalCity, StateOrProvince,
                    Country, ListPrice, PostalCode, BedroomsTotal,
                    BathroomsTotalInteger, StandardStatus, PhotosCount,
                    Cooling, Heating, Latitude, Longitude,
                    LeadRoutingEmail, FireplaceYN, WaterfrontYN)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    data = _create_data(properties[key])
    _execute_query_with_rollback(sql_query, data, db_conn, db_cursor)


# insert media entry to media table.
def insert_media(media_values, db_conn, db_cursor):
    sql_query_media = '''INSERT INTO media (MediaKey, MediaURL,
                          MediaModificationTimestamp, ListingKey)
                          VALUES (%s, %s, %s, %s)'''
    media = media_values.copy()
    media.pop(-1)
    listingKey = media.pop(1)
    media.append(listingKey)
    _execute_query_with_rollback(sql_query_media, media, db_conn, db_cursor)


# Update media table
def update_media(media_values, db_conn, db_cursor):
    sql_query_media = '''UPDATE media SET MediaURL = %s,
                          MediaModificationTimestamp = %s WHERE
                          ListingKey=%s AND MediaKey=%s'''
    media = media_values.copy()
    media.pop(-1)
    listingKey = media.pop(1)
    mediaKey = media.pop(0)
    media.append(listingKey)
    media.append(mediaKey)
    _execute_query_with_rollback(sql_query_media, media, db_conn, db_cursor)


# Delete the media from media table
def delete_media(media, db_conn, db_cursor):
    sql_query_media = '''DELETE FROM media WHERE MediaKey = %s and
                        ListingKey = %s'''
    data = (media[0], media[1],)
    _execute_query_with_rollback(sql_query_media, data, db_conn, db_cursor)


# Update property table.
def update_table_data(key, properties, db_conn, db_cursor):
    sql_query = '''UPDATE property SET ListingId = %s,
                    ModificationTimestamp = %s, PropertyType = %s,
                    PropertySubType = %s, UnparsedAddress = %s,
                    PostalCity = %s, StateOrProvince = %s, Country = %s,
                    ListPrice = %s, PostalCode = %s, BedroomsTotal = %s,
                    BathroomsTotalInteger = %s, StandardStatus = %s,
                    PhotosCount = %s, Cooling = %s,
                    Heating = %s, Latitude = %s, Longitude = %s,
                    LeadRoutingEmail = %s, FireplaceYN = %s,
                    WaterfrontYN = %s WHERE ListingKey=%s'''
    data = _create_data(properties[key])
    ListingKey = data.pop(0)
    data.append(ListingKey)
    _execute_query_with_rollback(sql_query, data, db_conn, db_cursor)


# Execute sql query with rollback when fails.
def _execute_query_with_rollback(sql_query, data, db_conn, db_cursor):
    try:
        db_cursor.execute(sql_query, data)
        db_conn.commit()
    except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
        print("DataError or IntegrityError")
        print(err)
        db_conn.rollback()
    except mysql.connector.ProgrammingError as err:
        print("Programming Error")
        print(err)
        db_conn.rollback()
    except mysql.connector.Error as err:
        print(err)
        db_conn.rollback()


# Execute sql query.
def _execute_query(sql_query, data, db_conn, db_cursor):
    try:
        db_cursor.execute(sql_query, data)
        return db_cursor
    except (mysql.connector.IntegrityError, mysql.connector.DataError) as err:
        print("DataError or IntegrityError")
        print(err)
    except mysql.connector.ProgrammingError as err:
        print("Programming Error")
        print(err)
    except mysql.connector.Error as err:
        print(err)
    return None


# Get all listing keys from database and return the keys.
def get_listingkeys_db(db_conn, db_cursor):
    ListingKeys = {}
    sql_query = '''SELECT ListingKey FROM property'''
    data = []
    db_result_cursor = _execute_query(sql_query, data, db_conn, db_cursor)
    if db_result_cursor is not None:
        ListingKeys = {i[0]: int(ListingState.DEFAULT) for i in
                       db_result_cursor.fetchall()}
    return ListingKeys


# Get all media data from database and return the dictionary.
def get_media_db(db_conn, db_cursor):
    media = {}
    sql_query = '''SELECT * FROM media'''
    data = []
    db_result_cursor = _execute_query(sql_query, data, db_conn, db_cursor)
    if db_result_cursor is not None:
        for i in db_result_cursor.fetchall():
            if i[1] in media:
                media[i[1]].append(list(i) + [int(ListingState.DEFAULT), ])
            else:
                media[i[1]] = [list(i) + [int(ListingState.DEFAULT), ], ]
    return media


# Delete the property table entry with given ListingKey.
def delete_table_data(listingkey, db_conn, db_cursor):
    sql_query = '''DELETE FROM property WHERE ListingKey = %s'''
    data = (listingkey,)
    _execute_query_with_rollback(sql_query, data, db_conn, db_cursor)

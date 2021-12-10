import db_utils
import os
import requests
import dateutil.parser as DP

path_to_download = "./media/"


# Convert iso format to datetime.
def _iso_to_time(time_str):
    timestamp = time_str[:-1]
    parsed_time = DP.parse(timestamp)
    return parsed_time


# Compare media and media from database using the modification timestamp
# and update the status
def _check_modification_time(media, media_db):
    media_modified_time = _iso_to_time(media["MediaModificationTimestamp"])
    media_db_modified_time = _iso_to_time(media_db[3])
    if media_modified_time > media_db_modified_time:
        media_db[2] = media["MediaURL"]
        media_db[3] = media["MediaModificationTimestamp"]
        media_db[4] = int(db_utils.ListingState.UPDATE)
    else:
        media_db[4] = int(db_utils.ListingState.NO_CHANGE)


def _create_media(media, listingKey):
    return [media["MediaKey"], listingKey, media["MediaURL"],
            media["MediaModificationTimestamp"],
            int(db_utils.ListingState.NEW)]


# Combine two dictionaries and copy the new ones to the source.
def _merge_media(media_db, new_media):
    for listingKey in new_media:
        for media in new_media[listingKey]:
            media_db[listingKey].append(media)


# Parse the properties and modify the media status from database
# by comparing the values present in the database.
# Based on the parsing results, the media is divided into,
# values to be DELETED, ADDED and MODIFIED in the database.
def parse_media(properties, media_db, is_full_pull):
    new_media = {}
    for listingKey in media_db:
        # If ListingKey from database is in the properties
        if listingKey in properties:
            for media in properties[listingKey]["Media"]:
                media_exists = False
                for i in range(len(media_db[listingKey])):
                    # If mediaKey from property matches the database entry
                    # Check for any change in LastModifiedTime
                    if media["MediaKey"] == media_db[listingKey][i][0]:
                        media_exists = True
                        _check_modification_time(media, media_db[listingKey][i])
                    if i == len(media_db[listingKey]) - 1:
                        # If mediaKey is not present in the database,
                        # Add it to the database
                        if media_exists is False:
                            try:
                                new_media[listingKey].append(_create_media(
                                    media, listingKey))
                            except KeyError:
                                new_media[listingKey] = [_create_media(
                                    media, listingKey)]
        else:
            # If ListingKey from database is not in properties
            for i in range(len(media_db[listingKey])):
                # Delete the property from database if full pull
                if is_full_pull is True:
                    media_db[listingKey][i][4] = int(
                        db_utils.ListingState.DELETE)
                # Do not change anything if not full pull
                else:
                    media_db[listingKey][i][4] = int(
                        db_utils.ListingState.NO_CHANGE)
    _merge_media(media_db, new_media)
    # If listingkey from properties is not present in database, add
    # the respective media of that ListingKey as NEW to the database
    for key in properties:
        if media_db.get(key) is None:
            media_values = properties[key]["Media"]
            for media in media_values:
                try:
                    media_db[key].append(_create_media(media, key))
                except KeyError:
                    media_db[key] = [_create_media(media, key)]
    return media_db


# Create a directory with ListingKey as parent directory and
# download the media to respective ListingKey directories
def _download_photo(media):
    file_dir = path_to_download + media[1] + '/'
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    response = requests.get(media[2])
    file_name = file_dir + media[2].rpartition('/')[-1] + '.jpeg'
    file = open(file_name, "wb")
    file.write(response.content)
    file.close()


# Delete the media and check if this is the last photo in the
# directory. If so, delete the directory.
def _delete_photo(media):
    file_dir = path_to_download + media[1] + '/'
    file_name = file_dir + media[2].rpartition('/')[-1] + '.jpeg'
    if os.path.exists(file_name):
        os.remove(file_name)
    dir = os.listdir(file_dir)
    if len(dir) == 0:
        os.rmdir(file_dir)


# Iterate through all the media items and download/delete the photos.
def handle_photos(media_db):
    for key in media_db:
        for i in range(len(media_db[key])):
            if media_db[key][i][4] == int(db_utils.ListingState.NEW):
                _download_photo(media_db[key][i])
            elif media_db[key][i][4] == int(db_utils.ListingState.UPDATE):
                _download_photo(media_db[key][i])
            elif media_db[key][i][4] == int(db_utils.ListingState.DELETE):
                _delete_photo(media_db[key][i])
            elif media_db[key][i][4] == int(db_utils.ListingState.DEFAULT):
                _delete_photo(media_db[key][i])

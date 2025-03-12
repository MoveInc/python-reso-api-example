import datetime
import requests

from multiprocessing import Pool
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import JSONB
from time import sleep

Base = declarative_base()
engine = create_engine('postgresql://root:root@localhost:5432/postgres', echo=False)

credentials = {
    "client_id": "public_sandbox",
    "client_secret": "public_sandbox",
    "grant_type": "client_credentials"
}


class Property(Base):
    __tablename__ = 'properties'
    listingkey = Column(String, primary_key=True)
    blob = Column(JSONB)


def get_access_token():
    response = requests.post("https://api.listhub.com/oauth2/token", data=credentials)
    return response.json()["access_token"]


def get_keys(token, params=""):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip"
    }
    response = requests.get(f"https://api.listhub.com/odata/Sync?$orderby=ListingKey asc&{params}", headers=headers)

    return response.json()


# keys_and_timestamps = [{'ListingKey': '3yd-FAKE1-66906', 'ModificationTimestamp': '2021-08-10T12:55:45.000-05:00'}, {'ListingKey': '3yd-FAKE1-1955807', 'ModificationTimestamp': '2021-08-10T12:55:45.000-05:00'}]
# split them into a list of strings no longer than 7,800 bytes (url length limit is somewhere around 8000) or 500 listings (api limit)
def split_keys(keys_and_timestamps):
    keys = []
    current_keys = ""
    current_count = 0
    for k in keys_and_timestamps['value']:
        # If current byte length is 7,800 or current count is 500
        byte_len_of_keys = len(current_keys.encode('utf-8'))
        if byte_len_of_keys >= 7800 or current_count >= 500:
            # Remove trailing comma
            current_keys = current_keys[:-1]
            keys.append(current_keys)
            current_keys = ""
            current_count = 0
        # Replace single quotes inside of ListingKey with double single quotes
        # for listings like '3yd-DVHBN-Walker'sHill-206' Irish Hill Drive
        lk = k['ListingKey'].replace("'", "%27%27")
        current_keys += "'" + lk + "'" + ","
        current_count += 1
    keys.append(current_keys)
    return keys


def get_properties(keys, token, fails=0):
    headers = {
        "Authorization": f"Bearer {token}"
    }
    # response = {"value":[{}, {}, {}, ...{}]}
    response = requests.get(f"https://api.listhub.com/odata/Property({keys})", headers=headers)
    print("Properties retrieved")
    jres = response.json()
    if 'value' not in jres and response.status_code != 200:
        print("CODE", response.status_code)
        if response.status_code == 404:
            return 'Done'
        sleep(10)
        if fails > 5:
            raise Exception("Max retries exceeded")
        get_properties(keys, token, fails+1)
    store_properties(jres)


def store_properties(props):
    session = Session(engine)

    for prop in props['value']:
        property = Property(
            blob = prop,
            listingkey = prop['ListingKey']
        )
        session.merge(property)
    session.commit()
    session.close()
    print(f"Stored {len(props['value'])} properties")


def clean_properties(keys_and_timestamps):
    print("Cleaning properties")
    api_keys = [k['ListingKey'] for k in keys_and_timestamps['value']]
    session = Session(engine)
    session.query(Property).filter(Property.listingkey.notin_(api_keys)).delete()
    session.commit()
    session.close()


def main():
    print("Initializing...")
    Base.metadata.create_all(engine)
    token = ""
    pools = 10

    keys_and_timestamps = ""

    # On script start do a full sync of data
    startup = True

    while True:
        # Every 12 hours do a full sync of data. Minute <= 30 attempts to
        # make sure the full only runs one time within the hour
        now = datetime.datetime.now()
        if (now.hour % 12 == 0 and now.minute < 30) or startup:
            pool = Pool(pools)
            startup = False
            token = get_access_token()
            print("Token retrieved")
            # Clean up properties that exist in the database, but not the API.
            keys_and_timestamps = get_keys(token)
            clean_properties(keys_and_timestamps)
            print("Cleaned properties")
            keys = split_keys(keys_and_timestamps)
            print("Keys retrieved")
            pool.starmap_async(
                get_properties,
                [(k, token) for k in keys]
            ).get()
            pool.close()
            print("Properties stored")
        # Every half hour (depending on the sleep duration) after the full sync do a incremental update
        else:
            pool = Pool(pools)
            token = get_access_token()
            print("Token retrieved")
            # Using an hour instead of half hour allows us a safety net in case something is missed from the previous incremental update.
            # Ultimately the full update every 12 hours will reconcile any discrepancies as well.
            an_hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)
            keys_and_timestamps = get_keys(token, "ModificaitonTimestamp gt '" + an_hour_ago.strftime('%Y-%m-%dT%H:%M:%SZ') + "'")
            print("Keys retrieved")
            keys = split_keys(keys_and_timestamps)
            pool.starmap_async(
                get_properties,
                [(k, token) for k in keys]
            ).get()
            pool.close()
            print("Properties stored")
        # Sleep until the next half hour, meaning if the incremental update starts at 12:30 and takes 20 minutes
        # the next run will start 10 minutes later at 1:00.
        next_half_hour = datetime.datetime.now().replace(minute=30, second=0, microsecond=0) + datetime.timedelta(minutes=30)
        sleep((next_half_hour - datetime.datetime.now()).total_seconds())


if __name__ == "__main__":
    main()

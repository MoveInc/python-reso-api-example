# RESO web API data pull using Python

This repo is meant to be an example of how to pull data from the RESO API and
insert it into a local DB. After starting the full pull runs once every 24 hours
to get all the listings of a publisher using the API. The modified listings are
updated and the inactive listings are deleted from the database, each time full
pull is run.

- The script also helps to download the media for all the listings in the
  database. Incremental pulls run every hour once the full pull is done. The
  incremental run only updates listings.
- The script will download 500 listings at a time and insert them into the db.

## Note

- The full dataset takes a while to download.
- You will need mysql running, by default the database uses username=root
  password=root and can be changed in src/create_db.py
  - The attached `docker-compose.yml` can be used to act as the database, it is
    not persistent.
- There are several improvements that can be made, but this is the general
  approach to getting data.

## Usage:

Get all the listings for the publisher with the provided client-id and
client-secret.

```bash
./get_listings_data.py -i <client-id> -s <client-secret> -d <database-name> -u <database-username> -c <database-password>
```

### Required arguments

- `-i` / `--id`: publisher id
- `-s` / `--secret`: publisher secret
- `-d` /`--database`: database name
- `-u` / `--db_user`: database user
- `-c` / `--db_password`: database password

#### Example:

```bash
docker-compose up -d
pip3 install -r requirements.txt
cd src/
./get_listings_data.py -i public_sandbox -s public_sandbox -d testing -u root -c root
```

Running this starts a loop that will download a full data set from the api, then
every hour after will download an incremental until a day has passed. After a
day has passed the full will run again. The full deletes old listings,
incrementals do not.

Running tests

```bash
cd src/
python3 -m unittest discover -s ../tests -p 'test*.py'
```

Documentation on how to query and use the API can be found at
[api.listhub.com](https://api.listhub.com).

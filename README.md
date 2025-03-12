# RESO web API data pull using Python

This repo is meant to be a minimal example of how to pull data from the RESO API and
insert it into a local DB. After starting the full pull runs and then runs again once
at hour 0 and once at hour 12 to get all the listings of a publisher using the API. The modified
listings are updated and the inactive listings are deleted from the database, each
time full pull is run.

- The script uses the API best practices shown in the [RESO API documentation](https://api.listhub.com).

## Note

- The full dataset takes a while to download. It is faster in the cloud, but can take around an hour or more locally. You can also increase the number of processes in the pool to increase the speed of the process, but beaware of the rate limits.
- You will need postgres running, by default the database uses username=root
  password=root and can be changed in the docker-compose.yml file.
  - The attached `docker-compose.yml` can be used to act as the database, it is
    not persistent.
- There are several improvements that can be made and this does not include media processing, but this is the general
  approach to getting data.

## Usage:
```bash
docker compose up -d
pip3 install -r requirements.txt
python3 src/main.py
```

Running this starts a loop that will download a full data set from the api, then
every half hour after will download an incremental update.

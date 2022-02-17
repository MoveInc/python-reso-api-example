# RESO web API data pull using Python
> This repo is meant to be an example of how to pull data from the RESO API and insert it into a local DB.
After starting the full pull runs once every 24 hours to get all the listings of a publisher using the API. 
The modified listings are updated and the inactive listings are deleted from the database, each time full pull is run. 
>- The script also helps to download the media for all the listings in the database.
Incremental pulls run every hour once the full pull is done. The incremental run only updates listings.
>- The script will download 500 listings at a time and insert them into the db. After all data is inserted/updated the photos will download if the script has been told to do so.
## Note:
> The full dataset takes a while to download.
> There are several improvements that can be made, but this is the general approach to getting data; for example you may want to download photos as you insert properties into the database, or you may process photos separately.
#Usage:
>Get all the listings for the publisher with the provided client-id and client-secret.
>- ./get_listings_data.py -i &lt;client-id&gt; -s &lt;client-secret&gt;

#### Example:
>- ./get_listings_data.py -i "public_sandbox" -s "public_sandbox"
- Running this starts a loop that will download a full data set from the api, then every hour after will download an incremental until a day has passed. After a day has passed the full will run again. The full deletes old listings, incrementals do not.
>- Running tests
- python3 -m unittest discover -s tests -p 'test*.py'
## Documentation on how to query and use the API can be found at <a href="https://api.listhub.com">api.listhub.com</a>

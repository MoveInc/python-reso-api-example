# RESO web API data pull using Python
> Sample script to get all the listings and store in a local database "listings_db".
Full pull runs once in every 24 hours to get all the listings of a publisher using the API. The modified listings are updated and the inactive listings are deleted from the database, each time full pull is run. The script also helps to download the media for all the listings in the database.
Incremental runs every hour once the full pull is done. This updates the listings.
>- The script downloads all listings first, downloads the photos, then inserts the data into the database. This means the download process is all or nothing.
## Note:
>- The full dataset pull takes a while.
>- You will need an mssql server running, the script by default uses user root and password root which can be changed in /scripts/python/create_db.py

# Usage: 
>Get all the listings for the publisher with the provided client-id and client-secret.
>- ./get_listings_data.py -i &lt;client-id&gt; -s &lt;client-secret&gt;

#### Example:
>- ./get_listings_data.py -i "public_sandbox" -s "public_sandbox"
- Running this starts a loop that will download a full data set from the api, then every hour after will download an incremental until a day has passed. After a day has passed the full will run again. The full deletes old listings, incrementals do not.

## Documentation on how to query and use the API can be found at <a href="https://api.listhub.com">api.listhub.com</a>

#!/usr/bin/env python3

# usage: ./get_listings_data.py -i <client-id> -s <client-secret>
# example: ./get_listings_data.py -i "public_sandbox" -s "public_sandbox"

from optparse import OptionParser
import create_db
import db_utils
import helper_utils
from datetime import datetime, timedelta
from time import sleep

time_incremental_cursor = ""


def full_pull(access_token, db_conn, db_cursor):
    global time_incremental_cursor
    url = "https://api.listhub.com/odata/Property?$select=Media,"
    url = url + ",".join([x for x in db_utils.keys if x != "LeadRoutingEmail"]) + ",CustomFields"
    last_access_time = time_incremental_cursor
    time_incremental_cursor = helper_utils.get_current_time_iso()
    properties = helper_utils.get_properties(url, access_token)
    listingKeys_db = db_utils.get_listingkeys_db(db_conn, db_cursor)
    helper_utils.parse_listings(properties, listingKeys_db, True,
                                last_access_time, db_conn, db_cursor)


def incremental_run(access_token, db_conn, db_cursor):
    global time_incremental_cursor
    last_access_time = time_incremental_cursor
    time_incremental_cursor = helper_utils.get_current_time_iso()
    base_url = "https://api.listhub.com/odata/Property?"
    select = "$select=Media," + ",".join([x for x in db_utils.keys if x != "LeadRoutingEmail"]) + ",CustomFields"
    query = "&$filter=ModificationTimestamp gt " + last_access_time
    url = base_url + select + query
    properties = helper_utils.get_properties(url, access_token)
    listingKeys_db = db_utils.get_listingkeys_db(db_conn, db_cursor)
    helper_utils.parse_listings(properties, listingKeys_db, False,
                                last_access_time, db_conn, db_cursor)


def main():
    global time_incremental_cursor
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-i", "--id", dest="client_id",
                      help="read listings for Client ID")
    parser.add_option("-s", "--secret", dest="client_secret",
                      help="Secret for Client ID")
    (options, args) = parser.parse_args()
    if options.client_id:
        client_id = options.client_id
    if options.client_secret:
        client_secret = options.client_secret
    create_db.create_db()
    last_pull = datetime.now()
    time_incremental_cursor = helper_utils.get_current_time_iso()
    first_pull = True
    while True:
        try:
            if (datetime.now() - last_pull) > timedelta(1) \
                    or first_pull is True:
                print("Full pull started at " + time_incremental_cursor)
                db_conn, db_cursor = create_db.connect_db()
                access_token = helper_utils.authenticate(client_id,
                                                         client_secret)
                full_pull(access_token, db_conn, db_cursor)
                last_pull = datetime.utcnow()
                print("Full pull finished!!")
                sleep(60 * 60)
                first_pull = False
                db_conn.close()
            else:
                db_conn, db_cursor = create_db.connect_db()
                access_token = helper_utils.authenticate(client_id,
                                                         client_secret)
                print("Incremental run started at " +
                      helper_utils.get_current_time_iso())
                incremental_run(access_token, db_conn, db_cursor)
                print("Incremental run finished!!")
                sleep(60 * 60)
                db_conn.close()
        except KeyboardInterrupt:
            print('Manual break by user')
            return


if __name__ == "__main__":
    main()

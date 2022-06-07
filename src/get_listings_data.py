#!/usr/bin/env python3
from optparse import OptionParser
import create_db as create_db
from db_utils import keys, get_listingkeys_db
import helper_utils as helper_utils
from datetime import datetime, timedelta
from time import sleep


time_incremental_cursor: str = helper_utils.get_current_time_iso()
base_url: str = "https://api.listhub.com/odata/Property?$select=Media,CustomFields,"
select: str = ",".join([x for x in keys if x != "LeadRoutingEmail"])


def get_and_reset_time() -> str:
    global time_incremental_cursor
    last_access_time: str = time_incremental_cursor
    time_incremental_cursor = helper_utils.get_current_time_iso()
    return last_access_time


# Begins a full dataset pull, updates and deletes listings
def full_pull(access_token: str, db_conn: str, db_cursor: str, opts: dict) -> None:
    get_and_reset_time()
    # Final url for pull is base url + the select statement
    url = base_url + select
    begin(access_token, db_conn, db_cursor, opts, url, True)


# Begins the incremental run process, only updates listings
def incremental_run(access_token: str, db_conn: str, db_cursor: str, opts: dict) -> None:
    last_access_time = get_and_reset_time()
    # Final url for pull is base url + the select statement + the query (filter by timestamp)
    url_query = "&$filter=ModificationTimestamp gt " + last_access_time
    url = base_url + select + url_query
    begin(access_token, db_conn, db_cursor, opts, url)


def begin(access_token: str, db_conn: str, db_cursor: str, opts: dict, url: str, full: bool = False) -> None:
    listingkey_db = get_listingkeys_db(db_cursor)
    helper_utils.get_properties(url, access_token, listingkey_db, opts, db_conn, db_cursor, full)


# Parses the arguments passed in to the program into a dict.
def parse_args() -> dict:
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-i", "--id", dest="client_id", help="read listings for Client ID")
    parser.add_option("-s", "--secret", dest="client_secret", help="Secret for Client ID")
    parser.add_option("-d", "--database", dest="db_name", help="Name of the database")
    parser.add_option("-u", "--db_user", dest="db_user", help="Database username")
    parser.add_option("-c", "--db_password", dest="db_password", help="Database password")

    (options, args) = parser.parse_args()
    return vars(options)


def main() -> None:
    opts = parse_args()
    helper_utils.ensure_args(opts)
    last_pull = datetime.now()

    conn = create_db.connect_db(opts)
    create_db.create_db(opts, conn)
    conn.close()

    # First pull will always be a full
    first_pull = True
    while True:
        db_conn = create_db.connect_db(opts)
        with db_conn.cursor() as db_cursor:
            access_token = helper_utils.authenticate(opts["client_id"], opts["client_secret"])
            try:
                if (datetime.now() - last_pull) > timedelta(1) or first_pull is True:
                    print("Full pull started at " + time_incremental_cursor)
                    full_pull(access_token, db_conn, db_cursor, opts)
                    last_pull = datetime.utcnow()
                    print(f"Full pull finished {datetime.now()}!!")
                    sleep(60 * 60)
                    first_pull = False
                    db_conn.close()
                else:
                    print("Incremental run started at " + helper_utils.get_current_time_iso())
                    incremental_run(access_token, db_conn, db_cursor, opts)
                    print("Incremental run finished!!")
                    sleep(60 * 60)
                    db_conn.close()
            except KeyboardInterrupt:
                print('Manual break by user')
                return


if __name__ == "__main__":
    main()

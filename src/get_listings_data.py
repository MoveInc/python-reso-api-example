#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from optparse import OptionParser
from time import sleep

from pymysql.connections import Connection

import db_utils
import helper_utils
from data_utils import PROPERTY_COLUMNS

BASE_URL: str = "https://api.listhub.com/odata/Property?$select=Media,CustomFields,"
SELECT_FIELDS: str = ",".join([x for x in PROPERTY_COLUMNS if x != "LeadRoutingEmail"])

time_incremental_cursor: str = helper_utils.get_current_time_iso()


def _get_and_reset_time() -> str:
    global time_incremental_cursor
    last_access_time: str = time_incremental_cursor
    time_incremental_cursor = helper_utils.get_current_time_iso()
    return last_access_time


def _full_pull(access_token: str, connection: Connection, options: dict) -> bool:
    """Begins a full dataset pull and updates and deletes listings."""
    print(f"Full pull started at: {helper_utils.get_current_time_iso()}")

    _get_and_reset_time()

    # Final url for pull is base url + the select fields
    url: str = BASE_URL + SELECT_FIELDS

    success: bool = helper_utils.get_properties(url, access_token, connection, options, True)

    print(f"Full pull finished: {helper_utils.get_current_time_iso()}")

    return success


def _incremental_pull(access_token: str, connection: Connection, options: dict) -> bool:
    """Begins an incremental dataset pull and only updates listings."""
    print(f"Incremental pull started at: {helper_utils.get_current_time_iso()}")

    last_access_time: str = _get_and_reset_time()

    # Final url for pull is base url + the select fields + the timestamp filter
    url_query: str = f"&$filter=ModificationTimestamp gt {last_access_time}"
    url: str = BASE_URL + SELECT_FIELDS  + url_query

    success: bool = helper_utils.get_properties(url, access_token, connection, options)

    print(f"Incremental pull finished: {helper_utils.get_current_time_iso()}")

    return success


def _parse_args() -> dict[str, str]:
    """Parses the arguments passed in to the program into a dict."""
    usage: str = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-i", "--id", dest="client_id", help="read listings for Client ID")
    parser.add_option("-s", "--secret", dest="client_secret", help="Secret for Client ID")
    parser.add_option("-d", "--database", dest="db_name", help="Name of the database")
    parser.add_option("-u", "--db_user", dest="db_user", help="Database username")
    parser.add_option("-c", "--db_password", dest="db_password", help="Database password")

    options, _ = parser.parse_args()
    return vars(options)


def _ensure_arg(args: dict[str, str], key: str) -> str:
    """
    Ensures an argument exists in args or the environment otherwise raises an error.
    """
    value: str = args.get(key) or os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required option '{key}' is not set in options or the environment.")

    return value


def _ensure_args(options: dict[str, str]):
    required_options: list[str] = ["client_id", "client_secret", "db_name", "db_user", "db_password"]
    for key in required_options:
        options[key] = _ensure_arg(options, key)


def main() -> None:
    options: dict[str, str] = _parse_args()
    _ensure_args(options)

    # First pull will always be a full
    first_pull: bool = True
    last_pull: datetime = datetime.now()

    db_name: str = options["db_name"]
    db_user: str = options["db_user"]
    db_password: str = options["db_password"]

    while True:
        connection: Connection = db_utils.get_db_connection(db_name, db_user, db_password)
        if not connection:
            print(f"Unable to get a connection to the database {db_name}")
            break

        success: bool = False
        access_token: str = helper_utils.authenticate(options["client_id"], options["client_secret"])
        try:
            if first_pull or ((datetime.now() - last_pull) > timedelta(1)):
                success = _full_pull(access_token, connection, options)
                connection.close()

                first_pull = False
            else:
                success = _incremental_pull(access_token, connection, options)
                connection.close()

            if not success:
                return

            last_pull = datetime.now()
            sleep(60 * 60)
        except KeyboardInterrupt:
            print('Manual break by user')
            return


if __name__ == "__main__":
    main()

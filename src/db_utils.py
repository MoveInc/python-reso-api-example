from typing import Optional

import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor


def _create_database(cursor: Cursor, db_name: str) -> bool:
    """Create a database."""
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        cursor.execute(f"USE {db_name}")

        print(f"Created database '{db_name}'")
    except pymysql.Error as err:
        print(f"Failed to create database '{db_name}':\n{err}")
        return False

    return True


def _create_table(cursor: Cursor, sql: str, table_name: str) -> bool:
    """Create a table."""
    try:
        cursor.execute(sql)
        print(f"Created table '{table_name}'")
    except pymysql.Error as err:
        # Treat OperationalError as table exists and a success
        if type(err) is not pymysql.err.OperationalError:
            print(f"Failed to create table '{table_name}':\n{err}")
            return False

    return True


def _create_tables(cursor: Cursor) -> bool:
    """Create property and media tables."""
    create_property: str = """
        CREATE TABLE property (
            ListingKey varchar(255) NOT NULL,
            ListingId varchar(255),
            ModificationTimestamp varchar(255),
            PropertyType varchar(255),
            PropertySubType varchar(255),
            UnparsedAddress varchar(255),
            PostalCity varchar(255),
            StateOrProvince varchar(255),
            Country varchar(255),
            ListPrice int,
            PostalCode varchar(255),
            BedroomsTotal int,
            BathroomsTotalInteger int,
            StandardStatus varchar(255),
            PhotosCount int,
            Cooling varchar(255),
            Heating varchar(255),
            Latitude decimal(12,8),
            Longitude decimal(12,8),
            LeadRoutingEmail varchar(255),
            FireplaceYN boolean,
            WaterfrontYN boolean,
            PRIMARY KEY (ListingKey)
        )
   """

    if not _create_table(cursor, create_property, "property"):
        return False

    create_media: str = """
        CREATE TABLE media (
            MediaKey varchar(255) NOT NULL,
            ListingKey varchar(255) NOT NULL,
            MediaURL varchar(600),
            MediaModificationTimestamp varchar(255),
            FOREIGN key (ListingKey) REFERENCES
            property(ListingKey)
        )
   """

    return _create_table(cursor, create_media, "media")


def _create_db(connection: Connection, db_name: str) -> bool:
    """
    Create a database and the tables.
    """
    success: bool = True

    with connection.cursor() as cursor:
        try:
            cursor.execute(f"USE {db_name}")
        except pymysql.Error as err:
            success = False
            if type(err) is pymysql.err.OperationalError:
                if _create_database(cursor, db_name):
                    connection.database = db_name
                    success = True
            else:
                print(err)

        if success:
            success = _create_tables(cursor)

        cursor.close()

    return success


def get_db_connection(db_name: str, db_user: str, db_password: str) -> Optional[Connection]:
    """
    Connect to the database and return a connection.
    """
    connection: Connection = pymysql.connect(user=db_user, password=db_password, port=3306)
    return connection if _create_db(connection, db_name) else None


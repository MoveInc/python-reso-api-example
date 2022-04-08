from errno import errorcode
import pymysql


# create database.
def _create_database(cursor, opts: dict):
    try:
        cursor.execute('create database ' + opts["db_name"])
    except pymysql.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


# Helper function to print databases.
def _show_db(cursor):
    cursor.execute("SHOW DATABASES")
    for db in cursor:
        print(db)


# Create tables property and media.
def _create_tables(cursor):
    try:
        create_property = ''' CREATE TABLE property (
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
                        PRIMARY KEY (ListingKey))'''
        create_media = ('CREATE TABLE media ( '
                        'MediaKey varchar(255) NOT NULL, '
                        'ListingKey varchar(255) NOT NULL, '
                        'MediaURL varchar(600), '
                        'MediaModificationTimestamp varchar(255), '
                        'FOREIGN key (ListingKey) REFERENCES '
                        'property(ListingKey))')
        cursor.execute(create_property)
        cursor.execute(create_media)
    except pymysql.Error as err:
        if type(err) is pymysql.err.OperationalError:
            print("Tables already exist.")
        else:
            print(err)
    else:
        print("OK")


# Create a database and the tables. Name of database can be changed by
# assigning value to the global variable DB_NAME.
def create_db(opts: dict, db_connection):
    with db_connection.cursor() as cursor:
        try:
            cursor.execute('USE ' + opts["db_name"])
        except pymysql.Error as err:
            print("Database {} does not exists.".format(opts["db_name"]))
            if type(err) is pymysql.err.OperationalError:
                _create_database(cursor, opts)
                print("Database {} created successfully.".format(opts["db_name"]))
                db_connection.database = opts["db_name"]
            else:
                print(err)
        _create_tables(cursor)
        cursor.close()


# Connect to the database and return the db connector and cursor.
def connect_db(opts: dict):
    try:
        connection = pymysql.connect(user='root', password='root')
        with connection.cursor() as cursor:
            cursor.execute('USE ' + opts["db_name"])
        print(f"DB NAME {opts['db_name']}")
        return connection
    except pymysql.err.OperationalError as err:
        create_db(opts, connection)
        print("No database named {}".format(err))
        return connection

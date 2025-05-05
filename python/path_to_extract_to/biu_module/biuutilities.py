from cryptography.fernet import Fernet
import pickle  # for object serialization and deserialization
import boto3
import json
import snowflake.connector
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from snowflake.sqlalchemy import URL
import pandas as pd
from pandas import DataFrame
import json
import boto3
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes
import dill
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import base64


key = Fernet.generate_key()
cipher_suite = Fernet(key)

global_key = None
global_iv = None


pandas_connection = None


def __validate_query_tag_string(query_tag_string):
    """
    Validates the format of the query_tag_string.

    Parameters:
    - query_tag_string (str): The JSON-formatted string to validate.

    Returns:
    - bool: True if the string is valid, False otherwise.
    - str: A message indicating the validation result.
    """
    mandatory_keys = {"owner": str}
    optional_keys = {"team": str, "source": str, "job": str, "table": str}

    try:
        query_tag_dict = json.loads(query_tag_string)
    except json.JSONDecodeError:
        return False, "Invalid JSON format"

    for key, value_type in mandatory_keys.items():
        if key not in query_tag_dict:
            return False, f"Missing required key: {key}"
        if not isinstance(query_tag_dict[key], value_type):
            return (
                False,
                f"Incorrect type for key '{key}', expected {value_type.__name__}",
            )

    for key, value_type in optional_keys.items():
        if key in query_tag_dict:
            if not isinstance(query_tag_dict[key], value_type):
                return (
                    False,
                    f"Incorrect type for key '{key}', expected {value_type.__name__}",
                )

    return True, "Valid query_tag_string"


key_id = "QUtJQVU2UTIzUldUM1MzSEgzNkM="


def __helper1(string):
    return base64.b64decode(string).decode("utf-8")


def __get_AWS_secret(system, environment):
    """
    Retrieves secrets from AWS Secrets Manager for a specified system and environment.

    Parameters:
    - system (str): The name of the system for which secrets are required, such as 'snowflake' or 'kafka'.
    - environment (str): The environment for which secrets are required, such as 'uat' or 'prod'.

    Returns:
    - dict: A dictionary containing the secrets, parsed from the JSON string returned by AWS Secrets Manager.
    """

    region_name = "ap-south-1"
    secrets_client = boto3.client(
        service_name="secretsmanager",
        aws_access_key_id=__helper1(key_id),
        aws_secret_access_key=__helper1(access_key),
        region_name=region_name,
    )
    secret_arn = "arn:aws:secretsmanager:ap-south-1:340432489895:secret:Snowflake_BIU_PROD_ANALYTICS-LbmTrJ"

    auth_token = secrets_client.get_secret_value(SecretId=secret_arn).get(
        "SecretString"
    )
    secret = json.loads(auth_token)

    return secret


def get_sfOptions(environment, role):
    """
    Constructs and returns a dictionary of Snowflake options based on the given environment.

    Parameters:
    - environment (str): The environment for which Snowflake options are required, such as 'uat' or 'prod'.

    Returns:
    - dict: A dictionary containing Snowflake options like user, password, database, role, etc.
    """
    secret = __get_AWS_secret("snowflake", environment.lower())
    sfOptions = {
        "sfUser": secret.get("sfUser"),
        "sfPassword": secret.get("sfPassword"),
        "sfAccount": secret.get("sfAccount"),
        "sfDatabase": secret.get("sfDatabase"),
        "sfWarehouse": secret.get("sfWarehouse"),
        "sfSchema": "UNO_DS",
        "sfRole": role,
        "sfURL": "zp87655.ap-south-1.privatelink.snowflakecomputing.com",
        "truncate_table": "off",
        "usestagingtable": "off",
    }
    return sfOptions


def get_SMTP_creds():
    """
    Returns the SMTP credentials for email sending.

    Returns:
    - dict: A dictionary containing SMTP credentials like host, port, username, and password.
    """
    smtp_creds = {
        "smtphost": "email-smtp.ap-south-1.amazonaws.com",
        "port": "587",
        "password": "BGw6NCcjrgNTpk3jPrSEZE867p4TyUqKuTE73vX8b/BO",
        "username": "AKIATFQ36KOLSB235DAY",
    }
    return smtp_creds


####################################################################################
# Snowflake methods
####################################################################################


class SecureSnowflakeConnection:
    def __init__(self, conn):
        self.__conn = conn

    def execute(self, query):
        cursor = self.__conn.cursor()
        cursor.execute(query)

    def cursor(self):
        return self.__conn.cursor()

    def close(self):
        self.__conn.close()


def get_sf_connection(environment, role, query_tag, schema_name=None):
    """
    Establishes a connection to a Snowflake database.

    Parameters:
    - environment (str): The environment to connect to (e.g., 'prod', 'dev').
    - role (str): The Snowflake role to assume.
    - query_tag (str): A tag to associate with the query session.
    - schema_name (str, optional): The schema name to use. Defaults to None.

    Returns:
    - snowflake.connector.SnowflakeConnection: The Snowflake connection object.
    """
    global global_key, global_iv, pandas_connection

    # Validate the query_tag
    is_valid, message = __validate_query_tag_string(query_tag)
    if not is_valid:
        raise ValueError(f"Invalid query_tag: {message}")

    secret = __get_AWS_secret("snowflake", environment.lower())
    secret = {
        "user": secret.get("sfUser"),
        "password": secret.get("sfPassword"),
        "account": secret.get("sfAccount"),
        "database": secret.get("sfDatabase"),
        "role": role,
        "warehouse": secret.get("sfWarehouse"),
        "schema": schema_name if schema_name else secret.get("sfSchema"),
        "session_parameters": {"QUERY_TAG": query_tag},
    }
    conn = snowflake.connector.connect(**secret)

    return SecureSnowflakeConnection(conn)


def get_cur(conn):
    """
    Returns a Snowflake cursor object after decrypting the encrypted connection object.

    Parameters:
    - encrypted_conn (bytes): The encrypted Snowflake connection object.

    Returns:
    - snowflake.connector.cursor.SnowflakeCursor: The Snowflake cursor object.
    """
    # dec_conn = __decrypt_and_get_conn(conn)
    cur = conn.cursor()
    return cur


def execute_sf_query(conn, query):
    """
    Executes a SQL query on a Snowflake database after decrypting the encrypted connection object.

    Parameters:
    - encrypted_conn (bytes): The encrypted Snowflake connection object.
    - query (str): The SQL query to execute.

    Returns:
    - None: This function performs an action and does not return anything.
    """

    # Decrypt and get the original connection object
    # dec_conn = __decrypt_and_get_conn(conn)

    # Now proceed with query execution as usual
    cur = conn.cursor()
    cur.execute(query)


access_key = "bDVBeE4wMktTYTFIS1NXaVVLUTZLelhxdHEyRnRZWEZ6SzEwTDdHRQ=="


def fetch_sf_data(conn, query):
    """
    Executes a SQL query on a Snowflake database after decrypting the encrypted connection object.

    Parameters:
    - encrypted_conn (bytes): The encrypted Snowflake connection object.
    - query (str): The SQL query to execute.

    Returns:
    - None: This function performs an action and does not return anything.
    """

    # Decrypt and get the original connection object
    # dec_conn = __decrypt_and_get_conn(conn)

    # Now proceed with query execution as usual
    cur = conn.cursor()
    cur.execute(query)

    return cur.fetchall()


def fetch_sf_dataframe(conn, query):
    """
    Fetches data from a Snowflake database with an optional limit.

    Parameters:
    - conn (snowflake.connector.SnowflakeConnection): The Snowflake connection object.
    - query (str): The SQL query to execute.
    - limit (int, optional): The maximum number of rows to fetch. Defaults to None.

    Returns:
    - pd.DataFrame: A Pandas DataFrame containing the fetched data.
    """

    # Decrypt and get the original connection object
    # dec_conn = __decrypt_and_get_conn(conn)

    cur = conn.cursor()
    df = pd.read_sql(query, conn)

    return df


def bulk_insert_sf(conn, table_name, data):
    """
    Performs a bulk insert into a Snowflake table.

    Parameters:
    - conn (snowflake.connector.SnowflakeConnection): The Snowflake connection object.
    - table_name (str): The name of the table to insert data into.
    - data (list): A list of tuples containing the data to insert.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    # Decrypt and get the original connection object
    # dec_conn = __decrypt_and_get_conn(conn)

    cur = conn.cursor()

    # Prepare the query string based on the number of columns in the first tuple
    num_columns = len(data[0])
    placeholders = ", ".join(["%s"] * num_columns)
    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cur.executemany(query, data)


def begin_transaction(conn):
    """
    Begins a new transaction on a Snowflake database connection.

    Parameters:
    - conn (snowflake.connector.SnowflakeConnection): The Snowflake connection object.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    # dec_conn = __decrypt_and_get_conn(conn)
    cur = conn.cursor()
    cur.execute("BEGIN")


def commit_transaction(conn):
    """
    Commits the current transaction on a Snowflake database connection.

    Parameters:
    - conn (snowflake.connector.SnowflakeConnection): The Snowflake connection object.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    # dec_conn = __decrypt_and_get_conn(conn)
    cur = conn.cursor()
    cur.execute("COMMIT")


def rollback_transaction(conn):
    """
    Rolls back the current transaction on a Snowflake database connection.

    Parameters:
    - conn (snowflake.connector.SnowflakeConnection): The Snowflake connection object.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    # dec_conn = __decrypt_and_get_conn(conn)
    cur = conn.cursor()
    cur.execute("ROLLBACK")


def close_sf_connection(conn):
    """
    Closes a Snowflake database connection.

    Parameters:
    - conn (snowflake.connector.SnowflakeConnection): The Snowflake connection object.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    # dec_conn = __decrypt_and_get_conn(conn)
    conn.close()


####################################################################################
# Pandas methods
####################################################################################


def read_pandas_sf(conn, query):
    """
    Reads data from a Snowflake table into a Pandas DataFrame.

    Parameters:
    - conn: The Snowflake connection object or SQLAlchemy engine.
    - query (str): The SQL query to execute for data retrieval.

    Returns:
    - pandas.DataFrame: The fetched data as a DataFrame.
    """

    df = pd.read_sql(query, conn)

    return df


def write_pandas_sf(conn, df, table_name, schema):
    """
    Writes data from a Pandas DataFrame to a Snowflake table.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the data to write.
    - conn: The Snowflake connection object or SQLAlchemy engine.
    - table_name (str): The name of the Snowflake table to write to.
    - schema (str): The name of the Snowflake schema to use.

    Returns:
    - tuple: A tuple containing 'success', 'num_chunks', and 'num_rows'.
    """
    if "biu_module.biuutilities.SecureSnowflakeConnection" in str(type(conn)):
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema,
            quote_identifiers=False,
        )
    else:
        print("Alchemy")
        df.to_sql(table_name, conn, schema=schema, if_exists="append", index=False)
        success, num_chunks, num_rows = True, 1, len(df)

    return success, num_chunks, num_rows


####################################################################################
# Pyspark methods
####################################################################################


def get_sfFormat():
    """
    Returns the Snowflake format string for Spark.

    Returns:
    - str: The Snowflake format string for Spark, which is "net.snowflake.spark.snowflake".
    """
    return "net.snowflake.spark.snowflake"


def write_sfSQL(spark, sfOptions, query):
    """
    Executes a Snowflake SQL query using Spark.

    Parameters:
    - spark (SparkSession): The SparkSession object.
    - sfOptions (dict): A dictionary containing Snowflake options.
    - query (str): The SQL query to be executed.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        sfOptions, query
    )


def enable_sfPushdown(sc, sfOptions):
    """
    Enables Snowflake query pushdown optimization in Spark.
    On by default.

    Parameters:
    - sc (SparkContext): The SparkContext object.
    - sfOptions (dict): A dictionary containing Snowflake options.

    Returns:
    - dict: The updated Snowflake options dictionary with "autopushdown" set to "on".
    """
    sc._jvm.net.snowflake


def disable_sfPushdown(sc, sfOptions):
    """
    Disables Snowflake query pushdown optimization in Spark.

    Parameters:
    - sc (SparkContext): The SparkContext object.
    - sfOptions (dict): A dictionary containing Snowflake options.

    Returns:
    - dict: The updated Snowflake options dictionary with "autopushdown" set to "off".
    """
    sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(
        sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
    )
    sfOptions["autopushdown"] = "off"
    return sfOptions


def read_spark_df_from_sf(
    spark,
    environment,
    query,
    query_tag,
    role,
    schema=None,
    truncate_table="off",
    keep_column_case="on",
):
    """
    Reads a Spark DataFrame from a Snowflake database using a SQL query.

    Parameters:
    - spark (SparkSession): The SparkSession object.
    - environment (str): The environment for which Snowflake options are required, such as 'uat' or 'prod'.
    - query (str): The SQL query to execute.
    - query_tag (str): A tag to associate with the query session.
    - truncate_table (str, optional): Truncate table option. Defaults to "off".
    - keep_column_case (str, optional): Keep column case option. Defaults to "on".

    Returns:
    - pyspark.sql.DataFrame: The Spark DataFrame loaded from Snowflake.
    """

    # Validate the query_tag
    is_valid, message = __validate_query_tag_string(query_tag)
    if not is_valid:
        raise ValueError(f"Invalid query_tag: {message}")

    if not query_tag:
        raise ValueError("Please provide the query_tag as a function parameter.")

    secret = __get_AWS_secret("spark", "prod")

    # Retrieve Snowflake options dynamically
    sfOptions = get_sfOptions(environment, role)

    schema_name = schema
    if schema_name is None:
        schema_name = secret.get("sfSchema")

    sfOptions["truncate_table"] = truncate_table
    sfOptions["keep_column_case"] = keep_column_case
    sfOptions["QUERY_TAG"] = query_tag

    df = (
        spark.read.format(get_sfFormat())
        .options(**sfOptions)
        .option("query", query)
        .load()
    )
    return df


def write_spark_df_to_sf(
    spark_df,
    environment,
    table_name,
    query_tag,
    mode,
    role,
    schema=None,
    truncate_table="off",
    keep_column_case="on",
):
    """
    Writes a Spark DataFrame to a Snowflake table.

    Parameters:
    - spark_df (pyspark.sql.DataFrame): The Spark DataFrame to write.
    - environment (str): The environment for which Snowflake options are required, such as 'uat' or 'prod'.
    - table_name (str): The Snowflake table name to write to.
    - query_tag (str): A tag to associate with the query session.
    - mode (str, optional): Writing mode for Spark DataFrame. Can be 'overwrite', 'append', or 'ignore'.
    - truncate_table (str, optional): Truncate table option. Defaults to "off".
    - keep_column_case (str, optional): Keep column case option. Defaults to "on".

    Returns:
    - None: This function performs an action and does not return anything.
    """

    # Validate the query_tag
    is_valid, message = __validate_query_tag_string(query_tag)
    if not is_valid:
        raise ValueError(f"Invalid query_tag: {message}")

    if not query_tag:
        raise ValueError("Please provide the query_tag as a function parameter.")

    secret = __get_AWS_secret("spark", "prod")

    sfOptions = get_sfOptions(environment, role)

    schema_name = schema
    if schema_name is None:
        schema_name = secret.get("sfSchema")

    sfOptions["truncate_table"] = truncate_table
    sfOptions["keep_column_case"] = keep_column_case
    sfOptions["QUERY_TAG"] = query_tag

    spark_df.write.mode(mode).format("net.snowflake.spark.snowflake").options(
        **sfOptions
    ).option("dbtable", table_name).save()


####################################################################################
# Alchemy methods
####################################################################################


def create_alchemy_engine(env, role, query_tag, schema_name=None):
    """
    Creates a SQLAlchemy engine for Snowflake.

    Parameters:
    - env (str): The environment (e.g., 'prod', 'dev').
    - role (str): The Snowflake role to use.
    - query_tag (dict): A dictionary to associate with the query session as a tag.
    - schema_name (str, optional): The schema name to use. Defaults to None.

    Returns:
    - CustomEngine: The custom SQLAlchemy engine.
    """

    # Validate the query_tag
    is_valid, message = __validate_query_tag_string(query_tag)
    if not is_valid:
        raise ValueError(f"Invalid query_tag: {message}")

    secret = __get_AWS_secret("alchemy", env.lower())

    if schema_name is None:
        schema_name = secret.get("sfSchema")

    alchemy_url = URL(
        account=secret.get("sfAccount"),
        user=secret.get("sfUser"),
        password=secret.get("sfPassword"),
        database=secret.get("sfDatabase"),
        warehouse=secret.get("sfWarehouse"),
        role=role,
    )

    engine = create_engine(
        alchemy_url,
        connect_args={"session_parameters": {"QUERY_TAG": query_tag}},
    )

    return engine


def execute_alchemy_query(engine, query):
    """
    Executes a SQL query using SQLAlchemy engine.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine.
    - query (str): The SQL query to execute.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    with engine.connect() as connection:
        connection.execute(query)


def fetch_alchemy_data(engine, query):
    """
    Fetches data from Snowflake using SQLAlchemy engine.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine.
    - query (str): The SQL query to execute.
    - limit (int): The number of rows to fetch.

    Returns:
    - pandas.DataFrame: The fetched data as a DataFrame.
    """

    connection = engine.connect()
    result = connection.execute(query).fetchall()
    connection.close()
    return pd.DataFrame(result)


def bulk_insert_alchemy(engine, table_name, schema_name, df, method=None, index=False):
    """
    Inserts data in bulk into a Snowflake table using SQLAlchemy engine.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine.
    - table_name (str): The name of the table to insert into.
    - data (list of dict): The data to insert.
    - method (str, optional): The method to insert data. Can be 'append', 'replace', or 'fail'. Has to be mentioned.
    - index (bool, optional): Whether to include index in the table. Defaults to False.

    Returns:
    - None: This function performs an action and does not return anything.
    """

    if method not in ["fail", "replace", "append"]:
        raise ValueError("Invalid method. Must be one of ['fail', 'replace', 'append']")

    df.to_sql(table_name, engine, if_exists=method, index=index, schema=schema_name)


def bulk_insert_with_engine(
    env, role, query_tag, table_name, schema_name, df, method=None, index=False
):
    """
    Wrapper function to create an SQLAlchemy engine, perform a bulk insert, and then dispose of the engine.

    Parameters:
    - env (str): The environment (e.g., 'prod', 'dev').
    - role (str): The Snowflake role to use.
    - query_tag (str): A string to associate with the query session as a tag.
    - table_name (str): The name of the table to insert into.
    - data (list of dict): The data to insert.
    - method (str, optional): The method to insert data. Can be 'append', 'replace', or 'fail'. Has to be mentioned.
    - index (bool, optional): Whether to include index in the table. Defaults to False.
    - schema_name (str, optional): The schema name to use. Defaults to None.
    """
    # Validate the query_tag
    is_valid, message = __validate_query_tag_string(query_tag)
    if not is_valid:
        raise ValueError(f"Invalid query_tag: {message}")

    # Create SQLAlchemy engine
    engine = create_alchemy_engine(env, role, query_tag, schema_name)

    try:
        bulk_insert_alchemy(engine, table_name, schema_name, df, method, index)
    finally:
        engine_dispose(engine)


def engine_dispose(engine):
    """
    Disposes of the SQLAlchemy engine, freeing up resources.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine.

    Returns:
    - None: This function performs an action and does not return anything.
    """
    engine.dispose()

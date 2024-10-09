import pyodbc
import pandas as pd

# Function to connect to Hive or Impala via ODBC on Windows using Kerberos (LDAP)
def create_odbc_connection(service, host, port, kerberos_service_name, ssl=True):
    """
    Create an ODBC connection to either Hive or Impala using Kerberos (LDAP) and SSL on Windows.

    Parameters:
    service (str): 'hive' or 'impala' to determine which service to connect to.
    host (str): The hostname or IP address of the Hive/Impala server.
    port (str): The port number where HiveServer2/Impala is listening.
    kerberos_service_name (str): The Kerberos service name for the respective service.
    ssl (bool): Enable SSL for the connection. Defaults to True.

    Returns:
    connection: pyodbc connection object.
    """
    driver = 'Cloudera ODBC Driver for Apache Hive' if service.lower() == 'hive' else 'Cloudera ODBC Driver for Apache Impala'
    ssl_option = '1' if ssl else '0'

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"HOST={host};"
        f"PORT={port};"
        f"AuthMech=1;"  # Kerberos authentication
        f"KrbHostFQDN={host};"
        f"KrbServiceName={kerberos_service_name};"
        f"KrbAuthType=2;"  # Windows Kerberos Auth (via LDAP/AD)
        f"SSL={ssl_option};"
        f"UID=;"  # Leave empty as Kerberos will handle user authentication
        f"PWD=;"  # Leave empty as Kerberos will handle password authentication
    )

    try:
        connection = pyodbc.connect(connection_string, autocommit=True)
        print(f"Successfully connected to {service.upper()}")
        return connection
    except Exception as e:
        print(f"Failed to connect to {service.upper()}: {e}")
        return None

# Function to run SQL queries and store results in a DataFrame
def run_sql_query_to_df(connection, sql_query):
    """
    Execute a SQL query against Hive or Impala and return the results in a Pandas DataFrame.

    Parameters:
    connection: The ODBC connection object.
    sql_query (str): The SQL query to be executed.

    Returns:
    df: Pandas DataFrame containing the query results.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        # Fetch the column names
        columns = [desc[0] for desc in cursor.description]
        # Fetch the data
        data = cursor.fetchall()
        # Convert the data to a Pandas DataFrame
        df = pd.DataFrame.from_records(data, columns=columns)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        cursor.close()

# Example usage
if __name__ == "__main__":
    # Parameters for Hive or Impala connection
    hive_or_impala = 'impala'  # Set 'hive' or 'impala'
    host = 'your.cloudera.host.com'
    port = '21050'  # Default Impala port is 21050, Hive typically uses 10000
    kerberos_service_name = 'impala' if hive_or_impala == 'impala' else 'hive'

    # Create connection
    conn = create_odbc_connection(hive_or_impala, host, port, kerberos_service_name)

    if conn:
        # Example SQL query
        query = "SELECT * FROM your_table LIMIT 10;"
        
        # Run the query and store results in a Pandas DataFrame
        df = run_sql_query_to_df(conn, query)
        
        # Print the DataFrame
        if not df.empty:
            print(df)

        # Close the connection
        conn.close()

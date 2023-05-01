def get_create_db_query(db_name: str, column_names: set) -> str:
    """
    Function to create a SQL create table query for given db_name and column_names.

    Args:
        db_name (str): name of the database
        column_names (set): set of column names to create table with

    Returns:
        str: SQL query to create a table
    """
    query = f"CREATE TABLE IF NOT EXISTS {db_name} (id INTEGER PRIMARY KEY"
    for column in column_names:
        if "_data" in column:
            query += f", {column} INTEGER"
        else:
            query += f", {column} TEXT"
    query += ");"
    return query


def get_drop_db_query(db_name: str) -> str:
    """
    Function to create a SQL drop table query for given db_name.

    Args:
        db_name (str): name of the database

    Returns:
        str: SQL query to drop a table
    """
    query = f"DROP TABLE IF EXISTS {db_name}"
    return query


def get_initial_query(db_name: str, column_names: str) -> str:
    """
    Function to create a SQL insert initial row query for given db_name and column_names.

    Args:
        db_name (str): name of the database
        column_names (str): comma separated string of column names

    Returns:
        str: SQL query to insert a row
    """
    values_len = ", ".join(["?"] * len(column_names))
    query = f"INSERT INTO {db_name} ({', '.join(column_names)}) VALUES ({values_len})"
    return query


def get_create_column_query(db_name: str) -> str:
    """
    Function to create a SQL query to add a date column to given db_name.

    Args:
        db_name (str): name of the database

    Returns:
        str: SQL query to add a column
    """
    query = f"ALTER TABLE {db_name} ADD COLUMN date DATE"
    return query


def get_add_date_query(db_name: str) -> str:
    """
    Function to create a SQL query to add date to rows that don't have a date.

    Args:
        db_name (str): name of the database

    Returns:
        str: SQL query to update rows with empty date
    """
    query = f"UPDATE {db_name} SET date = ? WHERE rowid = (SELECT rowid FROM {db_name} WHERE date IS NULL)"
    return query


def get_total_query(db_name: str) -> str:
    """
    Function to create a SQL query to get total calculated values from given db_name.

    Args:
        db_name (str): name of the database

    Returns:
        str; SQL query to calculate total values
    """
    query = f" \
            SELECT date, company, results, \
                SUM(fact_Qliq_data1 + fact_Qliq_data2) AS total_qliq, \
                SUM(fact_Qoil_data1 + fact_Qoil_data2) AS total_qoil \
            FROM (\
                SELECT date, company, 'fact' AS results, \
                    fact_Qliq_data1, fact_Qliq_data2, fact_Qoil_data1, fact_Qoil_data2 \
                FROM {db_name} \
                UNION ALL \
                SELECT date, company, 'forecast' AS results, \
                    forecast_Qliq_data1, forecast_Qliq_data2, forecast_Qoil_data1, forecast_Qoil_data2 \
                FROM {db_name} \
            ) \
            GROUP BY date, company, results"
    return query

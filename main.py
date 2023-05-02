from managers import (
    DatabaseConnector,
    DateFieldsGenerator,
    DatabasePrettyView,
    DatabaseQueryExecutor,
    FileReader,
)
from sql.queries import (
    get_add_date_query,
    get_create_column_query,
    get_create_db_query,
    get_drop_db_query,
    get_initial_query,
    get_total_query,
)

DB_NAME = "ursip"
TABLE_NAME = "table.xlsx"
SIMPLE_NAMES = ["company"]  # names for exclude


if __name__ == "__main__":
    # initialization
    db_conn = DatabaseConnector(DB_NAME)
    db_executor = DatabaseQueryExecutor(DB_NAME)
    file_reader = FileReader(TABLE_NAME)
    printer = DatabasePrettyView()
    fields_gen = DateFieldsGenerator()

    # read table in file
    file_reader.read_fields_values()
    file_reader.read_column_names()

    column_names = file_reader.get_all_col_names(SIMPLE_NAMES)
    data = file_reader.get_data()

    with db_conn as conn:
        # drop table if exists
        drop_query = get_drop_db_query(db_executor.db_name)
        db_executor.execute_query(conn, drop_query)

        # create db
        create_query = get_create_db_query(db_executor.db_name, column_names)
        db_executor.execute_query(conn, create_query)

        # create initial insert
        init_query = get_initial_query(db_executor.db_name, column_names)
        db_executor.execute_query_with_data(conn, init_query, data)

        # create date column
        create_date = get_create_column_query(db_executor.db_name)
        db_executor.execute_query(conn, create_date)

        # generate date fields
        size = len(data)
        dates = fields_gen.generate_fields(size)

        # add dates to db
        add_date_query = get_add_date_query(db_executor.db_name)
        db_executor.execute_query_with_data(conn, add_date_query, dates)

        # get total info
        get_total_query = get_total_query(db_executor.db_name)
        total_info = db_executor.execute_query(conn, get_total_query, result=True)

        # print results
        printer.pretty_print(total_info)

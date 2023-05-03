import random
import sqlite3 as db
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple

YEAR = 2023
MONTH = 5


class Database(ABC):
    """Abstract base class for database"""

    def __init__(self, db_name: str) -> None:
        """Initialize an instance of Database given a name

        Args:
            db_name (str): Name of the database
        """
        self.db_name = db_name


class DatabaseConnector(Database):
    """Connect to an sqlite database"""

    def __init__(self, db_name: str) -> None:
        """Initialize an instance of DatabaseConnector given a database name

        Args:
            db_name (str): Name of the database
        """
        super().__init__(db_name)
        self.connection = None

    def __enter__(self) -> db.Connection:
        """Connect to the sqlite database"""
        self.connection = db.connect(self.db_name)
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the connection to the sqlite database"""
        self.connection.close()


class DatabaseQueryExecutor(Database):
    """Execute database queries on an sqlite database"""
    def execute_query(
        self, conn: db.Connection, query: str, result: bool = None
    ) -> None | list:
        """Execute a query on a database and fetch all results

        Args:
            conn (db.Connection): Connection to the database
            query (str): SQL query to execute
            result (bool): Whether to return the fetched results or not

        Returns:
            list or None: A list of fetched results or None if result=False
        """
        try:
            cursor = conn.cursor()
            results = cursor.execute(query)
            conn.commit()

            if result:
                return results.fetchall()

        except db.Error:
            conn.rollback()

    def execute_query_with_data(
        self, conn: db.Connection, query: str, data: list
    ) -> None:
        """Execute a query with data on a database

        Args:
            conn (db.Connection): Connection to the database
            query (str): SQL query to execute
            data (list): Data to insert
        """
        try:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            conn.commit()
        except db.Error as e:
            conn.rollback()


class DatabaseFieldsGenerator(ABC):
    """Abstract base class for generating database fields"""

    @abstractmethod
    def generate_fields(self, size: int) -> list:
        """Generates fields for a database

        Args:
            size (int): The size of fields to generate

        Returns:
            list: A list of generated fields
        """
        pass


class DateFieldsGenerator(DatabaseFieldsGenerator):
    """Generate fields for a date table"""

    def generate_fields(self, size: int) -> list:
        """Generates fields for a date table

        Args:
            size (int): The size of fields to generate

        Returns:
            list: A list of generated fields
        """
        fields = [
            (str(datetime(YEAR, MONTH, random.randint(1, 31)).date()),)
            for _ in range(size)
        ]
        return fields


class DatabasePrettyView:
    """Pretty printing class for database results"""

    def pretty_print(self, data: List[Tuple]) -> None:
        """Prints database results in a pretty format

        Args:
            data (List[Tuple]): Database results to print
        """
        if not data:
            raise TypeError("Could not get data to output results")

        for result in data:
            date, company, results, total_qliq, total_qoil = result
            print(
                f"Date: {date} \
                Company: {company} \
                Results: {results} \
                Total Qliq: {total_qliq} \
                Total Qoil: {total_qoil}"
            )

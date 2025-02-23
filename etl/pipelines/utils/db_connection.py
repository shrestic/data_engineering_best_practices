import os
from contextlib import contextmanager
import psycopg2

class MetadataConnection:
    def __init__(self) -> None:
        self._host = os.getenv("METADATA_HOST")
        self._port = os.getenv("METADATA_PORT")
        self._dbname = os.getenv("METADATA_DATABASE")
        self._user = os.getenv("METADATA_USERNAME")
        self._password = os.getenv("METADATA_PASSWORD")

    @contextmanager
    def managed_cursor(self):
        """
        Provides a managed database cursor using a context manager.

        This method establishes a connection to the PostgreSQL database using the
        parameters specified in the environment variables (METADATA_HOST, METADATA_PORT, etc.),
        creates a cursor, and yields it to allow database operations within a 'with' block.

        Key Points:
        - Context Manager:
            The @contextmanager decorator simplifies resource management. It ensures that the 
            database connection and its cursor are properly initialized when entering the 
            'with' block and are cleaned up (committing transactions, closing cursor and connection)
            when exiting the block, even if an error occurs.
        - Yield Keyword:
            The 'yield' statement temporarily hands over control by providing the cursor to the caller.
            Once the code inside the 'with' block completes, execution resumes after the yield statement,
            where the cleanup code is executed.
        - Benefits:
            This pattern leads to cleaner code, automatic resource management, and robust error handling,
            reducing the risk of resource leaks or inconsistent database states.
        """
        _conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._dbname,
            user=self._user,
            password=self._password,
        )

        cur = _conn.cursor()
        try:
            # Yield the cursor to be used within the 'with' block.
            yield cur
        finally:
            # Ensure that any pending transactions are committed and that resources are cleaned up.
            _conn.commit()
            cur.close()
            _conn.close()

    def __str__(self):
        """
        Returns a string representation of the database connection in the format:
        postgresql://user:password@host:port/dbname
        """
        return f"postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._dbname}"

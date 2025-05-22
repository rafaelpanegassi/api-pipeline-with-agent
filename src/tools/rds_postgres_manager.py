import os

import psycopg2
from loguru import logger
from sqlalchemy import create_engine


class RDSPostgreSQLManager:
    def __init__(
        self,
        db_name=None,
        db_user=None,
        db_password=None,
        db_host=None,
        db_port="5432",
    ):
        env_vars_set = self.check_environment_variables()

        if (
            db_name is None
            and db_user is None
            and db_password is None
            and db_host is None
            and not env_vars_set
        ):
            logger.error(
                "Database credentials were not provided directly or via environment variables."
            )
            raise ValueError(
                "Database credentials were not provided directly or via environment variables."
            )

        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_port = db_port

        if not all(
            [
                self.db_name,
                self.db_user,
                self.db_password,
                self.db_host,
                self.db_port,
            ]
        ):
            missing_creds = [
                cred_name
                for cred_name, cred_val in {
                    "DB_NAME": self.db_name,
                    "DB_USER": self.db_user,
                    "DB_PASSWORD": self.db_password,
                    "DB_HOST": self.db_host,
                    "DB_PORT": self.db_port,
                }.items()
                if not cred_val
            ]
            msg = (
                "One or more database credentials are still missing after checking env vars: "
                f"{', '.join(missing_creds)}"
            )
            logger.error(msg)
            raise ValueError(msg)

        logger.info(
            f"RDSPostgreSQLManager initialized for DB: {self.db_name} on Host: {self.db_host}"
        )

    def connect(self):
        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            return connection
        except psycopg2.Error as e:
            logger.error(f"Error connecting to the PostgreSQL database (psycopg2): {e}")
            return None

    def execute_query(self, query, params=None):
        connection = self.connect()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    if cursor.description:
                        result = cursor.fetchall()
                    else:
                        result = []
                    connection.commit()
                return result
            except psycopg2.Error as e:
                logger.error(f"Error executing SQL query '{query[:50]}...': {e}")
                connection.rollback()
                return None
            finally:
                if connection:
                    connection.close()
        else:
            logger.warning("Could not establish database connection to execute query.")
            return None

    def execute_insert(self, query, values):
        connection = self.connect()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, values)
                    connection.commit()
                return True
            except psycopg2.Error as e:
                logger.error(f"Error executing SQL insert '{query[:50]}...': {e}")
                connection.rollback()
                return False
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred during insert execution '{query[:50]}...': {e}"
                )
                connection.rollback()
                return False
            finally:
                if connection:
                    connection.close()
        else:
            logger.warning(
                "Could not establish database connection to execute insert."
            )
            return False

    @staticmethod
    def check_environment_variables():
        required_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            return False
        else:
            return True

    def alchemy(self):
        db_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        logger.info(
            f"Creating SQLAlchemy engine for database: {self.db_name} on host: {self.db_host}"
        )
        engine = create_engine(db_url)
        return engine

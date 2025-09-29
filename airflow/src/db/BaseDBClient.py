# BaseClass For DB Client
# Each schema will habe it's own client

import os
import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert


def create_db_engine(
    user: str,
    password: str,
    host: str,
    database: str,
    port: int = 5432,
    echo: bool = False,
):

    encoded_password = urllib.parse.quote_plus(password)
    url = f"postgresql+psycopg2://{user}:{encoded_password}@{host}:{port}/{database}"
    try:
        return create_engine(url, echo=echo)
    except Exception as e:
        print(f"Failed to crete database engine. : {e.__class__} : {e}")
        raise e


class BaseDBClient:
    def __init__(self):
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = "postgres"  # container name as they are in same network
        db = os.getenv("POSTGRES_DB")
        self.engine = create_db_engine(user, password, host, db)
        self.sessionFactory = sessionmaker(bind=self.engine)

    def _get_session(self):
        return self.sessionFactory()

    def insert(self, obj):
        session = self._get_session()
        session.add(obj)
        session.commit()
        session.refresh(obj)
        _id = getattr(obj, "id", None)
        session.close()
        return _id

    def delete(self, obj):
        session = self._get_session()
        session.delete(obj)
        session.commit()
        session.close()

    def query(self, model):
        session = self._get_session()
        return QueryBuilder(session, model)

    def upsert(
        self,
        model,
        insert_values: dict,
        conflict_columns: list,
        update_values: dict = None,
    ):
        """
        Upsert a row into a PostgreSQL table.

        :param model: SQLAlchemy model class
        :param insert_values: dict of values to insert
        :param conflict_columns: list of columns to detect conflicts (usually primary key or unique constraint)
        :param update_values: dict of values to update if conflict occurs (defaults to insert_values)
        """
        if update_values is None:
            update_values = insert_values

        stmt = pg_insert(model).values(**insert_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_columns, set_=update_values
        )

        session = self._get_session()
        session.execute(stmt)
        session.commit()
        session.close()

    # def update(self, model, filters: dict, update_values: dict):
    #     """
    #     Update rows in a table.

    #     :param model: SQLAlchemy model class
    #     :param filters: dict mapping column names to values for WHERE clause
    #     :param update_values: dict mapping column names to new values
    #     """
    #     session = self._get_session()
    #     q = session.query(model)

    #     # Apply filters
    #     for col, val in filters.items():
    #         q = q.filter(getattr(model, col) == val)

    #     # Perform update
    #     q.update(update_values, synchronize_session="fetch")
    #     session.commit()
    #     session.close()


class QueryBuilder:
    def __init__(self, session, model):
        self.session = session
        self.query = session.query(model)

    def where(self, condition):
        """Add a filter condition (SQLAlchemy expression)."""
        self.query = self.query.filter(condition)
        return self

    def order_by(self, *columns):
        """Order by one or more columns."""
        self.query = self.query.order_by(*columns)
        return self

    def limit(self, n):
        """Limit rows."""
        self.query = self.query.limit(n)
        return self

    def all(self):
        """Fetch all results."""
        results = self.query.all()
        self.session.close()
        return results

    def first(self):
        """Fetch the first result."""
        result = self.query.first()
        self.session.close()
        return result

    def update(self, values: dict):
        """
        Perform an update on all rows that match the filters.
        :param values: dictionary of column names -> new values
        """
        self.query.update(values, synchronize_session="fetch")
        self.session.commit()
        return self  # allow chaining if needed

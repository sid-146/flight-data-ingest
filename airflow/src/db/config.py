from sqlalchemy.orm import declarative_base
# from sqlalchemy import MetaData

# BASE = declarative_base(metadata=MetaData(schema="flight_data"))
BASE = declarative_base()


__all__ = ["BASE"]

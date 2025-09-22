from sqlalchemy.orm import declarative_base


class BaseModel:
    def to_dict(self):
        data = {}
        for column in self.__table__.columns:
            data[column.name] = getattr(self, column.name)
        return data


# BASE = declarative_base(metadata=MetaData(schema="flight_data"))
BASE = declarative_base(cls=BaseModel)


__all__ = ["BASE"]

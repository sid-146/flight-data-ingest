# BaseClass For DB Client
# Each schema will habe it's own client

from sqlalchemy.orm import sessionmaker

class BaseDBClient:
    def __init__(self, engine):
        self.engine = engine
        self.Session = sessionmaker(bind=engine)

    def _get_session(self):
        return self.Session()

    def insert(self, obj):
        session = self._get_session()
        session.add(obj)
        session.commit()
        session.close()

    def delete(self, obj):
        session = self._get_session()
        session.delete(obj)
        session.commit()
        session.close()

    def query(self, model, **filters):
        session = self._get_session()
        q = session.query(model)
        for col, val in filters.items():
            q = q.filter(getattr(model, col) == val)
        result = q.all()
        session.close()
        return result

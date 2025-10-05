from src.db.BaseDBClient import BaseDBClient
from src.db.models.monitoring_models import ProcessRunLog


class MonitoringClient(BaseDBClient):
    def __init__(self):
        self._schema = "monitoring"
        super().__init__()

    def update_process_log(values: dict):
        raise NotImplementedError("Yet to implement.")

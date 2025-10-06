from src.db.monitoring_db_client import MonitoringClient

from src.processor.jobs.airlines_job import start_job


"""
Todo:
    decide the flow; How to identify which job to trigger using File Ingestion Table;

    # First Think of only how to process airlines (Think for multiple files)

"""


def process_airlines_task(**context):
    # Todo: This function must perform following task.
    # Insert process log record

    try:
        pass
    except Exception as e:
        pass

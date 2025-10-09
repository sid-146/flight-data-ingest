from src.core.logger import console

from src.processor.utils import get_spark_session


def main():
    spark = get_spark_session("FirstAppForTesting")
    console.info("Spark session created successfully.")

    states = {
        "MH": "Maharastra",
        "MP": "Madhya Pradesh",
        "RJ": "Rajasthan",
        "GJ": "Gujurat",
    }

    broadcastStates = spark.sparkContext.broadcast(states)

    data = [
        ("James", "Smith", "India", "MH"),
        ("Alex", "Rose", "India", "MP"),
        ("Maria", "Williams", "India", "RJ"),
        ("Robert", "Jones", "India", "GJ"),
        ("RL", "Alonso", "India", "MP"),
        ("Max", "Verstapan", "India", "MH"),
    ]

    spark.stop()

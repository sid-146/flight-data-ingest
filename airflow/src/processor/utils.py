from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Initialize Spark Session with config
    """

    spark = (
        SparkSession.builder.appName(app_name)
        # Use the container hostname when in same network
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "1G")
        .config("spark.driver.memory", "1G")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    )

    return spark

import argparse
import gzip
import json
import boto3
import io

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit


def 
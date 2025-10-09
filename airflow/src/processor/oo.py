from pyspark.sql import SparkSession


# Code to connect to spark session
spark: SparkSession = (
    SparkSession.builder.appName("ExampleApp")
    .master("spark://spark-master:7077")  # Use the container hostname
    .config("spark.executor.memory", "1G")
    .config("spark.driver.memory", "1G")
    .getOrCreate()
)


spark.sparkContext


# returns appname

spark.sparkContext.appName


spark.sparkContext.applicationId


spark.sparkContext.version


spark.sparkContext.uiWebUrl

accum = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize(list(range(1, 10)))
print(rdd.collect())
print()
rdd.foreach(lambda x: accum.add(x))
print(accum.value)


accumSquare = spark.sparkContext.accumulator(0)  # initialize with 0


def squared_sum(x):
    global accumSquare
    accumSquare += x**2


rdd.foreach(squared_sum)

print(accumSquare.value)


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

rdd = spark.sparkContext.parallelize(data)


def convert_state(code):
    return broadcastStates.value[code]


result = rdd.map(
    lambda x: (x[0], x[1], x[2], x[3], convert_state(x[3]))
).collect()  # collect returns a list containing all elements of rdd.


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

columns = [
    "firstname",
    "lastname",
    "country",
    "stateCode",
]

df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show()

columns = [
    "firstname",
    "lastname",
    "country",
    "state",
]


def convert_state(code):
    return broadcastStates.value[code]


result = rdd.map(
    lambda x: (x[0], x[1], x[2], convert_state(x[3]))
).collect()  # collect returns a list containing all elements of rdd.

columns.append("Statename")
result = df.rdd.map(lambda x: (x[0], x[1], x[2], x[3], convert_state(x[3]))).toDF(
    columns
)

result.show()


# Using broadcast to filter data.

data = [
    ("James", "Smith", "India", "MH"),
    ("Alex", "Rose", "India", "MP"),
    ("Maria", "Williams", "India", "RJ"),
    ("Robert", "Jones", "India", "GJ"),
    ("RL", "Alonso", "India", "MP"),
    ("Max", "Verstapan", "India", "MH"),
    ("Nick", "Fury", "India", "BH"),
    ("Lewis", "Hamilton", "India", "KL"),
]


columns = [
    "firstname",
    "lastname",
    "country",
    "state",
]

df = spark.createDataFrame(data, columns)
df.show()


filteredDf = df.where((df["state"].isin(list(broadcastStates.value.keys()))))
filteredDf.show()

from pyspark.sql import Row, SparkSession


spark = SparkSession \
    .builder \
    .appName("SparkSession-Delta") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()
    
data = [
    Row(id=1, name="Alice", age=29, salary=50000),
    Row(id=2, name="Bob", age=35, salary=60000),
    Row(id=3, name="Charlie", age=40, salary=70000),
    Row(id=4, name="David", age=45, salary=None)  # Null value example
]

df = spark.createDataFrame(data)
df.show()
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pandas as pd

CATALOG_URL = "http://localhost:3000"
DEMO_WAREHOUSE = "/Users/temp/internal-iceberg-catalog/wh"
SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.5.2"



config = {
    "spark.sql.catalog.example-catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.example-catalog.type": "rest",
    "spark.sql.catalog.example-catalog.uri": CATALOG_URL,
    "spark.sql.catalog.example-catalog.warehouse": DEMO_WAREHOUSE,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "example-catalog",
    "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
}

spark_config = SparkConf().setMaster('local').setAppName("pyspark-example")
for k, v in config.items():
    spark_config = spark_config.set(k, v)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
spark.sql("SHOW NAMESPACES").toPandas()
spark.sql("CREATE NAMESPACE westeros")

data = pd.DataFrame([[1, 'jon_snow', 0.0]], columns=['id', 'name', 'what_they_know'])
spark_df = spark.createDataFrame(data)
spark_df.writeTo("westeros.my_table").createOrReplace()

df = spark.sql("select * from westeros.my_table").toPandas()
print(df)







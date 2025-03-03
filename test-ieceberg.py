from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Iceberg-Sample") \
    .config("spark.jars", "iceberg-spark-runtime-3.5_2.12-1.8.0.jar") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "file:///mnt/d/spark-warehouse") \
    .getOrCreate()
# spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog")
# spark.sql(""" CREATE TABLE my_catalog.db.emp_table ( id INT, data STRING ) USING iceberg """)
spark.sql("""INSERT INTO my_catalog.db.emp_table VALUES (1, 'Alice'), (2, 'Bob');""")
df = spark.sql(""" SELECT * FROM my_catalog.db.emp_table;""")
df.show()
# spark.sql(""" INSERT INTO my_catalog.db.emp_table VALUES (14, 'james'),(15,'john')""")
# spark.sql("SHOW TABLES IN my_catalog").show()
# spark.sql("SHOW FUNCTIONS LIKE 'iceberg*'").show()

# spark.sql("SHOW CATALOGS").show()
from pyspark.sql.functions import col, round
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

# STEP 1 - Mount Azure Data Lake Storage Gen2 to Databricks
configs = {
    # Configuration settings for mounting Azure Data Lake Storage Gen2
    # ... (same as provided)
}

dbutils.fs.mount(
    source="abfss://tokyodatasources@tokyoplympicsdata.dfs.core.windows.net",
    mount_point="/mnt/tokyoolympic",
    extra_configs=configs
)

# Check the mounted data
# %fs ls "/mnt/tokyoolympic"  # This is Databricks specific, won't work in VSCode

# STEP 2 - Spark Session - Databricks
#spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
spark

# STEP 3 - Load Data
athletes = spark.read.csv("/mnt/tokyoolympic/raw-data/athletes.csv", header=True)
coaches = spark.read.csv("/mnt/tokyoolympic/raw-data/coaches.csv", header=True)
medals = spark.read.csv("/mnt/tokyoolympic/raw-data/medals.csv", header=True, inferSchema=True)
teams = spark.read.csv("/mnt/tokyoolympic/raw-data/teams.csv", header=True)
entriesgender = spark.read.csv("/mnt/tokyoolympic/raw-data/entriesgender.csv", header=True)

# STEP 4 - Show & Print Schema
athletes.show()
"""
Expected Output:
+-------+-------------+-----+
|  Name |     Country | Age |
+-------+-------------+-----+
| Alice | USA         |  25 |
| Bob   | Japan       |  28 |
| ...   | ...         | ... |
+-------+-------------+-----+
"""

athletes.printSchema()
"""
Expected Schema:
root
 |-- Name: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Age: integer (nullable = true)
"""

# STEP 5 - Correcting the Schema for entriesgender
entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
                             .withColumn("Male", col("Male").cast(IntegerType()))\
                             .withColumn("Total", col("Total").cast(IntegerType()))

entriesgender.printSchema()
"""
Expected Schema:
root
 |-- Discipline: string (nullable = true)
 |-- Female: integer (nullable = true)
 |-- Male: integer (nullable = true)
 |-- Total: integer (nullable = true)
"""

# STEP 6 - Data Analysis
top_countries_medal = medals.orderBy("Gold", ascending=False).select("Team_Country", "Gold", "Rank by Total")
top_countries_medal.show()
"""
Expected Output:
+--------------------+----+-------------+
|        Team_Country|Gold|Rank by Total|
+--------------------+----+-------------+
|United States of ...|  39|            1|
|People's Republic...|  38|            2|
| ...                | ...|          ...|
+--------------------+----+-------------+
"""

average_entries_by_gender = entriesgender.withColumn('Avg_Female', round((entriesgender['Female']/entriesgender['Total']), 2))\
                                         .withColumn('Avg_Male', round((entriesgender['Male']/entriesgender['Total']), 2))
average_entries_by_gender.show()
"""
Expected Output:
+--------------------+------+----+-----+----------+--------+
|          Discipline|Female|Male|Total|Avg_Female|Avg_Male|
+--------------------+------+----+-----+----------+--------+
|      3x3 Basketball|    32|  32|   64|      0.50|    0.50|
|             Archery|    64|  64|  128|      0.50|    0.50|
| ...                | ...  | ...|  ...|      ... |    ... |
+--------------------+------+----+-----+----------+--------+
"""

# Export data
average_entries_by_gender.write.mode('overwrite').csv('/mnt/tokyoolympic/transformed-data/EntriesGenderDT')
# Start by specifying the DataFrame that you want to write out.
average_entries_by_gender.write \

    # Use the 'mode' method to specify how the write operation should behave if data already exists 
    # at the specified location. In this case, 'overwrite' mode means any existing data will be replaced.
    .mode('overwrite') \

    # Finally, specify the format ('csv') and location where the data should be written.
    # The data will be written to the path '/mnt/tokyoolympic/transformed-data/EntriesGenderDT' in CSV format.
    .csv('/mnt/tokyoolympic/transformed-data/EntriesGenderDT')

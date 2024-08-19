# Import
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import collect_set
# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
    .getOrCreate()

# Prepare data
data = [('James','Java'),
        ('James','Python'),
        ('James','Python'),
        ('Anna','PHP'),
        ('Anna','Javascript'),
        ('Maria','Java'),
        ('Maria','C++'),
        ('James','Scala'),
        ('Anna','PHP'),
        ('Anna','HTML')
        ]

# Create DataFrame
df = spark.createDataFrame(data,schema=["name","languages"])
df.printSchema()
df.show()

df2 = df.groupBy("name").agg(collect_list("languages") \
    .alias("languages"))
df2.printSchema()    
df2.show(truncate=False)

# Using collect_set()

df3 = df.groupBy("name").agg(collect_set("languages") \
    .alias("languages"))
df3.printSchema()    
df3.show(truncate=False)


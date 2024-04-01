# Databricks notebook source
# MAGIC %md
# MAGIC col and df.column_name can be used interchangeably in most cases. However, here are some situations where one of them might be preferred over the other:
# MAGIC
# MAGIC Use col:
# MAGIC
# MAGIC When the column name is dynamic and constructed at runtime (i.e., you don't know the name of the column in advance).
# MAGIC When you need to use a function on a specific column. For instance, col("age") + 1 adds 1 to the age column.
# MAGIC
# MAGIC Use df.column_name:
# MAGIC
# MAGIC When you need to strongly reference a column in a dataframe. If you reference a column using dot notation, Spark checks the column name at compile time. Therefore, if there's a typo in the column name, you'll get an error right away.
# MAGIC When you need to filter or select columns. For example, df.select("age", "name") selects the age and name columns from the dataframe.
# MAGIC In general, you can choose whichever option you feel more comfortable with or find more readable.

# COMMAND ----------

print('test')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

StructType([StructField(col, datatype, null) ,       ])-----------999

# COMMAND ----------


d1 = [ (243 , 'siva' , 4000)   , (244 , 'sankar', 500000)  ]
s1 = StructType( [ StructField('eno' , IntegerType(), True) , StructField("ename" , StringType(), True), StructField("esale" , IntegerType(), True) ] )

df_1 = spark.createDataFrame(d1 ,s1)

# COMMAND ----------

df_1.display()

# COMMAND ----------

df.dtypes

# COMMAND ----------



# create a DataFrame with sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", '35')]
df = spark.createDataFrame(data, ["name", "age"])

# show the initial DataFrame
df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(df.name , df.age).display()

# COMMAND ----------

df.select(df["name"] , df["age"]).display()

# COMMAND ----------

df.select(df["name"] , df["age"]).display()

# COMMAND ----------

df.select(col('name')  , col('age')).display()

# COMMAND ----------

df.select(df.name ,  df.age).display()

# COMMAND ----------

df.select("name" , "age").display()

# COMMAND ----------

df1= df.select("name" , col("name") , df.name)

# COMMAND ----------

df1.show()

# COMMAND ----------

df.select("name").show()

# COMMAND ----------

df.select(col('name').alias('testing')).display()

# COMMAND ----------

df.select(df.name.alias('testing')).display()

# COMMAND ----------

df2 = df.select(df.name.alias("full_name"))

# COMMAND ----------

df2.show()

# COMMAND ----------

df.columns

# COMMAND ----------

#df.select(df.name).show()
df.select(col("name").alias("full_name")).show()


# COMMAND ----------

var = col("name").alias("full_name")

df.select(var).show()

df.select(col("name").alias("full_name")).show()



# COMMAND ----------

# use alias() to rename a Column
col_name = col("name").alias("full_name")

# show the result of alias()
df.select(col_name).show()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.select(col('age').alias('siva')   , col('age').alias('test')).display()

# COMMAND ----------

df_2.dtypes

# COMMAND ----------

df.dtypes

# COMMAND ----------

df_2 = df.select(col("age").cast('int'))

# COMMAND ----------

# use cast() to convert a Column to a different data type

# show the result of cast()
df3 = df.select(    col("age").alias('age_original'),      col("age").cast("double"))
df3.dtypes

# COMMAND ----------

# use desc() to sort a Column in descending order
c1 = col("age").desc()

df.sort(col("age").desc())

# show the result of desc()
df.sort(c1).show()



# COMMAND ----------

df.sort(df.age.desc()).display()

# COMMAND ----------

# use asc() to sort a Column in ascending order (default)
col_age_asc = col("age").asc()

# show the result of asc()
df.sort(col_age_asc).show()

# COMMAND ----------

df.filter(col('age') == 30).display()

# COMMAND ----------

df.filter(col('age').isin(30,35)).display()

# COMMAND ----------

df.filter(col("age").isNotNull()  ).display()

# COMMAND ----------

# use isNull() to filter a Column based on null values


# show the result of isNull()
df.filter(col("age").isNull()).show()



# COMMAND ----------

# use isNotNull() to filter a Column based on non-null values
 

# show the result of isNotNull()
df.filter(col("age").isNotNull()).show()

# COMMAND ----------

# use between() to filter a Column based on a range of values
col_age_between = col("age").between(20, 30)

# show the result of between()
df.filter(col_age_between).show()

# COMMAND ----------


# use like() to filter a Column based on a pattern


# show the result of like()
df.filter(col("name").like("A%")).show()

# COMMAND ----------


# use rlike() to filter a Column based on a regular expression
col_name_rlike = col("name").rlike("^A")

# show the result of rlike()
df.filter(col_name_rlike).show()

# COMMAND ----------

# use startswith() to filter a Column based on a prefix
col_name_startswith = col("name").startswith("A")

# show the result of startswith()
df.filter(col("name").startswith("A")).show()

# COMMAND ----------

# use endswith() to filter a Column based on a suffix
col_name_endswith = col("name").endswith("e")

# show the result of endswith()
df.filter(col_name_endswith).show()

# COMMAND ----------

# use substr() to extract a substring from a Column
col_name_substr = col("name").substr(1, 2).alias('subs')

# show the result of substr()
df.select(col("name"), col_name_substr).show()

# COMMAND ----------

# use substr() to extract a substring from a Column
col_name_substr = col("name").substr(1, 2)

# show the result of substr()
df.select(col_name, col_name_substr).show()

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

select (case when eno =20 then 'yes' else 'No' end) 

# COMMAND ----------

lit('youg') , lit(2)

# COMMAND ----------

when( condition , value               ).when().otherwise()

when(col('age') < 30 ,  lit('young')).when(col('age').between(31,50) , lit('MiddleAge')).otherwise("old").alias('Age_group')

# COMMAND ----------


# use when() and otherwise() to conditionally compute a Column
col_age_category = (
    
)

# show the result of when() and otherwise()
df.select(col_name, col("age"),      when(col("age") < 30, lit("Young")).when(col("age").between(30 , 50) , lit("middle")).otherwise("Old").alias("age_category")).show()

# COMMAND ----------



# use when() and otherwise() to conditionally compute a Column
col_age_category = (
    when(col("age") < 30, "Young")
    .otherwise("Old")
    .alias("age_category")
)

# show the result of when() and otherwise()
df.select(col_name, col("age"), col_age_category).show()



# COMMAND ----------

from pyspark.sql.functions import *

# create a DataFrame with sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# show the initial DataFrame
df.show()

df.select(col("name").alias("full_name")).show()

# Databricks notebook source
#import
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan,when,count

# COMMAND ----------

# setting up the Enviroment

from pyspark.sql import SparkSession

spak=SparkSession.builder.appName("Flipkart data engineering ").getOrCreate()


# COMMAND ----------

#load the csv Data
file_path='/FileStore/tables/flipkart_com_ecommerce_sample.csv'

flipkart_df=spark.read.csv(file_path,header=True,inferSchema=True)
flipkart_df.show(5)


# COMMAND ----------


file_path='/FileStore/tables/flipkart_com_ecommerce_sample.csv'

flipkart_df=spark.read.csv(file_path,header=True,inferSchema=True)
flipkart_df.display()

# COMMAND ----------

#checking the schema

flipkart_df.printSchema()

# COMMAND ----------


flipkart_df.printSchema()
flipkart_df.describe().show()

# COMMAND ----------

#handling the missing data

flipkart_df.select([count(when(col(c).isNull(),c)).alias(c) for c in flipkart_df.columns]).display()

#drop the rows missing 
flipkart_df_clean=flipkart_df.dropna()
#filling specific values tothe nan column or missing columns
flipkart_df_filled=flipkart_df.fillna({"retail_price":0})

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize SparkSession
spark = SparkSession.builder.appName("Flipkart Data Transformation").getOrCreate()

# Load the data
file_path = "/FileStore/tables/flipkart_com_ecommerce_sample.csv"
flipkart_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Cast 'retail_price' and 'discounted_price' to numeric types
flipkart_df = flipkart_df.withColumn("retail_price", col("retail_price").cast("double")) \
                         .withColumn("discounted_price", col("discounted_price").cast("double"))

# Add a new column for recalculated retail price based on a discount (if needed)
# Assuming the discount percentage needs to be calculated:
flipkart_df_transformed = flipkart_df.withColumn(
    "calculated_discounted_price",
    expr("retail_price - (retail_price * (retail_price - discounted_price) / retail_price)")
)

# Show updated values
flipkart_df_transformed.select("product_name", "retail_price", "discounted_price", "calculated_discounted_price").display()



# COMMAND ----------

# group by the category and calculate the average  rating

from pyspark.sql.functions import col, avg, regexp_extract

# Extract main category from `product_category_tree` (assuming it's the first category in the tree)
flipkart_df_filled = flipkart_df.withColumn(
    "maincateg",
    regexp_extract(col("product_category_tree"), r"'([^']+)'", 1)
)

# Cast `overall_rating` to a numeric type for aggregation
flipkart_df_filled = flipkart_df_filled.withColumn("overall_rating", col("overall_rating").cast("double"))

# Calculate average rating by category
avg_rating_by_category = flipkart_df_filled.groupBy("maincateg").agg(avg("overall_rating").alias("avg_rating"))

# Show the result
avg_rating_by_category.show()


# COMMAND ----------

from pyspark.sql.functions import col, avg, regexp_extract

# Extract main category from `product_category_tree` (assuming it's the first category in the tree)
flipkart_df_filled = flipkart_df.withColumn(
    "maincateg",
    regexp_extract(col("product_category_tree"), r"'([^']+)'", 1)
)

# Cast `overall_rating` to a numeric type for aggregation
flipkart_df_filled = flipkart_df_filled.withColumn("overall_rating", col("overall_rating").cast("double"))

# Remove rows with null values in required columns (maincateg and overall_rating)
flipkart_df_cleaned = flipkart_df_filled.dropna(subset=["maincateg", "overall_rating"])

# Calculate average rating by category
avg_rating_by_category = flipkart_df_cleaned.groupBy("maincateg").agg(avg("overall_rating").alias("avg_rating"))

# Show the result
avg_rating_by_category.show()


# COMMAND ----------

from pyspark.sql.functions import col, sum, regexp_extract

# Extract main category from `product_category_tree`
flipkart_df_filled = flipkart_df.withColumn(
    "maincateg",
    regexp_extract(col("product_category_tree"), r"'([^']+)'", 1)
)

# Cast `discounted_price` and `retail_price` to numeric types
flipkart_df_filled = flipkart_df_filled.withColumn("discounted_price", col("discounted_price").cast("double"))
flipkart_df_filled = flipkart_df_filled.withColumn("retail_price", col("retail_price").cast("double"))

# Remove rows with null values in required columns
flipkart_df_cleaned = flipkart_df_filled.dropna(subset=["maincateg", "discounted_price"])

# Calculate total revenue by category
total_revenue_by_category = flipkart_df_cleaned.groupBy("maincateg").agg(sum("discounted_price").alias("total_revenue"))

# Show the result
total_revenue_by_category.show()


# COMMAND ----------

# save the processed data

output_table='Flipkart_Data_Analysis_table'
flipkart_df_filled.write.mode("overwrite").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from flipkart_data_analysis_table limit 20

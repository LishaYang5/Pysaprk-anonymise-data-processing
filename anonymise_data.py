'''
Author: your name
Date: 2021-09-27 03:29:54
LastEditTime: 2021-09-27 03:39:52
LastEditors: Please set LastEditors
Description: Anonymise first_name, last_name and address in the csv file.
coding: utf-8
FilePath: In databrickes' table.
'''
# File location and type
file_location = "/FileStore/tables/au_500.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location)

# Valid dataframe, drop missing data
df.na.drop()

# Rename columns
df = df.withColumnRenamed("_c0","first_name")
df = df.withColumnRenamed("_c1","last_name")
df = df.withColumnRenamed("_c2","date_of_birth")
df = df.withColumnRenamed("_c3","address")
df = df.withColumnRenamed("_c4","city")
df = df.withColumnRenamed("_c5","state")
df = df.withColumnRenamed("_c6","post")
df = df.withColumnRenamed("_c7","phone1")
df = df.withColumnRenamed("_c8","phone2")
df = df.withColumnRenamed("_c9","email")
df = df.withColumnRenamed("_c10","web")

# generate fake firstname, lastname, and address columns
import pyspark.sql.functions as sf
from faker import Faker
from pyspark.sql import functions as F

fake = Faker()

fake_firstname = F.udf(fake.first_name)
df_fakeFirstName = df.withColumn("fake_FirstName", fake_firstname())
fake_lastname = F.udf(fake.last_name)
df_fakenames = df_fakeFirstName.withColumn("fake_LastName",fake_lastname())
fake_address =F.udf(fake.address)
fake_info = df_fakenames.withColumn("fake_address",fake_address())

# generate anonymised file with fake firstname, fake lastname and address
anonymised_data = fake_info.select("fake_FirstName","fake_LastName","fake_address","date_of_birth")

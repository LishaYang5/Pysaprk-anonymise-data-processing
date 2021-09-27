# Anonymoise data process

It is used to process the csvfile to anonymise first_name, last_name and address by using spark.

## Assumptions

1. Values in each column cannot be null.
2. Values in each column cannot be meaningless after anonymised.
3. Raw data values cannot be changed.

## Source data

It is downloaded from Free Sample Data (https://www.briandunning.com/sample-data/)

```
root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- date_of_birth: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- post: string (nullable = true)
 |-- phone1: string (nullable = true)
 |-- phone2: string (nullable = true)
 |-- email: string (nullable = true)
 |-- web: string (nullable = true)
```


## Data process 

### Required dataframe schema

Use Spark dataframe to generate required dataframe, which schema is showing below.

```
root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- date_of_birth: string (nullable = true)
 |-- address: string (nullable = true)
```



### Target dataframe schema

```
root
 |-- fake_FirstName: string (nullable = true)
 |-- fake_LastName: string (nullable = true)
 |-- fake_address: string (nullable = true)
 |-- date_of_birth: string (nullable = true)
```

### Anonymised data overview

```
+----------+--------------+----------+-------------+--------------------+--------------------+
|first_name|fake_FirstName| last_name|fake_LastName|             address|        fake_address|
+----------+--------------+----------+-------------+--------------------+--------------------+
|first_name|          Cody| last_name|        Davis|             address|9405 Jennifer Mou...|
|  Rebbecca|      Courtney|     Didio|       Fuller|              1/1/80|41224 Nichols Cur...|
|    Stevie|        Steven|     Hallo|       Murray|      22222 Acoma St|63853 Wallace Mea...|
|    Mariko|        Autumn|    Stayer|      Andrews|534 Schoenborn St...|91242 Micheal Cen...|
|   Gerardo|       Anthony|    Woodka|      Johnson|   69206 Jackson Ave|35230 Moore Inlet...|
```



### Version

Spark:3.1.1

### Work Environment
Databricks community

Notebook Link: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2037001060564868/4467286779690775/7714366041074664/latest.html
import json
import requests
import pyspark
from pyspark.sql.functions import col, udf, lit, current_timestamp, concat_ws, trim, datediff, to_timestamp, abs as _abs, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, DateType, TimestampType, IntegerType


url = 'https://randomuser.me/api/?results=5000&nat=br'
challenge_json = json.loads(requests.get(url).text)['results']


schema = StructType(
    [
        StructField('gender', StringType()),
        StructField('name', MapType(StringType(), StringType())),
        StructField('location', StructType(
            [
                StructField('street', StructType(
                    [
                        StructField('number', IntegerType()),
                        StructField('name', StringType())
                    ])),
                StructField('city', StringType()),
                StructField('state', StringType()),
                StructField('country', StringType()),
                StructField('postcode', IntegerType()),
                StructField('coordinates', StructType(
                    [
                        StructField('latitude', StringType()),
                        StructField('longitude', StringType())
                    ])),
                StructField('timezone', StringType()),
            ])),
        StructField('email', StringType()),
        StructField('dob', MapType(StringType(), StringType())),
        StructField('registered', MapType(StringType(), StringType())),
        StructField('phone', StringType()),
        StructField('cell', StringType()),
        StructField('nat', StringType()),
        
    ])


df_challenge = spark.createDataFrame(challenge_json, schema)

df_challenge = df_challenge.withColumn('source_url', lit(url))
df_challenge = df_challenge.withColumn('created_at', current_timestamp())
df_challenge = df_challenge.withColumn('first_name', df_challenge.name.getItem('first'))\
    .withColumn('last_name', df_challenge.name.getItem('last'))

df_challenge = df_challenge.withColumn('full_name', concat_ws(' ', df_challenge.first_name, df_challenge.last_name))
df_challenge = df_challenge.withColumn('street', df_challenge.location.getItem('street'))

df_challenge = df_challenge.withColumn('street_name', df_challenge.street.getItem('name'))\
    .withColumn('street_number', df_challenge.street.getItem('number'))

df_challenge = df_challenge.withColumn('address', concat_ws(', ', trim(df_challenge.street_name), df_challenge.street_number))

df_challenge = df_challenge.withColumn('city', df_challenge.location.getItem('city'))\
    .withColumn('state', df_challenge.location.getItem('state'))\
    .withColumn('country', df_challenge.location.getItem('country'))

df_challenge = df_challenge.withColumn('city_state_country', concat_ws(' - ', df_challenge.city, df_challenge.state, df_challenge.country))
df_challenge = df_challenge.withColumn('complete_address', concat_ws(' - ', df_challenge.address, df_challenge.city_state_country))

df_challenge = df_challenge.withColumn('dob_date', df_challenge.dob.getItem('date'))\
    .withColumn('registered_date', df_challenge.registered.getItem('date'))

df_challenge = df_challenge.withColumn("dob_date", to_timestamp("dob_date"))\
    .withColumn("registered_date", to_timestamp("registered_date"))

df_challenge = df_challenge.withColumn('current_timestamp', current_timestamp())
df_challenge = df_challenge.withColumn("diff_in_years", datediff(col("dob_date"), col("current_timestamp"))/365.25)
df_challenge = df_challenge.withColumn("age", df_challenge.diff_in_years.cast(IntegerType()))
df_challenge = df_challenge.withColumn('age', _abs(col('age')))

df_challenge = df_challenge.withColumn('age_group', when((col('age') >= 12) & (col('age') <= 18), 'adolescent')\
                        .when((col('age') >= 0) & (col('age') < 12), 'child')\
                        .when((col('age') > 18), 'adult'))

df_challenge = df_challenge.withColumn('purpose_of_use', when(col('age_group') == 'adult', 'marketing')\
                        .when((col('age_group') == 'child') | (col('age_group') == 'adolescent'), 'risks'))

df_challenge = df_challenge.select('full_name', 'gender', 'age', 'email', 'phone', 'cell', 'dob_date', 'registered_date', 'complete_address', 'age_group', 'purpose_of_use', 'source_url')
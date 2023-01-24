import json
import requests
import pyspark
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, DateType, TimestampType, IntegerType
from datetime import date


challenge_json = json.loads(requests.get('https://randomuser.me/api/?results=5000&nat=br').text)['results']


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


@udf(returnType=StringType())
def ajust_name(x):
    return x['first'] + " " + x['last']


@udf(returnType=StringType())
def get_adress(x):
    street_name = x['street']['name'].strip()
    street_number = str(x['street']['number'])
    return street_name + ", " + street_number + " - " + str(x['postcode']) +  " - " + x['city'] + " - " + x['state'] + " - " + x['country']


@udf(returnType=StringType())
def get_date(x):
    return x['date'][:10]


@udf(returnType=IntegerType())
def get_age(x):
    today = date.today()
    return int(today.strftime("%Y")) - int(x[:4])


@udf(returnType=StringType())
def get_category(x):
    if x > 0 and x < 12:
        return 'child'
    elif x > 12 and x <= 18:
        return 'adolescent'
    elif x > 18:
        return 'adult'


@udf(returnType=StringType())
def purpose_of_use(x):
    if x == 'adult':
        return 'marketing'
    if x == 'adult' or 'adolescent':
        return 'risk'


df_challenge = df_challenge.withColumn('name', ajust_name(col('name')))
df_challenge = df_challenge.withColumn('complete_address', get_adress(col('location')))
df_challenge = df_challenge.withColumn('dob', get_date(col('dob')))
df_challenge = df_challenge.withColumn('registered', get_date(col('registered')))\
                                      .select('name', 'gender', 'email', 'dob', 'registered', 'phone', 'cell', 'complete_address')

df_challenge = df_challenge.withColumn('years_old', get_age(col('dob')))
df_challenge = df_challenge.withColumn('age_group', get_category(col('years_old')))
df_challenge = df_challenge.withColumn('purpose_of_use', purpose_of_use(col('age_group')))
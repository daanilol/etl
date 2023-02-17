from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType

spark = SparkSession.builder \
      .master("local[*]") \
      .appName("challenge_spark") \
      .getOrCreate()

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

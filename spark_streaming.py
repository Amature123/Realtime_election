from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':
    #initilizing SparkSession
    spark = (SparkSession.builder
             .appName('RealtimeVoting')
             .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0')#kafka integration
             .config('spark.jars','E:/data eng/election_vote/postgresql-42.7.4.jar')#postgresql driver
             .config('spark.sql.adaptive.enable','false')#disable adaptive query execution
             .getOrCreate())
    # Define the combined schema
    schema = StructType([
        # Candidate table fields
        StructField("candidate_id", StringType(), nullable=False),
        StructField("candidate_name", StringType(), nullable=True),
        StructField("party_affiliation", StringType(), nullable=True),
        StructField("biography", StringType(), nullable=True),
        StructField("campaign_platform", StringType(), nullable=True),
        StructField("photo_url", StringType(), nullable=True),
        
        # Voter table fields
        StructField("voter_id", StringType(), nullable=False),
        StructField("voter_name", StringType(), nullable=True),
        StructField("age", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("nationality", StringType(), nullable=True),
        StructField("registration_number", StringType(), nullable=True),
        StructField("address_street", StringType(), nullable=True),
        StructField("address_city", StringType(), nullable=True),
        StructField("address_state", StringType(), nullable=True),
        StructField("address_country", StringType(), nullable=True),
        StructField("address_postal_code", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("phone_number", StringType(), nullable=True),
        StructField("picture_url", StringType(), nullable=True),
        
        # Vote table fields
        StructField("vote_date", TimestampType(), nullable=True),
        StructField("vote", IntegerType(), nullable=True),
    ])
    vote_df = (spark.readStream.
               format('kafka').
               option('kafka.bootstrap.servers', 'localhost:9092').
               option('subscribe', 'voter').
               option('startingOffsets', 'earliest').
               load().
               selectExpr('CAST(value AS STRING)').
               select(from_json('value', schema).alias('data')).
               select('data.*'))
    
    vote_df = vote_df.withColumn('vote_date',col('vote_date').cast(TimestampType())).withColumn('vote',col('vote').cast(IntegerType()))
    enriched_votes_df = vote_df.withWatermark('vote_date', '1 minute')
    

    #Aggregate vote per candidate and turnout by location
#initialize spark 
import findspark
findspark.init()

# Import the necessary packages to connect spark streaming to kafka 
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.1 pyspark-shell'

# Import the necessary packages 
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from subprocess import check_output
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import json
import time
 
from geopy.extra.rate_limiter import RateLimiter
from geopandas import *
from geopy import * 
from geopy.geocoders import Nominatim
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer,StopWordsRemover

# Configure spark 
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()
SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP

spark_conf = SparkConf()
spark_conf.setAll(
    [
        ("spark.master", SPARK_MASTER_URL),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.app.name", "Data processing in Spark"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]
)
# Create a sparkSession 
topic_name = "congestion"
appName = "Data processing in Spark"
if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "20g").appName(appName).getOrCreate()  
    sc = spark.sparkContext 
    sqlContext = SQLContext(sc)
    # Connect to ES 
    es = Elasticsearch(hosts = "http://localhost:9200/")
    # elasticsearch mapping
    es_mapping = {
        "mappings": {
           "properties": 
             {
            "id": {"type": "long"},
            "created_at": {"type": "date"}, 
            "lat" : { "type" : "long" },
            "lng" : { "type" : "long" },
            "Tweet" : { "type" : "text" }, 
            "geo": {"type": "geo_point"},
            "Prediction" : { "type" : "text" }
           }
        
        }
        
    }
    
    response = es.indices.create(
        index = "tws_index",
        body = es_mapping,
        ignore = 400 # Ignore Error 400 Index already exists
    )
    if 'acknowledged' in response:
        if response['acknowledged'] == True:
              print("Index mapping SUCCESS: ", response['index'])
    
    # Read tweet stream directly from kafka broker and store it into spark Dataframe 
    print("Reading from topic")
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "congestion") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 100) \
        .option("enable.auto.commit", "true") \
        .load()
    twitterSchema = (
        StructType()
            .add("id", StringType(), True)
            .add("created_at",StringType(),True) 
            .add("text", StringType(), True)) 
    def func_call(df, batch_id):
        df.selectExpr("CAST(key as STRING)","CAST(value as STRING)").select(
            from_json(col("value"), twitterSchema).alias("tweet_field")
        ).select(
            "tweet_field.id",
            "tweet_field.created_at",
            "tweet_field.text",) 
        
        requests = df.rdd.map(lambda x:  x[1])
        tweetList = requests.map(lambda x: json.loads(x)).collect() 
        tweets_DF = spark.createDataFrame(data=tweetList, schema = twitterSchema)
        geolocator = Nominatim(user_agent = "tweetgeolocationanalysis")
        geo_udf = udf(lambda y,x: [x, y], ArrayType(FloatType(),False))

        location=[]
        Tweet=[]
        latitude=[]
        longitude=[]
        count=0
        for index, row_iterator in tweets_DF.toPandas().iterrows():
          Counter=0
          count+=1
          tweet = row_iterator[2]
          li=tweet.split("\n")
          for i in li:
            if i:
              Counter += 1
          if Counter>1:
            lo=tweet.split('\n')[0]
            location.append(lo)
            Tweet.append(tweet.split('\n')[1])
            geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.5)
            if geolocator.geocode(lo, timeout=None) != None:
              
              lat=geolocator.geocode(lo, timeout=None).latitude
              lng=geolocator.geocode(lo, timeout=None).longitude
              print(lat)
              print(lng)
              print(count)
              latitude.append(lat)
              longitude.append(lng)
              
            else:
              latitude.append(0.0)
              longitude.append(0.0)
          else:
            location.append('')
            Tweet.append(tweet)
            latitude.append(0.0)
            longitude.append(0.0)
        
        loc_df= sqlContext.createDataFrame([(l,) for l in latitude], ['lat'])
        loc_df.show(truncate=False)
        loc_df=loc_df.toPandas()
        print(len(loc_df))
        loc_df['lng']=longitude
        loc_df['Tweet']=Tweet
        

        text_df=spark.createDataFrame(loc_df)
        tw_DF = tweets_DF.join(text_df)
        tw_DF.show(4)
        tw_DF = tw_DF.withColumn('geo', geo_udf(tw_DF.lat, tw_DF.lng))
        tw_DF.show(4)
        tw_DF = tw_DF.withColumn('created_at',to_timestamp(col('created_at')))
        
        loaded_model = PipelineModel.load("model")
        tw_DF = loaded_model.transform(tw_DF).select("id","created_at","lat","lng","Tweet","geo", 'prediction')
        
        tw_DF = tw_DF.withColumn('prediction', when(tw_DF.prediction == 0,'fires')
                                              .when(tw_DF.prediction == 1,'robbery')
                                              .when(tw_DF.prediction == 2,'accident')
                                              .otherwise('crime')) 
        tw_DF.show(4)
        
        tw_DF.write.format("org.elasticsearch.spark.sql").mode("append").option("checkpointLocation", "/tmp/fben/stream_cp/").option("es.nodes", "http://localhost:9200/").save("tws_index")
        
    display=df.writeStream.format("console").foreachBatch(func_call).trigger(processingTime=("180 seconds")).start().awaitTermination()

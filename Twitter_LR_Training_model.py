# Initialize spark 
import findspark
findspark.init()

# Import the necessary packages to connect spark streaming to kafka 
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:3.2.0 pyspark-shell'

# Import the necessary packages 
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import when
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer,StopWordsRemover
from subprocess import check_output
from pyspark.sql.functions import from_json, col
import json 
from geopy.extra.rate_limiter import RateLimiter
from geopandas import *
from geopy import * 
from geopy.geocoders import Nominatim
from kafka import KafkaConsumer



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
        ("spark.app.name", "Data modelling in Spark"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]
)
# Create a sparkSession 
topic_name = "congestion"
appName = "Data modelling in Spark"
if __name__ == "__main__":
    
    spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "20g").appName(appName).getOrCreate()  
    sc = spark.sparkContext 
    sqlContext = SQLContext(sc)
    
# Creating ML model 
dataset = '/home/malek/Documents/training_set.csv'
def train(dataset):
    # Prepare training set
    trainingSchema = (
        StructType()
            .add("Columnid", StringType(), True)
            .add("id", StringType(), True)
            .add("created_at", StringType(), True)
            .add("Tweet", StringType(), True)
            .add("location", StringType(), True)
            .add("type_incident", StringType(), True)
            .add("sentiment", IntegerType(), True)
            )
    training_set = spark.read.csv(dataset, schema = trainingSchema, header = True, sep = ',') 
    #this will show only 10 rows of the training_set  
    training_set.show(truncate=False, n=10)
    print("Creating ML pipeline...", "\n")
    # Configure an ML pipeline, which consists of ten stages: tokenizer,StopWordsRemover,hashingTF, IDF and two LogisticRegression instances 
    stage_1 = Tokenizer(inputCol= "Tweet", outputCol="tokens")
    stage_2 = StopWordsRemover(inputCol = 'tokens', outputCol = 'filtered_words')
    stage_3 = HashingTF(numFeatures=2**16, inputCol="filtered_words", outputCol='tf')
    stage_4 = IDF(inputCol='tf', outputCol="vector", minDocFreq=1) 
    # Create a LogisticRegression instance 
    lr = LogisticRegression(featuresCol = 'vector', labelCol = 'sentiment', maxIter=10)
    pipeline = Pipeline(stages = [stage_1, stage_2, stage_3, stage_4, lr]) 
    print("Pipeline created", "\n")
    # Fit the pipeline to training set 
    print("Creating logit model...", "\n")
    model = pipeline.fit(training_set)
    print(" model Created! ", "\n")
    
    # Make predictions on testing set  and print columns of interest.
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
        

        location=[]
        Tweet=[]
        Point=[]
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
        #loc_df.printSchema()
        #loc_df.show(truncate=False)
        loc_df=loc_df.toPandas()
        loc_df['lng']=longitude
        loc_df['Tweet']=Tweet
        text_df=spark.createDataFrame(loc_df)
        tw_DF = tweets_DF.join(text_df)
        
        prediction = model.transform(tw_DF)
        selected = prediction.select("id","created_at","text","lat","lng","Tweet", "probability", "prediction")
        selected.show(truncate=False, n=5)
    
    # Building the model
        modelSummary = model.stages[-1].summary
        print('model accuracy :',  modelSummary.accuracy)
    
    model.save("model")
    print(" model saved", "\n")
    display=df.writeStream.format("console").foreachBatch(func_call).trigger(processingTime=("180 seconds")).start().awaitTermination()
train(dataset) 

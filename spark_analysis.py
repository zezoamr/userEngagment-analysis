from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import column, expr, json_tuple, window, current_timestamp
import sys, os

class spark_analysis():
    
    def __init__(self):
        self.baseUsers : dataframe = None
        self.basePages : dataframe = None
        scala_version = '2.12'  # TODO: Ensure this is correct
        spark_version = '3.3.0'
        packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.2.3','org.apache.commons:commons-pool2:2.11.1'
        ,'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1'
        ]
        
        self.spark = (SparkSession.builder.config("spark.master", "local[2]")
                    .config("spark.jars.packages", ",".join(packages))
                    .getOrCreate())
        self.sc = self.spark.sparkContext
        self.sc.setLocalProperty("spark.scheduler.mode", "FAIR")
        
        #self.sc = self.spark.sparkContext #for debugging
        #self.sc.setLogLevel("INFO")
    
    def read_kafka(self, basetopic : str = 'pages', derivedtopic : str = 'pages_per_customer', source : str = "localhost:9092",  **kwargs):
        self.basePages = ( self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", source)
        .option("subscribe", basetopic).option("startingOffsets", "earliest") 
        .load()
        )
        
        self.baseUsers = ( self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", source)
        .option("subscribe", derivedtopic).option("startingOffsets", "earliest") 
        .load()
        )
        
    
    def write(self):
        self.basePages.writeStream.format("csv").outputMode("append") \
        .option("path", r"D:\projects\kafka_userEngagment\output\mostactivePages") \
        .option("checkpointLocation", r"D:\projects\kafka_userEngagment\spark_checkpoints\pages")\
        .start() 
        
        self.baseUsers.writeStream.format("csv").outputMode("append")\
        .option("path", r"D:\projects\kafka_userEngagment\output\mostactiveUsers")\
        .option("checkpointLocation", r"D:\projects\kafka_userEngagment\spark_checkpoints\users")\
        .start() \
        .awaitTermination()
    
    def analysis(self):
        try:
            self.read_kafka()
            
            #get most active pages sorted within window repeating
            self.basePages.select(expr("cast(value as string)"), current_timestamp().alias("processingTime")) \
            .withColumn("PAGE_ID", json_tuple(column("value"),"PAGE_ID"))\
            .groupBy( window( ("processingTime"),  "10 seconds").alias("window"), "PAGE_ID")\
            .agg({"PAGE_ID": "count"})\
            .select(
            column("window").getField("start").alias("start"),
            column("window").getField("end").alias("end"),
            column("PAGE_ID"),
            column("count(PAGE_ID)")
            )\
            .orderBy("count(PAGE_ID)", ascending=False)\
            # .writeStream.format("console").outputMode("complete").start().awaitTermination() #for debugging
            
            #get most active customer by id and the number of visits, updated every 10 seconds
            self.baseUsers.select(expr("cast(value as string)"), expr("cast(Key as string) as CUSTOMER"),
                                current_timestamp().alias("processingTime"))\
            .withColumn("COUNT_DISTINCT", json_tuple(column("value"),"COUNT_DISTINCT"))\
            .groupBy( window( ("processingTime"),  "10 seconds").alias("window"), "CUSTOMER")\
            .agg({"COUNT_DISTINCT": "max"})\
            .select(
            column("window").getField("start").alias("start"),
            column("window").getField("end").alias("end"),
            column("CUSTOMER"),
            column("max(COUNT_DISTINCT)")
            )\
            .orderBy("max(COUNT_DISTINCT)", ascending=False)\
            #.writeStream.format("console").outputMode("complete").start().awaitTermination() #for debugging
            
            self.write()
        
        except KeyboardInterrupt:
            print('Interrupted')
            self.terminate()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def terminate(self):
            self.spark.stop()


m = spark_analysis()
m.analysis() 
import requests
import json

class ksqldb_chat_init():
    header = {
    "Accept" : "application/vnd.ksql.v1+json", 
    "Content-Type" : "application/json"
    }
    url = "http://localhost:8088/ksql"
    
    #data dir is specified in docker yaml file as a volume (could have used $pwd)
    Source = json.dumps({
    "ksql" :  '''
    CREATE SOURCE CONNECTOR IF NOT EXISTS `customer` WITH (
    'name' = 'engagment_analysis_csvSource',
    'topic' = 'pages',
    'connector.class' =  'com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector',
    "input.path" = '/data/mock_source', 
    "input.file.pattern" = '.*\\.csv',
    "error.path" ='/data/errorAndStatuses',
    "finished.path" ='/data/finished',
    'csv.first.row.as.header' = 'true',
    'schema.generation.key.fields' = 'customer',
    'schema.generation.enabled' = 'true');
    ''',
      "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest" }})
    #value.converter=io.confluent.connect.avro.AvroConverter  'output.data.format'       = 'JSON'
    # "cleanup.policy" = 'MOVE',    'tasks.max' = 1,
    #value.converter.schema.registry.url=http://schema-registry:8081 "csv.separator.char" = '44',    "halt.on.error" = 'false',
    # 

    #FORMAT = 'JSON',
    streamsAndTables = json.dumps({
    "ksql": '''
    -- Create stream of pages
    CREATE STREAM IF NOT EXISTS pages (
      customer INTEGER KEY,
      time BIGINT,
      page_id VARCHAR,
      pageurl VARCHAR
    ) WITH (
      KAFKA_TOPIC = 'pages', FORMAT = 'JSON',
      PARTITIONS = 6
    );

    -- Create stateful table with Array of pages visited by each customer, using the `COLLECT_LIST` function
    -- Get `COUNT_DISTINCT` page IDs
    CREATE TABLE IF NOT EXISTS pages_per_customer WITH (KAFKA_TOPIC = 'pages_per_customer') AS
    SELECT
      customer,
      COLLECT_LIST(pageurl) AS page_list,
      COUNT_DISTINCT (page_id) AS count_distinct
    FROM pages
    GROUP BY customer
    EMIT CHANGES;''',
      "streamsProperties": {
        "ksql.streams.auto.offset.reset": "earliest"
      }
    })
    
    def ksqldb_post(self, url, header, data):
      r = requests.post(
      url=url,
      headers=header,
      data=data
      )
      print(r.json())
        
    def createStreams(self):
      self.ksqldb_post(self.url, self.header, self.streamsAndTables)
    
    def createSource(self):
      self.ksqldb_post(self.url, self.header, self.Source)
      
#if __name__ == '__main__':
  #m = ksqldb_chat_init()
  #m.createStreams() 
  #m.createSource()


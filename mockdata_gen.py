from faker import Faker
from random import randrange, choice
import sys, os, string, time, csv

class mockdata():
    
    def __init__(self):
        self.stop : bool = False
        self.fake : Faker = Faker()
        self.fake.seed_instance(4322)
        
    def customer(self): 
        return randrange(0,1000,1)
        
    def page(self): #url
        name = self.fake.name().replace(" ", "")
        return f"www.{name}swebsite.com" 
        
    def page_id(self): #xx-xx
        return choice(string.ascii_letters) + choice(string.ascii_letters) + "-" + choice(string.ascii_letters) + choice(string.ascii_letters)
    
    def getUnixTime(self):
        return int(time.time()) 
        
    def writeLineToCsv(self):
        c = self.customer()
        t = self.getUnixTime()
        p = self.page()
        pid = self.page_id()
        row = [c , t , p , pid] 
        #print(row)
        # open the file in the write mode
        with open('D:\projects\kafka_userEngagment\mock_source\mock.csv', 'a', newline='') as f:
            # create the csv writer
            writer = csv.writer(f, escapechar='' ,quoting=csv.QUOTE_NONE)
            # write a row to the csv file
            writer.writerow(row)
            
    def writeheader(self):
        row =  ['customer','time','page_id','pageurl']
        with open(f'D:\projects\kafka_userEngagment\mock_source\mock.csv', 'w', newline='') as f:
            # create the csv writer
            writer = csv.writer(f, escapechar='' ,quoting=csv.QUOTE_NONE)
            # write a row to the csv file
            writer.writerow(row)
            
    def writeLinesToCsv(self, delay = 0.3):
        self.stop = False
        try:
            while not self.stop :    
                self.writeLineToCsv()
                time.sleep(delay)
        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def producer(self):
        from confluent_kafka import Producer
        self.prod = Producer({"bootstrap.servers":"localhost:9092"})
        c = self.customer()
        t = self.getUnixTime()
        p = self.page()
        pid = self.page_id()
        self.prod.produce('pages', f"{str(c)}, {str(t)}, {str(p)}", pid)
        self.prod.flush()    
    
    def ksqldb_insert(self):
        import json, requests
        header = {
        "Accept" : "application/vnd.ksql.v1+json", 
        "Content-Type" : "application/json"
        }
        url = "http://localhost:8088/ksql"
        c = self.customer()
        #t = self.getUnixTime()
        p = self.page()
        pid = self.page_id()
        Ss = json.dumps({
        "ksql" :  f"INSERT INTO pages (customer, time, page_id, pageurl) VALUES ({c}, UNIX_TIMESTAMP(), '{pid}', '{p}');",
        "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest" }})
        r = requests.post(
        url=url,
        headers=header,
        data=Ss
        )
        print(r.json())
        
    def ksqldb_insertStream(self, delay = 0.3):
        self.stop = False
        try:
            while not self.stop :    
                self.ksqldb_insert()
                time.sleep(delay)
        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def terminate(self):
        self.stop = True    

#if __name__ == '__main__':      
    #mock = mockdata()
    #mock.ksqldb_insertStream()
    #mock.producer()
    #mock.writeheader()
    #mock.writeLinesToCsv()

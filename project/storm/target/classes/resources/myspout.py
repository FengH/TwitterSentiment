from storm import Spout, emit, log
from kafka.consumer import KafkaConsumer

consumer = KafkaConsumer('sentiment', bootstrap_servers='ip-172-31-18-27.ec2.internal:6667')

def getData():	
	data = consumer.next().value
	return data

class MySpout(Spout):
    def nextTuple(self):
        data = getData()
        emit([data])
   
MySpout().run()

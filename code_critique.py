from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import date
from datetime import datetime
import pandas as pd
import numpy as np
if __name__ == '__main__':

        # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'project1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)



    # Subscribe to topic
    consumer.subscribe([topic])

    
    now = datetime.now()
    d = now.strftime("%m-%d-%Y")
    
    
    fname = d + 'out.json'
    file = open(fname, 'w')
    file.close
    # Process messages
    total_count = 0
    out = []
    done = 0; 
    try:
        while True:
            msg = consumer.poll(1.0)
            #once the data is all consumed we start transformation
            if( msg is None and done == 0):
                #save to out.json
                with open(fname, mode = 'a', encoding = 'utf-8') as f:
                    json.dump(out, f, indent = 4)
                    done = 1

                #load and get ready to transform
                with open(fname, mode = 'r', encoding='utf-8') as f:
                    obj = json.load(f)
                #Limit: check if date is unique
                sr = data['OPD_DATE'].unique()
                if(len(sr) == 1):
                    print("Only one date in file")
                else:
                    #process to change all date back to moust occurence
                    print("More than one date in file")
                    date = data['OPD_DATE'].value_counts().idxmax()
                    print(date)
                    for entry in obj:
                        if(entry['OPD_DATE'] != date):
                            entry['OPD_DATE'] = date
                #Limit: direction transformation
                for entry in obj:
                    if(entry['DIRECTION'] == '' or int(entry['DIRECTION']) < 0 or int(entry['DIRECTION']) > 360):
                        entry['DIRECTION'] = 0
                #Limit: Velocity can't be negative
                    if(entry['VELOCITY'] == '' or int(entry['VELOCITY']) < 0):
                        entry['VELOCITY'] = 0

                #transformation done, dump back to out.json
                with open(fname, mode = 'w', encoding = 'utf-8') as f:
                    json.dump(obj, f, indent = 4)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                done = 0
                # Check for Kafka message
                record_value = msg.value()
                value = json.loads(record_value)
                total_count+=1
                #entry is a json object
                entry = {}
                #put each breadscmp into object
                entry = value
                #add to the out
                out.append(entry)
                
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

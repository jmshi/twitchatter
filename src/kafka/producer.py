#!/usr/bin/env python
from __future__ import print_function
from chat.chat import TwitchChatStream
import config
import datetime
import json
from kafka import KafkaProducer
import sys
import threading, time
#import twitch_client
#from twitch_client.twitch import TwitchClient

def get_active_channel(limit=2):
    #client = TwitchClient(client_id=config.channel_client_id)
    #stream  = client.streams.get_live_streams(limit=100)
    #
    #return [item[u'channel'][u'display_name'].encode('ascii','ignore').lower() for item in stream]
    return ['tsm_hamlinz','sodapoppin']


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
    def stop(self):
        self.stop_event.set()

    def run(self):
        self.producer = KafkaProducer(bootstrap_servers=config.ip_address)
        n_channel = 2
        channel_list = get_active_channel(limit=n_channel)
        chatstream_list = []
        for channel in channel_list:
            chatstream = TwitchChatStream(username=channel,
                                          oauth=config.chat_token,
                                          verbose=False)
            chatstream_list.append(chatstream)
    
        # Continuously check if messages are received (every ~10s)
        # This is necessary, if not, the chat stream will close itself
        # after a couple of minutes (due to ping messages from twitch)
        while True:
            for chatstream in chatstream_list:
                received = chatstream.twitch_receive_messages()
                if received:
                    #print("producer: {}".format(received[0]))
                    self.producer.send('my_topic', json.dumps(received[0]).encode('utf-8'))
        self.producer.close()


if __name__ == "__main__":
    prod = Producer()
    prod.run()
  

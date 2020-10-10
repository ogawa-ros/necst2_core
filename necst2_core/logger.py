#!/usr/bin/env python3

node_name = 'logger'

import time
import db_logger_operation
from functools import partial
import rclpy
import std_msgs.msg

class Logger(object):
    
    def __init__(self):
        self.node = rclpy.create_node(node_name)
        
        self.db_log = db_logger_operation.db_logger_operation()
        
        self.ignore_topics = [
                        '/rosout',
                        '/parameter_events',
                        ]
        
        self.sub_path = self.node.create_subscription(std_msgs.msg.String,'/logger_path',self.callback_path,1,)

        self.node.create_timer(1,self.loop)
    
    #old logger function
    def get_current_topic_list(self):
        topic_list = self.node.get_topic_names_and_types()
        for topic in topic_list[:]:
            if topic[0] in self.ignore_topics:
                topic_list.remove(topic)
            #elif topic[1] in ignore_types:
            #    topic_list.remove(topic)
                pass
            continue
        return topic_list

    def make_subscriber(self,topic):
        topic_name = topic[0]
        topic_type = eval(topic[1][0].replace('/', '.'))
        self.node.create_subscription(
            topic_type,
            topic_name,
            partial(self.callback, topic_name),
            1
            )

    def callback(self, topic_name, req):
        print(req.SLOT_TYPES)
        #slots = [{'key': key,
        #          'type': type_,
        #          'value': req.__getattribute__(key)}
        #        for key, type_
        #        in zip(req.__slots__, req._slot_types)]

        data = {'topic': topic_name, 'received_time': time.time(), 'data': req.data}

        self.db_log.regist(data)
        return


    def loop(self):
        self.subscribing_topic_list = []
        current_topic_list = self.get_current_topic_list()
        for topic in current_topic_list:
            if topic not in self.subscribing_topic_list:
                self.make_subscriber(topic)
                self.subscribing_topic_list.append(topic)
                pass
            continue


    #old db_logger_operation function
    def callback_path(self, req):
        self.db_log.callback_path(req)
        return


        
def main():
    rclpy.init()
    logger = Logger()
    rclpy.spin(logger.node)

    logger.node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()


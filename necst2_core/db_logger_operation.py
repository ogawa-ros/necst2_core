#!usr/bin/env python3

ndoe_name  = 'db_logger_operation'

import time
import threading
import necstdb
import pathlib
import std_msgs.msg

class db_logger_operation(object):

    def __init__(self):
        self.data_list = []
        self.table_dict = {}
        self.db_dir = pathlib.Path.home() / 'data/operation'
        self.db_path = ''

        self.th = threading.Thread(target= self.loop)
        self.th.start()
        pass

    def callback_path(self, req):
        if req.data != '':
            self.db_path = ''
            self.data_list = []
            self.close_tables()
            self.db = necstdb.opendb(self.db_dir / req.data, mode = 'w')
            self.db_path = req.data
            time.sleep(0.1)

        else:
            self.db_path = req.data
            pass
        return

    def close_tables(self):
        tables = self.table_dict
        self.table_dict = {}
        [tables[name].close() for name in tables]
        return

    def regist(self, data):
        if self.db_path != '':
            self.data_list.append(data)
            pass
        return

    def loop(self):

        while True:
            if len(self.data_list) ==0:
                self.close_tables()
                continue

            d = self.data_list.pop(0)

            table_name = d['topic'].replace('/', '-').strip('-')
            table_time = d['received_time']
            table_data = d['data']
            if table_name not in self.table_dict:
                f[table_name] = open(self.db_dir,'w')
                


                #self.db.create_table(table_name,
                 #           {'data': table_info,
                  #           'memo': 'generated by db_logger_operation',
                   #          'version': necstdb.__version__,})

                self.table_dict[table_name] = self.db.open_table(table_name, mode='ab')
                pass

            self.table_dict[table_name].append(*table_data)
            continue
            
        return

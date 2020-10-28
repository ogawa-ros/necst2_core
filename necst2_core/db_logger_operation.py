#!usr/bin/env python3

ndoe_name  = 'db_logger_operation'

import time
import threading
import necstdb
import pathlib
import array
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
                time.sleep(0.01)
                continue

            d = self.data_list.pop(0)

            table_name = d['topic_name'].replace('/', '-').strip('-')
            table_data = [d['received_time']]
            table_info = [{'key': 'received_time',
                           'format': 'd',
                           'size': 8}]

            for slot in d['slots']:
                print(slot)
                if slot['type'] in 'bool':
                    info = {'format': 'c', 'size': 1}

                elif slot['type'] in 'byte[]':
                    continue

                elif slot['type'] in 'byte':
                    info = {'format': '{0}s'.format(len(slot['value'])), 'size': len(slot['value'])}

                elif slot['type'] in 'char[]':
                    continue

                elif slot['type'] in 'char':
                    info = {'format': 'c', 'size': 1}
                    if isinstance(slot['value'], str):
                        slot['value'] = slot['value'].encode()
                        pass

                elif slot['type'] in 'float':
                    info = {'format': 'f', 'size': 4}

                elif slot['type'] in 'double':
                    info = {'format': 'd', 'size': 8}

                elif slot['type'] in 'int8':
                    info = {'format': 'b', 'size': 1}

                elif slot['type'] in 'int16':
                    info = {'format': 'h', 'size': 2}

                elif slot['type'] in 'int32':
                    info = {'format': 'i', 'size': 4}

                elif slot['type'] in 'int64':
                    info = {'format': 'q', 'size': 8}

                elif slot['type'] in 'string[]':
                    continue

                elif slot['type'] in 'string':
                    print('ok')
                    info = {'format': '{0}s'.format(len(slot['value'])), 'size': len(slot['value'])}
                    if len(slot['value'])%4 == 0:
                        str_size = len(slot['value'])
                    else:
                        str_size = len(slot['value']) + (4-len(slot['value'])%4)

                    info = {'format': '{0}s'.format(str_size), 'size': str_size}
                    if isinstance(slot['value'], str):
                        slot['value'] = slot['value'].encode()
                        pass

                elif slot['type'] in 'uint8':
                    info = {'format': 'B', 'size': 1}

                elif slot['type'] in 'unit16':
                    info = {'format': 'H', 'size': 2}

                elif slot['type'] in 'unit32':
                    info = {'format': 'I', 'size': 4}

                elif slot['type'] in 'unit64':
                    info = {'format': 'Q', 'size': 8}
                else:
                    continue

                if isinstance(slot['value'], array.array):
                    # for MultiArray
                    dlen = len(slot['value'])
                    info['format'] = '{0:d}{1:s}'.format(dlen, info['format'])
                    info['size'] *= dlen
                    table_data +=list(slot['value'])
                else:
                    table_data += [slot['value']]
                    pass

                info['key'] = slot['key']
                table_info.append(info)


            if table_name not in self.table_dict:
                self.db.create_table(table_name,
                            {'data': table_info,
                             'memo': 'generated by db_logger_operation',
                             'version': necstdb.__version__,})

                self.table_dict[table_name] = self.db.open_table(table_name, mode='ab')
                pass
            print(*table_data)
            self.table_dict[table_name].append(*table_data)
            continue
        return

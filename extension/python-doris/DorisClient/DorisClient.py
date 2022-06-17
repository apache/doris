#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2022/1/10 10:24
# @Author : way
# @Site :
# @Describe: doris client

import logging
import base64
import json
import time
import requests
import pymysql


def Logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


logger = Logger(__name__)


# 重试装饰器
def retry(func):
    max_retry = 3

    def run(*args, **kwargs):
        for i in range(max_retry + 1):
            if i > 0:
                logger.warning(f"等待3秒后，尝试第{i}次重试...")
            time.sleep(3)
            flag = func(*args, **kwargs)
            if flag:
                return flag

    return run


class DorisSession:

    def __init__(self, fe_servers, database, user, passwd, mysql_port=9030):
        """
        :param fe_servers: fe servers ['127.0.0.1:8030', '127.0.0.2:8030', '127.0.0.3:8030']
        :param database: 数据库名称
        :param user: 用户名
        :param passwd: 密码
        """
        assert fe_servers
        assert database
        assert user
        assert passwd
        self.fe_servers = fe_servers
        self.database = database
        self.Authorization = base64.b64encode((user + ':' + passwd).encode('utf-8')).decode('utf-8')
        self.conn = pymysql.connect(host=fe_servers[0].split(':')[0],
                                    port=mysql_port,
                                    database=database,
                                    user=user,
                                    passwd=passwd)

    def _label(self, table):
        return f"{table}_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"

    def _columns(self, keys):
        return ','.join([f'`{column}`' for column in keys])

    def _get_be(self, table, headers):
        for fe_server in self.fe_servers:
            host, port = fe_server.split(':')
            url = f'http://{host}:{port}/api/{self.database}/{table}/_stream_load'
            response = requests.put(url, '', headers=headers, allow_redirects=False)
            if response.status_code == 307:
                return response.headers['Location']
        else:
            raise Exception(f"执行失败，获取不到可用的BE节点，请检查 fe servers 配置")

    @retry
    def streamload(self, table, json_array, **kwargs):
        """
        :param table: 目标表
        :param json_array: json 数组
        :param kwargs: streamload 的其它特殊用途参数
        比如 merge_type：数据合并类型。默认为 APPEND，表示本次导入是普通的追加写操作。
                         MERGE 和 DELETE 类型仅适用于 Unique Key 模型表。
                         其中 MERGE 类型需要配合 delete 参数使用，以标注 Delete Flag 列。
                         而 DELETE 类型则表示本次导入的所有数据皆为删除数据。
                         -H "merge_type: MERGE"
             delete：仅在 MERGE 类型下有意义，用于指定 Delete Flag 列以及标示删除标记的条件。
                     -H "delete: col3 = 1"
             sequence_col ：仅针对 Unique Key 模型的表。用于指定导入数据中表示 Sequence Col 的列。
                            主要用于导入时保证数据顺序。
                            -H "function_column.sequence_col: col3"
        :return:
        """
        headers = {
            'Expect': '100-continue',
            'Authorization': 'Basic ' + self.Authorization,
            'format': 'json',
            'strip_outer_array': 'true',  # json数组
            'fuzzy_parse': 'true',  # 数组中每一行的字段顺序完全一致,加速导入速度
        }
        if kwargs.get('sequence_col'):  # 保证数据顺序, 需启用 Sequence Column 功能
            headers['function_column.sequence_col'] = kwargs.get('sequence_col')
        if kwargs.get('merge_type'):
            headers['merge_type'] = kwargs.get('merge_type')
        if kwargs.get('delete'):
            headers['delete'] = kwargs.get('delete')

        url = self._get_be(table, headers)
        headers['label'] = self._label(table)
        headers['columns'] = self._columns(json_array[0].keys())
        response = requests.put(url, json.dumps(json_array), headers=headers, allow_redirects=False)
        if response.status_code == 200:
            res = response.json()
            if res.get('Status') == 'Success':
                logger.info(res)
                return True
            else:
                logger.error(res)
                return False
        else:
            logger.error(response.text)
            return False

    def execute(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            self.conn.commit()
        return True

    def read(self, sql):
        with self.conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def __del__(self):
        try:
            self.conn.close()
        except:
            ...
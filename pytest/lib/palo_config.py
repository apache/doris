#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
/***************************************************************************
  *
  * @file palo_config.py
  * @date 2015/01/30 09:38:51
  * @brief this file is config about palo for test
  *
  **************************************************************************/
"""

# 测试环境相关的信息记在这里
# 比如hadoop, mysql
import sys
sys.path.append('../deploy')
import env_config
import util


class PaloConfig(object):
    """
    PaloConfig类
    """
    def __init__(self):
        """
        PaloConfig__init__方法
        """
        import os
        self.fe_host = env_config.master
        self.fe_query_port = env_config.fe_query_port
        self.fe_http_port = env_config.fe_query_port - 1000
        self.fe_user = "root"
        self.fe_password = ""
        self.palo_db = "test_query_qa"

        self.host_username = env_config.host_username
        self.host_password = env_config.host_password
        self.java_home = env_config.JAVA_HOME
        self.fe_path = env_config.fe_path
        self.be_path = env_config.be_path
        self.be_data_path = env_config.be_data_path_list
        self.dpp_config_str = env_config.dpp_config_str
        self.fe_observer_list = env_config.observer_list
        self.fe_follower_list = env_config.follower_list

        self.mysql_host = "127.0.0.1"
        self.mysql_port = 61001
        self.mysql_user = "root"
        self.mysql_password = ""
        self.mysql_db = "test"

        self.canal_user = 'canal'
        self.canal_password = 'canal'
        self.canal_ip = '127.0.0.1'

        if "FE_HOST" in os.environ.keys():
            self.fe_host = os.environ["FE_HOST"]
        if "FE_QUERY_PORT" in os.environ.keys():
            self.fe_query_port = int(os.environ["FE_QUERY_PORT"])
        if "FE_USER" in os.environ.keys():
            self.fe_user = os.environ["FE_USER"]
        if "FE_PASSWORD" in os.environ.keys():
            self.fe_password = os.environ["FE_PASSWORD"]
        if "FE_WEB_PORT" in os.environ.keys():
            self.fe_http_port = int(os.environ["FE_WEB_PORT"])
        if "FE_DB" in os.environ.keys():
            self.palo_db = os.environ["FE_DB"]

        if "MYSQL_HOST" in os.environ.keys():
            self.mysql_host = os.environ["MYSQL_HOST"]
        if "MYSQL_PORT" in os.environ.keys():
            self.mysql_port = int(os.environ["MYSQL_PORT"])
        if "MYSQL_USER" in os.environ.keys():
            self.mysql_user = os.environ["MYSQL_USER"]
        if "MYSQL_PASSWORD" in os.environ.keys():
            self.mysql_password = os.environ["MYSQL_PASSWORD"]
        if "MYSQL_DB" in os.environ.keys():
            self.mysql_db = os.environ["MYSQL_DB"]

        self.bos_accesskey = "xxxxxxxx"
        self.bos_secret_accesskey = "xxxxx"
        self.bos_endpoint = "http://bj.bcebos.com"
        self.bos_region = "bj"
        self.s3_endpoint = "https://s3.bj.bcebos.com"
        # kafka例行导入信息
        self.kafka_broker_list = 'xxxx'
        self.kafka_zookeeper = 'xxxxx'
        # 备份恢复的仓库地址
        self.defaultFS = 'xxx'
        self.hdfs_location = 'xxxx'
        self.repo_location = 'xxxx'
        self.hdfs_username = 'root'
        self.hdfs_passwd = ''

        # apache hdfs
        self.broker_name = 'hdfs'
        self.broker_property = {"username": self.hdfs_username, "password": self.hdfs_passwd}


config = PaloConfig()


def gen_hdfs_file_path(file_path):
    """
    hadoop上的数据文件的路径，这个路径以"data"目录结束
    hadoop/
         data/
              dir1/
              dir2/...
              file1
              file2...
    这个目录对应于hdfs/data目录
    hdfs/
         data/
              dir1/
              dir2/...
              file1
              file2...
    更改hadoop地址时，只需要将hdfs下的的data目录上传到新的hadoop
    并修改这个配置即可，所有的case就可以正常运行
    放到代码库便于数据文件的组织和维护
    case中只需定义hdfs_path之后的路径
    """
    hdfs_path = 'hdfs://xxxxxxx/data/'
    entire_path = "%s/%s" % (hdfs_path, file_path)
    return entire_path.replace(r"//", r"/").replace(r":/", r"://")


def gen_apache_hdfs_file_path(file_path):
    """
    apache
    """
    hdfs_path = 'hdfs://xxxxxxxx/data'
    entire_path = "%s/%s" % (hdfs_path, file_path)
    return entire_path.replace(r"//", r"/").replace(r":/", r"://")


def gen_bos_file_path(file_path):
    """
    bos
    """
    hdfs_path = 'bos://xxxxx/data/'
    entire_path = "%s/%s" % (hdfs_path, file_path)
    return entire_path.replace(r"//", r"/").replace(r":/", r"://")


def gen_remote_file_path(file_path):
    """生成远端数据的文件地址"""
    return gen_apache_hdfs_file_path(file_path)
    # return gen_hdfs_file_path(file_path)


def gen_s3_file_path(file_path):
    """s3 storage path"""
    s3_path = 's3://xxxx/data/'
    entire_path = "%s/%s" % (s3_path, file_path)
    return entire_path.replace(r"//", r"/").replace(r":/", r"://")


class HDFSInfo(object):
    """
    HDFSInfo
    """
    def __init__(self, properties):
        self.properties = properties

    def get_property(self):
        """get propert str"""
        if isinstance(self.properties, str):
            property = '(%s)' % self.properties
        elif isinstance(self.properties, dict):
            property = util.convert_dict2property(self.properties)
        return property

    def __str__(self):
        s = 'WITH HDFS %s' % (self.get_property())
        return s


class S3Info(HDFSInfo):
    """
    S3Info
    """
    def __str__(self):
        s = 'WITH S3 %s' % (self.get_property())
        return s


class BrokerInfo(HDFSInfo):
    """
    BrokerInfo
    """
    def __init__(self, broker_name, properties):
        super(BrokerInfo, self).__init__(properties)
        self.broker_name = broker_name

    def __str__(self):
        s = 'WITH BROKER "%s" %s' % (self.broker_name, self.get_property())
        return s

    def to_broker_property_dict(self):
        """make broker property to dict"""
        if isinstance(self.properties, str):
            str_p = '{%s}' % self.properties
            property = eval(str_p.replace('=', ':'))
        elif isinstance(self.properties, dict):
            property = self.properties
        return property

    def to_select_into_broker_property_str(self):
        """generate broker property str for select into """
        property_dict = self.to_broker_property_dict()
        select_into_broker_property_list = list()
        select_into_broker_property_list.append('"broker.name"="%s"' % self.broker_name)
        for k, v in property_dict.items():
            select_into_broker_property_list.append('"broker.%s"="%s"' % (k, v))
        return ','.join(select_into_broker_property_list)


broker_info = BrokerInfo(config.broker_name, config.broker_property)
s3_info = S3Info({"AWS_ENDPOINT": config.s3_endpoint, 
                  "AWS_ACCESS_KEY": config.bos_accesskey,
                  "AWS_SECRET_KEY": config.bos_secret_accesskey,
                  "AWS_REGION": config.bos_region})
hdfs_info = HDFSInfo(config.broker_property)


if __name__ == '__main__':
    print(config.broker_property['username'])


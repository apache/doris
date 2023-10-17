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

###########################################################################
#
#   @file palo_client.py
#   @date 2015/02/04 15:26:21
#   @brief Palo client
#
############################################################################

"""
Palo client for Palo2 testing.
"""
# 路径设置
import sys
import os

# 系统库
import time
import logging
import socket
import pycurl
from io import BytesIO
import pytest
import json
import pymysql

# local库
import util
import palo_logger
import palo_job
from palo_config import BrokerInfo
from palo_exception import PaloException
from palo_exception import PaloClientException
from palo_sql import *
from palo_verify import verify, verify_by_sql


# 日志 异常 对象
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


def get_client(host_name, port, database_name=None, user='root', password='',
               charset='utf8', retry=True, http_port=None):
    """get client
    """
    client = PaloClient(host_name, port, database_name, user, password, charset, http_port)
    if client.init(retry):
        return client
    else:
        raise PaloClientException('get client error: host:%s port:%s database:%s '
                                  'user:%s password:%s' % (host_name, port, database_name, user, password))


class PaloClient(object):
    """Palo client.
    成员变量有: host port user password database_name charset connection
    """

    def __init__(self, host, port, database_name=None, user='root', password='', charset='utf8',
                 http_port=None):
        """
        Connect to Palo FE.
        """
        # TODO 成员变量私有化
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database_name = database_name
        self.charset = charset
        self.connection = None
        self.cluster_name = 'default_cluster'
        if http_port is None:
            self.http_port = port - 1000
        else:
            self.http_port = http_port
        self.palo_job_map = {}

    def init(self, retry=True):
        """init
        """
        assert self.host
        retry_times = 0
        while retry_times < 10:
            try:
                self.connection = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    passwd=self.password,
                    charset=self.charset)
                break
            except Exception as e:
                LOG.error(L('Connect to Palo error', fe=str(self), error=e))
                if not retry:
                    return False
                retry_times += 1
                time.sleep(1)
        else:
            LOG.error(L('Connect to Palo error', retry_times=retry_times))
            return False

        LOG.info(L("Connected to Palo", host=self.host, port=self.port, user=self.user))
        database_name = self.database_name
        if database_name is None:
            return True

        database_name_list = self.get_database_list()
        if database_name not in database_name_list:
            if not self.create_database(database_name):
                return False
        self.use(database_name)

        return True

    def __str__(self):
        return "[host: %s, port: %d, database: %s]" % (self.host, self.port, self.database_name)

    def __del__(self):
        if self.connection:
            self.connection.close()

    def __execute_and_rebuild_meta_class(self, sql, palo_job_cls):
        if palo_job_cls.__name__ in self.palo_job_map.keys():
            result = self.execute(sql)
        else:
            cursor, result = self.execute(sql, return_cursor=True)
            description = cursor.description
            idx = 0
            for col_info in description:
                setattr(palo_job_cls, col_info[0], idx)
                idx += 1
            self.palo_job_map[palo_job_cls.__name__] = True
        return result

    def use(self, database_name):
        """
        specify default database.
        """
        try:
            self.database_name = database_name
            self.connection.select_db(database_name)
        except Exception as error:
            LOG.error(L("USE database error", fe=str(self), database_name=database_name, \
                        error=error))
            return False
        return True

    def connect(self):
        """
        建立连接
        """
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                charset=self.charset)
        except:
            LOG.info(L("Re-connected to Palo FE fail.",
                       host=self.host, port=self.port, user=self.user))
            return False
        LOG.info(L("Re-connected to Palo FE success.",
                   host=self.host, port=self.port, user=self.user))
        return True

    def execute(self, sql, return_rows=False, return_cursor=False):
        """
        执行sql语句
        """
        if 'PALO_CLIENT_LOG_SQL' in os.environ.keys():
            LOG.info(L("execute SQL", sql=sql))
        if 'PALO_CLIENT_STDOUT' in os.environ.keys():
            # if sql.upper().startswith('SHOW') or sql.upper().startswith('SELECT'):
            if sql.upper().startswith('SHOW PROC'):
                pass
            else:
                print(sql.strip(';') + ';')
        # avoid: table state is not normal, do not allow doing ALTER ops.
        if sql.upper().startswith('ALTER'):
            LOG.info(L("before alter sleep 0.5s"))
            time.sleep(0.5)
        cursor = self.connection.cursor()
        try:
            rows = cursor.execute(sql)
        except pymysql.err.InternalError:
            self.connection.ping(reconnect=True)
            rows = cursor.execute(sql)
        if not return_rows and not return_cursor:
            return cursor.fetchall()
        elif return_rows:
            return rows, cursor.fetchall()
        elif return_cursor:
            return cursor, cursor.fetchall()
        else:
            return cursor.fetchall()

    def verify(self, expected_file_list, table_name, database_name=None,
                save_file_list=None, encoding='utf8', cluster_name=None, key_desc='aggregate'):
        """
        verify
        """
        LOG.info(L("check file:", file=expected_file_list))
        database_name = self.database_name if database_name is None else database_name
        # 获取的表的schema，desc
        schema = self.desc_table(table_name, database_name)
        data = self.select_all(table_name, database_name)
        return verify(expected_file_list, data, schema, table_name, database_name, encoding, save_file_list)

    def verify_by_sql(self, expected_file_list, sql, schema, save_file_list=None):
        """verify by sql"""
        LOG.info(L("check file:", file=expected_file_list))
        # 获取的表的schema，desc
        data = self.execute(sql)
        return verify_by_sql(expected_file_list, data, schema, 'check_by_sql', self.database_name, None, save_file_list)

    def create_database(self, database_name, cluster_name=None):
        """
        创建database
        """
        sql = "CREATE DATABASE %s" % database_name
        self.execute(sql)
        if not cluster_name:
            cluster_name = self.cluster_name
        database_name_list = self.get_database_list(cluster_name)
        self.use(database_name)
        if database_name in database_name_list:
            self.database_name = database_name
            LOG.info(L("CREATE DATABASE succ", database_name=database_name, fe=str(self)))
            return True
        else:
            LOG.warning(L("CREATE DATABASE fail", database_name=database_name, fe=str(self)))
            return False

    def create_table(self, table_name, column_list,
                     partition_info=None, distribution_info=None, storage_type=None,
                     storage_medium=None, storage_cooldown_time=None, bloom_filter_column_list=None,
                     replication_num=None, database_name=None, set_null=False, keys_desc=None,
                     bitmap_index_list=None, dynamic_partition_info=None, replication_allocation=None,
                     light_schema_change=None, enable_unique_key_merge_on_write=None):
        """
        Create table
        Attributes:
            column_list: 由4元组(name, type, agg_type, default_value)组成的list, 后两项可省略
                需要注意的是key列指定默认值是agg_type设置为None
                Example: [("k1", "int"), ("k2", "char", None, ""), ("v", "date", "replace")]
            bitmap_index_list: 由3元组(index_name, column_name, index_type)组成的list
                               Example:[("k1_index", "k1", "BITMAP")]
            partition_info: PartitionInfo对象
            keys_desc: "AGGREGATE KEYS(k1)"
            dynamic_partition_info:创建动态分区用到的参数
            replication_allocation:设置副本的资源组
            enable_unique_key_merge_on_write:unique表是否开启merge-on-write数据更新模式，默认开启
        deprecated:
            storage_type:
            distribution:
        added:
            3种数据模型 aggregate，duplicate，unique
        """
        database_name = database_name if database_name is not None else self.database_name
        # table name
        sql = 'CREATE TABLE %s.%s (' % (database_name, table_name)
        # columns
        key_columns = list()
        aggregate_flag = False
        for column in column_list:
            sql = '%s %s,' % (sql, util.column_to_sql(column, set_null))
            if len(column) == 2 or column[2] is None:
                key_columns.append(column[0])
            if len(column) > 2 and column[2] is not None and column[2].upper() in \
                    ['MAX', 'MIN', 'SUM', 'REPLACE', 'HLL_UNION', 'REPLACE_IF_NOT_NULL']:
                aggregate_flag = True
        if bitmap_index_list is not None:
            for bitmap_index in bitmap_index_list:
                sql = '%s %s,' % (sql, util.bitmap_index_to_sql(bitmap_index))
        sql = '%s )' % sql.rstrip(',')
        if keys_desc is None:
            if aggregate_flag:
                keys_desc = 'AGGREGATE KEY(%s)' % ','.join(key_columns)
            else:
                keys_desc = ''
        sql = '%s %s' % (sql, keys_desc)
        # partition
        if partition_info:
            sql = '%s %s' % (sql, str(partition_info))
        # distribution
        if distribution_info is None:
            distribution_info = DistributionInfo('HASH(%s)' % column_list[0][0], 5)
        elif isinstance(distribution_info, DistributionInfo) and \
                distribution_info.distribution_type.upper() == 'RANDOM':
            distribution_info.distribution_type = 'HASH(%s)' % column_list[0][0]
        elif isinstance(distribution_info, str):
            distribution_info.replace(' RANDOM', 'HASH(%s)' % column_list[0][0])
            distribution_info.replace(' random', 'HASH(%s)' % column_list[0][0])
        else:
            pass
        sql = '%s %s' % (sql, str(distribution_info))
        # properties
        sql = '%s PROPERTIES (' % (sql)
        if storage_medium is not None:
            sql = '%s "storage_medium"="%s",' % (sql, storage_medium)
        if storage_cooldown_time is not None:
            sql = '%s "storage_cooldown_time"="%s",' % (sql, storage_cooldown_time)
        if bloom_filter_column_list is not None:
            sql = '%s "bloom_filter_columns"="%s",' % (sql, ",".join(bloom_filter_column_list))
        if replication_num is not None:
            sql = '%s "replication_num"="%s",' % (sql, replication_num)
        if dynamic_partition_info is not None:
            sql = '%s %s' % (sql, str(dynamic_partition_info))
        if replication_allocation is not None:
            sql = '%s "replication_allocation"="%s"' % (sql, replication_allocation)
        if light_schema_change is not None:
            sql = '%s "light_schema_change"="%s"' % (sql, light_schema_change)
        if enable_unique_key_merge_on_write is not None:
            sql = '%s "enable_unique_key_merge_on_write"="%s"' % (sql, enable_unique_key_merge_on_write)
        if sql.endswith(' PROPERTIES ('):
            sql = sql.rstrip(' PROPERTIES (')
        else:
            sql = '%s )' % (sql.rstrip(','))
        try:
            ret = self.execute(sql)
        except Exception as e:
            LOG.warning(L('CREATE TABLE fail.', table_name=table_name, msg=str(e)))
            if "Failed to find" in str(e) or \
                    "replication num should be less than the number of available backends" in str(e):
                alive_be = self.get_alive_backend_list()
                if len(alive_be) < 3:
                    LOG.warning(L('SKIP: some backends are dead, create table failed, skip test case '))
                    raise pytest.skip("some backends are dead, create table failed, skip test case")
                else:
                    raise e
            else:
                raise e
        if ret == ():
            LOG.info(L('CREATE TABLE succ.', database_name=database_name,
                       table_name=table_name))
            return True
        else:
            LOG.info(L('CREATE TABLE fail.', database_name=database_name,
                       table_name=table_name))
            return False

    def create_table_like(self, table_name, source_table_name, database_name=None,
                          source_database_name=None, rollup_list=None, external=False,
                          if_not_exists=False):
        """help create table like see more info"""
        sql = 'CREATE {external}TABLE {if_not_exists}{database_name}{table_name} LIKE ' \
              '{source_database_name}{source_table_name}{with_rollup}'
        external = 'EXTERNAL' if external else ''
        if_not_exists = 'IF NOT EXISTS ' if if_not_exists else ''
        database_name = database_name + '.' if database_name else ''
        with_rollup = ' ROLLUP (%s)' % ','.join(rollup_list) if rollup_list else ''
        source_database_name = source_database_name + '.' if source_database_name else ''
        sql = sql.format(external=external, if_not_exists=if_not_exists, database_name=database_name,
                         table_name=table_name, source_database_name=source_database_name,
                         source_table_name=source_table_name, with_rollup=with_rollup)
        ret = self.execute(sql)
        return ret == ()

    def create_rollup_table(self, table_name, rollup_table_name, column_name_list,
                            storage_type=None, database_name=None, base_index_name=None,
                            is_wait=False, force_alter=False, cluster_name=None, after_column_name_list=''):
        """
        Create a rollup table
        todo:storage_type
        """
        database_name = database_name if database_name is not None else self.database_name
        sql = 'ALTER TABLE %s.%s ADD ROLLUP %s (%s) %s' % (database_name, table_name,
                                                           rollup_table_name, ','.join(column_name_list),
                                                           after_column_name_list)
        if base_index_name:
            sql = "%s FROM %s" % (sql, base_index_name)
        if storage_type or force_alter:
            sql = '%s PROPERTIES(' % sql
            if storage_type:
                sql = '%s "storage_type"="column",' % sql
            if force_alter:
                sql = '%s "force_alter"="true",' % sql
            sql = '%s)' % sql.rstrip(',')

        ret = self.execute(sql)

        if ret != ():
            LOG.info(L("CREATE ROLLUP TABLE fail.", database_name=database_name,
                       table_name=table_name,
                       rollup_table_name=rollup_table_name))
            return False
        ret = True
        if is_wait:
            ret = self.wait_table_rollup_job(table_name, cluster_name=cluster_name,
                                             database_name=database_name)
            return ret
        LOG.info(L("CREATE ROLLUP TABLE succ.", database_name=database_name,
                   table_name=table_name,
                   rollup_table_name=rollup_table_name))
        return ret

    def cancel_rollup(self, table_name, database_name=None):
        """
        取消rollup
        """
        database_name = database_name if database_name is not None else self.database_name
        sql = 'CANCEL ALTER TABLE ROLLUP FROM %s.%s' % (database_name, table_name)
        ret = self.execute(sql)
        if ret == ():
            LOG.info(L("CANCEL ALTER ROLLUP succ.", database_name=database_name,
                       table_name=table_name))
            return True
        else:
            LOG.info(L("CANCEL ALTER ROLLUP fail.", database_name=database_name,
                       table_name=table_name))
            return False

    def create_materialized_view(self, table_name, materialized_view_name, view_sql,
                                 database_name=None, is_wait=False):
        """
        create_materialized_view

        :param table_name:
        :param materialized_view_name:
        :param view_sql:
        :param database_name:
        :param is_wait:
        :return:
        """
        database_name = database_name if database_name is not None else self.database_name
        sql = 'CREATE MATERIALIZED VIEW %s AS %s' % (materialized_view_name, view_sql)
        # before do it sleep 1s
        time.sleep(1)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L("CREATE MATERIALIZED VIEW fail.", database_name=database_name,
                       materialized_view_name=materialized_view_name))
            return False
        ret = True
        if is_wait:
            ret = self.wait_table_rollup_job(table_name, database_name=database_name)
            return ret
        LOG.info(L("CREATE MATERIALIZED VIEW succ.", database_name=database_name,
                   table_name=table_name, materialized_view_name=materialized_view_name, ))
        return ret

    def drop_materialized_view(self, database_name, table_name, view_name):
        """
        drop_materialized_view
        目前没有delete功能，rd开发中，采用alter table的方式删除
        :param database_name:
        :param table_name:
        :param view_name:
        :return:
        """
        sql = 'DROP MATERIALIZED VIEW IF EXISTS %s ON %s.%s' % (view_name, database_name, table_name)
        ret = self.execute(sql)
        return ret

    def get_index_list(self, table_name, database_name=None):
        """
        获取table的所有index
        """
        if not table_name:
            return None
        ret = self.desc_table(table_name, database_name, is_all=True)
        idx_list = util.get_attr(ret, palo_job.DescInfoAll.IndexName)
        while '' in idx_list:
            idx_list.remove('')
        return idx_list

    def get_index(self, table_name, index_name=None, database_name=None):
        """
        获取table的指定名字的Index，如无指定，返回默认创建的Index info
        """
        if not table_name:
            return None
        if index_name is None:
            index_name = table_name
        idx_list = self.get_index_list(table_name, database_name)
        if index_name in idx_list:
            return index_name
        else:
            LOG.info(L('can not get index from table', index_name=index_name, table_name=table_name))
            return None

    def get_index_schema(self, table_name, index_name=None, database_name=None):
        """
        Get table schema
        """
        if index_name is None:
            return self.desc_table(table_name, database_name, is_all=False)
        ret = self.desc_table(table_name, database_name, is_all=True)
        record = False
        return_ret = list()
        for item in ret:
            desc_column = palo_job.DescInfoAll(item)
            if record is False and desc_column.get_index_name() != index_name:
                continue
            elif desc_column.get_index_name() == index_name:
                record = True
                return_ret.append((desc_column.get_field(), desc_column.get_type(), desc_column.get_null(),
                                   desc_column.get_key(), desc_column.get_default(), desc_column.get_extra()))
            elif record is True:
                return_ret.append((desc_column.get_field(), desc_column.get_type(), desc_column.get_null(),
                                   desc_column.get_key(), desc_column.get_default(), desc_column.get_extra()))
            else:
                LOG.info(L('get index schema error'))
        return tuple(return_ret)

    def set_time_zone(self, zone, global_var=False):
        """设置系统的时区
          zone:要设置的时区,是否要global参数
        """
        if global_var:
            sql = "set global time_zone = '%s'" % zone
        else:
            sql = "set time_zone = '%s'" % zone
        LOG.info(L('palo sql', sql=sql))
        palo_res = self.execute(sql)

    def set_sql_mode(self, sql_mode=None):
        """set_sql_mode"""
        sql_mode = "PIPES_AS_CONCAT" if sql_mode is None else sql_mode
        sql = "set sql_mode = %s" % sql_mode
        LOG.info(L("palo sql", sql=sql))
        res = self.execute(sql)
        return res

    def get_sql_mode(self):
        """get sql mode"""
        sql = "select @@sql_mode"
        LOG.info(L("palo sql", sql=sql))
        p_res = self.execute(sql)
        return p_res

    def wait_load_job(self, load_label, database_name=None, cluster_name=None, timeout=1800):
        """
        wait load job
        """
        database_name = database_name if database_name is not None else self.database_name
        LOG.debug(L("wait load job.", load_label=load_label, database_name=database_name))
        retry_times = 0
        sql = 'SHOW LOAD FROM %s WHERE LABEL="%s"' % (database_name, load_label)
        while retry_times < 10 and timeout > 0:
            time.sleep(1)
            timeout -= 1
            load_job = self.execute(sql)
            if len(load_job) == 0:
                retry_times += 1
                continue
            state = palo_job.LoadJob(load_job[-1]).get_state()
            if state.upper() != "FINISHED" and state.upper() != "CANCELLED":
                continue
            elif state.upper() == "FINISHED":
                return True
            else:
                LOG.info(L("LOAD FAILED.", database_name=database_name,
                           msg=palo_job.LoadJob(load_job[-1]).get_errormsg(),
                           url=palo_job.LoadJob(load_job[-1]).get_url()))
                return False
        else:
            LOG.info(L("LOAD LABEL NOT EXIST", load_label=load_label, database_name=database_name))
            return False

    def bulk_load(self, table_name, load_label, data_file, max_filter_ratio=None,
                  column_name_list=None, timeout=None, database_name=None, host=None, port=None,
                  user=None, password=None, is_wait=False, cluster_name=None,
                  hll_column_list=None, column_separator=None, backend_id=None):
        """

        Args:
            table_name: string, table name
            load_label: string, load label
            data_file: string, 导入的文件
            max_filter_ratio: float, max_filter_ratio
            column_name_list: list, column name list
            timeout: int, timeout
            database_name: string, database name
            host: string, fe host
            port: int, fe http port
            user: string, fe user
            password: string, fe password
            is_wait: True/False, is wait
            cluster_name: string, cluster name
            hll_column_list: list, like ['hll_column1,k1', 'hll_column,k2']
            backend_id兼容以前的代码
        Returns:
            True: mini load success
            False: mini load Fail

        """
        database_name = database_name if database_name is not None else self.database_name
        host = self.host if host is None else host
        port = self.http_port if port is None else port
        user = self.user if user is None else user
        password = self.password if password is None else password
        url = 'http://%s:%s/api/%s/%s/_load?label=%s' % (host, port, database_name,
                                                         table_name, load_label)
        if max_filter_ratio:
            url = '%s&max_filter_ratio=%s' % (url, max_filter_ratio)
        if column_name_list:
            url = '%s&columns=%s' % (url, ','.join(column_name_list))
        if hll_column_list:
            url = '%s&hll=%s' % (url, ':'.join(hll_column_list))
        if timeout:
            url = '%s&timeout=%s' % (url, timeout)
        if column_separator:
            url = '%s&column_separator=%s' % (url, column_separator)
        cmd = 'curl --location-trusted -u %s:%s -T %s %s' % (user, password, data_file, url)
        print(cmd)
        LOG.info(L('bulk multi load', cmd=cmd))
        file = open(data_file)
        c = pycurl.Curl()
        buf = BytesIO()
        c.setopt(c.URL, url)
        c.setopt(pycurl.WRITEFUNCTION, buf.write)
        # basic认证
        c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
        c.setopt(pycurl.USERNAME, user)
        c.setopt(pycurl.PASSWORD, password)
        # 上传文件
        c.setopt(pycurl.UPLOAD, 1)
        c.setopt(pycurl.READDATA, file)
        # 重定向, --location-trusted
        c.setopt(pycurl.UNRESTRICTED_AUTH, 1)
        c.setopt(pycurl.FOLLOWLOCATION, True)
        LOG.info(L("BULK LOAD.", url=url, file_name=data_file))
        msg = None
        try:
            c.perform()
            msg = buf.getvalue()
            LOG.info(L("BULK LOAD ret.", ret=msg))
            print(c.getinfo(c.HTTP_CODE))
            # if c.getinfo(c.HTTP_CODE) == 200:
            if c.getinfo(c.HTTP_CODE):
                ret = json.loads(msg)
                status = ret.get('status')
                if status == 'Success':
                    if is_wait:
                        r = self.wait_load_job(load_label, database_name, cluster_name=cluster_name)
                        return r
                    else:
                        return True
                elif status == 'Fail':
                    return False
                else:
                    return False
        except Exception as e:
            LOG.info(L("BULK LOAD failed.", err=str(e), msg=msg))
        return False

    def batch_load(self, load_label, load_data_list, max_filter_ratio=None, timeout=None,
                   hadoop_info=None, by_load_cluster=None, property_list=None,
                   database_name=None, is_wait=False, cluster_name=None, broker=None,
                   strict_mode=None, timezone=None, temp_partition=None):
        """
        Load data
        Attributes:
            load_data_list: LoadDataInfo对象或list
        """
        database_name = self.database_name if database_name is None else database_name
        data_list = list()
        if isinstance(load_data_list, LoadDataInfo):
            data_list.append(str(load_data_list))
        elif isinstance(load_data_list, list):
            for data in load_data_list:
                data_list.append(str(data))
        else:
            raise PaloClientException('Load data list should be list or LoadDataInfo',
                                      load_data_list=load_data_list)
        sql = 'LOAD LABEL %s.%s (%s)' % (database_name, load_label,
                                         ', '.join(data_list))
        if by_load_cluster is not None:
            sql = '%s BY "%s"' % (sql, by_load_cluster)
        if broker is not None:
            sql = '%s %s' % (sql, str(broker))
        sql = '%s %s' % (sql, 'PROPERTIES(')
        if max_filter_ratio is not None:
            sql = '%s "max_filter_ratio"="%s",' % (sql, max_filter_ratio)
        if timeout is not None:
            sql = '%s "timeout"="%d",' % (sql, timeout)
        if strict_mode is not None:
            sql = '%s "strict_mode"="%s",' % (sql, strict_mode)
        if timezone is not None:
            sql = '%s "timezone"="%s",' % (sql, timezone)

        if hadoop_info is not None:
            sql = '%s %s' % (sql, str(hadoop_info))

        if property_list:
            sql = sql + ', '.join(property_list) + ','

        if sql.endswith(','):
            sql = sql.rstrip(',')
            sql = '%s %s' % (sql, ')')
        else:
            sql = sql.rstrip('PROPERTIES(')

        ret = self.execute(sql)
        if ret != ():
            LOG.info(L('LOAD fail.', database_name=database_name, load_label=load_label))
            return False
        if is_wait:
            ret = self.wait_load_job(load_label, database_name, cluster_name=cluster_name)
            if not ret:
                LOG.info(L('LOAD fail.', database_name=database_name, load_label=load_label))
                return False
        LOG.info(L('LOAD succ.', database_name=database_name, load_label=load_label))
        return True

    def cancel_load(self, load_label, database_name=None):
        """
        取消导入任务
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'CANCEL LOAD FROM %s WHERE LABEL = "%s"' % (database_name, load_label)
        ret = self.execute(sql)
        if ret == ():
            LOG.info(L("CANCEL LOAD succ.", database_name=database_name, label=load_label))
            return True
        else:
            LOG.info(L("CANCEL LOAD fail.", database_name=database_name, label=load_label))
            return False

    def get_quota(self, database_name, cluster_name=None):
        """
        get quota
        """
        if cluster_name:
            database_name = '%s:%s' % (cluster_name, database_name)
        sql = "SHOW PROC '/dbs'"
        result = self.execute(sql)
        for info in result:
            if info[1] == database_name:
                return info[3]
        LOG.warning(L("Get quota fail.", database_name=database_name))
        return None

    def alter_database(self, database_name, quota):
        """
        alter database
        """
        sql = "ALTER DATABASE %s SET DATA QUOTA %d" % (database_name, quota)
        result = self.execute(sql)
        if result != ():
            LOG.error(L("ALTER DATABASE fail.", database_name=database_name, quota=quota))
            return False
        return True

    def drop_database(self, database_name=None, force=None):
        """
        删除数据库
        Parameters：
            database_name：如果为None，将删除默认的database. Type: str.
        Returns:
            False：数据库删除失败
            True：数据库删除成功
        Raises:
            PaloClientException: 数据库删除异常
        """
        database_name = self.database_name if database_name is None else database_name
        if self.database_name == database_name:
            self.database_name = None
        sql = "DROP DATABASE %s" % database_name
        if force is True:
            sql = "DROP DATABASE %s FORCE" % database_name
        self.execute(sql)
        database_name_list = self.get_database_list()
        if database_name in database_name_list:
            LOG.warning(L("DROP DATABASE fail.", database_name=database_name))
            return False
        else:
            LOG.info(L("DROP DATABASE succ.", database_name=database_name))
            return True

    def clean(self, database_name=None):
        """
        清除所有数据
        Parameters：
            database_name：默认删除所有database。Type: str.
        Returns:
            None
        """
        if database_name is None:
            database_name_list = self.get_database_list()
            for database_name in database_name_list:
                if database_name.find("information_schema") == -1:
                    self.clean(database_name)
        else:
            database_name_list = self.get_database_list()
            while database_name in database_name_list:
                try:
                    self.drop_database(database_name, force=True)
                except PaloClientException:
                    pass
                database_name_list = self.get_database_list()

    def drop_table(self, table_name, database_name=None, cluster_name=None, if_exist=False):
        """删除table family
        Parameters：
            database_name：如果为None，将删除默认的database. [Type str]
            table_family_name: table family name. [Type str]
        Returns:
            目前无返回值
        Raises:
            没有捕获，可能抛出
        """
        database_name = self.database_name if database_name is None else database_name
        if not if_exist:
            sql = 'DROP TABLE'
        else:
            sql = 'DROP TABLE IF EXISTS'
        sql = "%s %s.%s" % (sql, database_name, table_name)
        self.execute(sql)

        return True

    def drop_rollup_table(self, table_name, rollup_table_name, database_name=None):
        """删除rollup table
        Parameters：
            database_name：如果为None，将删除默认的database. [Type str]
            table_family_name: table family name. [Type str]
        Returns:
            目前无返回值
        Raises:
            没有捕获，可能抛出
        """
        database_name = self.database_name if database_name is None else database_name
        sql = "ALTER TABLE %s.%s DROP ROLLUP %s" % \
              (database_name, table_name, rollup_table_name)
        ret = self.execute(sql)
        return ret == ()

    def select_all(self, table_name, database_name=None):
        """
        select all
        """
        database_name = self.database_name if database_name is None else database_name
        sql = "SELECT * FROM %s.%s" % (database_name, table_name)
        result = self.execute(sql)
        return result

    def query(self, sql):
        """
        query
        """
        result = self.execute(sql)
        return result

    def get_load_job_state(self, label, database_name=None, cluster_name=None):
        """
        get load job state
        """
        load_job_list = self.get_load_job_list(database_name=database_name, \
                                               cluster_name=cluster_name)
        for load_job in load_job_list:
            job = palo_job.LoadJob(load_job)
            if job.get_label() == label:
                return job.get_state()
        return None

    def get_unfinish_load_job_list(self, database_name=None, cluster_name=None):
        """
        获取所有未完成的导入任务，即：状态为pending、etl、loading的任务
        """
        load_job_list = self.get_load_job_list( \
            database_name=database_name, cluster_name=cluster_name)
        result = list()
        for load_job in load_job_list:
            job = palo_job.LoadJob(load_job)
            if job.get_state() != "FINISHED" and job.get_state() != "CANCELLED":
                result.append(load_job)

        return result

    def get_load_job_list(self, state=None, database_name=None, cluster_name=None):
        """
        获取指定状态的导入任务信息, according job state get load job
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'SHOW LOAD FROM %s' % database_name
        job_list = self.__execute_and_rebuild_meta_class(sql, palo_job.LoadJob)
        result = list()
        if state:
            state = state.upper()
            for job in job_list:
                if palo_job.LoadJob(job).get_state() == state:
                    result.append(job)
            return result
        else:
            return job_list

    def get_load_job(self, label, database_name=None, cluster_name=None):
        """
        get load job, according label get load job
        """
        load_job_list = self.get_load_job_list(database_name=database_name, \
                                               cluster_name=cluster_name)
        for load_job in load_job_list:
            if palo_job.LoadJob(load_job).get_label() == label:
                return load_job
        return None

    def get_delete_job_list(self, state=None, database_name=None, cluster_name=None):
        """
        获取指定状态的delete job信息
        """
        database_name = self.database_name if database_name is None else database_name
        if not cluster_name:
            cluster_name = self.cluster_name
        if cluster_name:
            database_name = '%s:%s' % (cluster_name, database_name)
        sql = 'SHOW DELETE FROM %s' % database_name
        job_list = self.execute(sql)
        result = list()
        if state:
            state = state.upper()
            for job in job_list:
                if palo_job.DeleteJob(job).get_state() == state:
                    result.append(job)
            return result
        else:
            return job_list

    def wait_table_rollup_job(self, table_name, database_name=None, cluster_name=None,
                              state='FINISHED', timeout=2000):
        """
        等待rollup完成
        """
        time.sleep(5)
        database_name = self.database_name if database_name is None else database_name
        retry_times = 10
        while timeout > 0:
            job_list = self.get_table_rollup_job_list(table_name,
                                                      database_name, cluster_name=cluster_name)
            if not job_list:
                retry_times -= 1
                if retry_times == 0:
                    LOG.info(L("CANNOT GET ROLLUP JOB.", database_name=database_name))
                    break
                time.sleep(1)
                continue
            last_job_state = palo_job.RollupJob(job_list[-1]).get_state()
            if last_job_state == state:
                LOG.info(L("GET ROLLUP JOB STATE.", database_name=database_name,
                           state=palo_job.RollupJob(job_list[-1]).get_state()))
                return True
            if last_job_state == 'CANCELLED':
                LOG.info(L("ROLLUP JOB CANCELLED.", database_name=database_name,
                           msg=palo_job.RollupJob(job_list[-1]).get_msg()))
                return False
            if state != 'FINISHED' and last_job_state == 'FINISHED':
                LOG.info(L("ROLLUP JOB FINISHED.", database_name=database_name))
                return False
            time.sleep(3)
            timeout -= 3
        LOG.warning(L("WAIT ROLLUP JOB TIMEOUT.", database_name=database_name))
        return False

    def wait_table_load_job(self, database_name=None, timeout=1800):
        """等待db的所有load任务完成"""
        database_name = self.database_name if database_name is None else database_name
        flag = False
        while timeout > 0:
            flag = True
            load_job_list = self.execute('SHOW LOAD FROM %s' % database_name)
            for job in load_job_list:
                LOG.info(L("LOAD JOB STATE.", database_name=database_name,
                           state=job[palo_job.LoadJob.State]))
                if job[palo_job.LoadJob.State] != 'CANCELLED' and \
                        job[palo_job.LoadJob.State] != 'FINISHED':
                    LOG.info(L("LOAD JOB RUNNING.", database_name=database_name,
                               state=job[palo_job.LoadJob.State]))
                    flag = False
                    break
            time.sleep(3)
            timeout -= 3
            if flag:
                LOG.info(L("LOAD JOB FINISHED.", database_name=database_name))
                break
        LOG.info(L("WAIT LOAD JOB FINISHED.", database_name=database_name))

    def get_table_rollup_job_list(self, table_name, database_name=None, cluster_name=None):
        """
        获取指定table family的rollup任务信息
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'SHOW ALTER TABLE ROLLUP FROM %s WHERE TableName = "%s"' % (database_name, table_name)
        database_rollup_job_list = self.__execute_and_rebuild_meta_class(sql, palo_job.RollupJob)
        table_rollup_job_list = []
        for rollup_job in database_rollup_job_list:
            if palo_job.RollupJob(rollup_job).get_table_name() == table_name:
                table_rollup_job_list.append(rollup_job)
        return table_rollup_job_list

    def get_database_rollup_job_list(self, database_name=None, cluster_name=None):
        """
        获取database的所有rollup job信息
        """
        database_name = self.database_name if database_name is None else database_name
        if cluster_name:
            database = '%s:%s' % (cluster_name, database_name)

        sql = "SHOW PROC '/jobs/%s/rollup'" % self.get_database_id(database_name)
        return self.execute(sql)

    def get_database_list(self, cluster_name=None):
        """
        显示所有的database name list
        Parameters：
            None
        Returns：
            database name list
        Raises：
            PaloClientException：获取数据异常
        """
        sql = r"SHOW DATABASES"
        result = self.execute(sql)
        database_name_list = [name[0] for name in result]
        return database_name_list

    def get_partition_list(self, table_name, database_name=None, cluster_name=None):
        """
        get table families
        """
        database_name = self.database_name if database_name is None else database_name
        if cluster_name:
            database_name = '%s:%s' % (cluster_name, database_name)
        sql = "SHOW PARTITIONS FROM %s.%s" % (database_name, table_name)
        result = self.__execute_and_rebuild_meta_class(sql, palo_job.PartitionInfo)
        return result

    def get_partition(self, table_name, partition_name, database_name=None, cluster_name=None):
        """
        获取指定的table family
        """
        partition_list = self.get_partition_list(table_name, database_name, \
                                                 cluster_name=cluster_name)
        for partition in partition_list:
            if partition[palo_job.PartitionInfo.PartitionName] == partition_name:
                return partition
        return None

    def get_partition_id(self, table_name, partition_name, database_name=None):
        """
        get table family id.
        Parameters：
            database_name
            table_family_name
        Returns:
            None：如果table family不存在
        """
        partition = self.get_partition(table_name, \
                                       partition_name, database_name)
        if partition:
            return partition[palo_job.PartitionInfo.PartitionId]
        else:
            return None

    def get_partition_name_by_id(self, table_name, partition_id, \
                                 database_name=None, cluster_name=None):
        """get partition name by id
        """
        partition_list = self.get_partition_list(table_name, \
                                                 database_name, cluster_name=cluster_name)
        for partition in partition_list:
            if partition[palo_job.PartitionInfo.PartitionId] == partition_id:
                return partition[palo_job.PartitionInfo.PartitionName]
        return None

    def get_partition_version(self, table_name, \
                              partition_name, database_name=None, cluster_name=None):
        """
        获取table family的version号
        """
        partition = self.get_partition(table_name, \
                                       partition_name, database_name, cluster_name=cluster_name)
        if partition:
            return partition[palo_job.PartitionInfo.VisibleVersion]
        else:
            return None

    def get_partition_storage_medium(self, table_name, partition_name, \
                                     database_name=None, cluster_name=None):
        """
        get table family id.
        Parameters：
            database_name
            table_family_name
        Returns:
            None：如果table family不存在
        """
        partition = self.get_partition(table_name, \
                                       partition_name, database_name, cluster_name=cluster_name)
        if partition:
            return partition[palo_job.PartitionInfo.StorageMedium]
        else:
            return None

    def get_partition_cooldown_time(self, table_name, partition_name, \
                                    database_name=None, cluster_name=None):
        """
        get table family id.
        Parameters：
            database_name
            table_family_name
        Returns:
            None：如果table family不存在
        """
        partition = self.get_partition(table_name,
                                       partition_name, database_name, cluster_name=cluster_name)
        if partition:
            return partition[palo_job.PartitionInfo.CooldownTime]
        else:
            return None

    def get_partition_replication_num(self, table_name, partition_name,
                                      database_name=None, cluster_name=None):
        """
        get table family id.
        Parameters：
            database_name
            table_family_name
        Returns:
            None：如果table family不存在
        """
        partition = self.get_partition(table_name,
                                       partition_name, database_name, cluster_name=cluster_name)
        if partition:
            return partition[palo_job.PartitionInfo.ReplicationNum]
        else:
            return None

    def get_partition_buckets(self, table_name, partition_name,
                              database_name=None, cluster_name=None):
        """
        get table family id.
        Parameters：
            database_name
            table_family_name
        Returns:
            None：如果table family不存在
        """
        partition = self.get_partition(table_name, partition_name,
                                       database_name, cluster_name=cluster_name)
        if partition:
            return partition[palo_job.PartitionInfo.Buckets]
        else:
            return None

    def delete(self, table_name, delete_conditions,
               partition_name=None, database_name=None, is_wait=False):
        """
        按条件删除指定 table family 中的数据
        Parameters:
            delete_conditions：可以是删除条件组成的list，每个删除条件是一个三元组，如下。也可以是字符串
                由列名，比较运算符和值三项组成。如：
                [("k1", "=", "0"), ("k2", "<=", "10")]
        Returns:
            True：执行成功
            False：执行失败
        Raises：
        """
        database_name = self.database_name if database_name is None else database_name
        if not partition_name:
            sql = "DELETE FROM %s.%s WHERE" % (database_name, table_name)
        else:
            sql = "DELETE FROM %s.%s PARTITION %s WHERE" % (database_name,
                                                            table_name, partition_name)
        if isinstance(delete_conditions, list):
            for where_condition in delete_conditions:
                # 对value项加上引号, 防止datetime因空格分隔造成错误
                where_condition = list(where_condition)
                where_condition[2] = "\"%s\"" % where_condition[2]
                sql = "%s %s AND" % (sql, " ".join(where_condition))
            sql = sql[:-4]
        elif isinstance(delete_conditions, str):
            sql = "%s %s" % (sql, delete_conditions)
        # 同步的，返回时，已删除，未返回时，其他连接show delete状态为DELETING状态
        time.sleep(1)
        ret = self.execute(sql)
        LOG.info(L("DELETE EXECUTE SUCC", ret=ret))
        if is_wait is True:
            if database_name is not None:
                line = 'SHOW DELETE FROM %s' % database_name
            else:
                line = 'SHOW DELETE'
            delete_job = self.execute(line)
            for job in delete_job:
                if job[4] == 'FINISHED':
                    pass
                else:
                    time.sleep(1)
        return ret == ()

    def show_delete(self, database_name=None):
        """show delete """
        if not database_name:
            database_name = self.database_name
        sql = 'SHOW DELETE FROM %s' % database_name
        ret = self.execute(sql)
        return ret

    def schema_change(self, table_family_name, add_column_list=None,
                      drop_column_list=None, modify_column_list=None, order_column_list=None,
                      bloom_filter_column_list=None, database_name=None,
                      colocate_with_list=None, distribution_type=None,
                      is_wait=False, cluster_name=None, comment=None, replication_allocation=None):
        """schema change
        Parameters:
            table_family_name:
            rollup_table_name:
            add_column_list: 新加列，每个元素为一个新加列
                "列名 类型 聚合方法 default '默认值' after '列名' in 'rollup表名'"
                e.g. ["addc1 int default '1'", \
                        "addc2 float sum default '2.3'", \
                        "addc3 int default '2' after k1", \
                        "addc1 int default '1' in 'rollup_table'"]
            drop_column_list: 删除列，每个元素为一个删除的列名
                e.g. ["k1", "v"]
            modify_column_list: 修改列属性，每个元素为一个列, 参考add_column_list
            order_column_list: 调整列顺序， 每个元素为一个删除的列名
            bloom_filter_column_list:
            database_name:
            colocate_with_list:
            distribution_type:
            is_wait:
            cluster_name:
            comment:表注释
            replication_allocation:副本标签
        """
        database_name = self.database_name if database_name is None else database_name
        sql = "ALTER TABLE %s.%s" % (database_name, table_family_name)
        if add_column_list:
            if len(add_column_list) == 1:
                sql = "%s ADD COLUMN %s" % (sql, ", ".join(add_column_list))
            else:
                sql = "%s ADD COLUMN (%s)" % (sql, ", ".join(add_column_list))
        if drop_column_list:
            sql = "%s DROP COLUMN %s" % (sql, ", DROP COLUMN ".join(drop_column_list))
        if order_column_list:
            if len(order_column_list) == 1:
                sql = "%s ORDER BY %s" % (sql, ", ".join(order_column_list))
            else:
                sql = "%s ORDER BY (%s)" % (sql, ", ".join(order_column_list))
        if modify_column_list:
            sql = "%s MODIFY COLUMN %s" % (sql, ", MODIFY COLUMN ".join(modify_column_list))
        if bloom_filter_column_list:
            if add_column_list is None and drop_column_list is None \
                    and modify_column_list is None and order_column_list is None:
                sql = '%s SET ("bloom_filter_columns"="%s")' % (sql, ",".join(bloom_filter_column_list))
            else:
                sql = '%s PROPERTIES ("bloom_filter_columns"="%s")' % (sql, ",".join(bloom_filter_column_list))
        if colocate_with_list:
            sql = '%s SET ("colocate_with"="%s")' % (sql, ",".join(colocate_with_list))
        if distribution_type:
            sql = '%s SET ("distribution_type"="%s")' % (sql, ",".join(distribution_type))
        if comment:
            sql = '%s MODIFY comment "%s"' % (sql, comment)
        if replication_allocation:
            sql = '%s SET ("replication_allocation"="%s")' % (sql, replication_allocation)
        result = self.execute(sql)
        if result != ():
            LOG.info(L("SCHEMA CHANGE fail.", database_name=database_name,
                       table_family_name=table_family_name))
            return False
        if is_wait:
            LOG.info(L("wait for SCHEMA CHANGE.", database_name=database_name,
                       table_family_name=table_family_name))
            if not self.wait_table_schema_change_job(table_family_name, cluster_name=cluster_name):
                LOG.info(L("SCHEMA CHANGE fail.", database_name=database_name,
                           table_family_name=table_family_name))
                return False
        return True

    def schema_change_add_column(self, table_name, column_list, after_column_name=None,
                                 to_table_name=None, force_alter=False, database_name=None,
                                 is_wait_job=False, is_wait_delete_old_schema=False,
                                 cluster_name=None, set_null=False):
        """
        增加列
        column_list: column_name, column_type, aggtype, default_value
        same to function create table family
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s ADD COLUMN' % (database_name, table_name)
        if len(column_list) > 1:
            sql = '%s(' % sql
        for column in column_list:
            sql = '%s %s, ' % (sql, util.column_to_sql(column, set_null))
        sql = sql.rstrip(', ')
        if len(column_list) > 1:
            sql = '%s)' % sql

        if after_column_name:
            if after_column_name == 'FIRST':
                sql = '%s %s' % (sql, after_column_name)
            else:
                sql = '%s AFTER %s' % (sql, after_column_name)
        if to_table_name:
            sql = '%s TO %s' % (sql, to_table_name)

        if force_alter:
            sql = '%s PROPERTIES("force_alter"="true")' % sql

        result = self.execute(sql)

        if result != ():
            LOG.info(L("SCHEMA CHANGE fail.", database_name=database_name,
                       table_name=table_name))
            return False

        if is_wait_job:
            if not self.wait_table_schema_change_job(table_name, cluster_name=cluster_name):
                return False

        return True

    def schema_change_drop_column(self, table_name, column_name_list,
                                  from_table_name=None, force_alter=False, database_name=None,
                                  is_wait_job=False, is_wait_delete_old_schema=False, cluster_name=None):
        """
        删除列
        column_name_list: ['k1', 'v1', 'v3']
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s' % (database_name, table_name)

        from_table_sql = ''
        if from_table_name:
            from_table_sql = 'FROM %s' % (from_table_name)

        force_alter_sql = ''
        if force_alter:
            force_alter_sql = 'PROPERTIES("force_alter"="true")'

        for column in column_name_list:
            sql = '%s DROP COLUMN %s %s %s, ' % (sql, column, from_table_sql, force_alter_sql)

        sql = sql.rstrip(', ')

        result = self.execute(sql)

        if result != ():
            LOG.info(L("SCHEMA CHANGE fail.", database_name=database_name,
                       table_name=table_name))
            return False

        if is_wait_job:
            if not self.wait_table_schema_change_job(table_name, cluster_name=cluster_name):
                return False

        return True

    def schema_change_order_column(self, table_name, column_name_list,
                                   from_table_name=None, force_alter=False, database_name=None,
                                   is_wait_job=False, is_wait_delete_old_schema=False, cluster_name=None):
        """
        重新排序
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s' % (database_name, table_name)

        from_table_sql = ''
        if from_table_name:
            from_table_sql = 'FROM %s' % (from_table_name)

        force_alter_sql = ''
        if force_alter:
            force_alter_sql = 'PROPERTIES("force_alter"="true")'

        sql = '%s ORDER BY (%s)' % (sql, ', '.join(column_name_list))

        sql = '%s %s %s' % (sql, from_table_sql, force_alter_sql)

        result = self.execute(sql)

        if result != ():
            LOG.info(L("SCHEMA CHANGE fail.", database_name=database_name,
                       table_name=table_name))
            return False

        if is_wait_job:
            if not self.wait_table_schema_change_job(table_name, cluster_name=cluster_name):
                return False

        return True

    def schema_change_modify_column(self, table_name, column_name, column_type,
                                    after_column_name=None, from_table_name=None, force_alter=False,
                                    database_name=None, is_wait_job=False,
                                    is_wait_delete_old_schema=False, cluster_name=None, aggtype='', column_info=""):
        """
        修改列类型
        """
        database_name = self.database_name if database_name is None else database_name
        schema = self.desc_table(table_name, database_name=database_name)
        if not aggtype:
            for field in schema:
                if field[0] == column_name and field[3] == 'false':
                    aggtype = field[5].split(',')[0]
                    break
        if aggtype == '-' or aggtype == 'NONE':
            aggtype = ''

        sql = 'ALTER TABLE %s.%s MODIFY COLUMN %s %s %s %s' % (database_name,
                                                               table_name, column_name, column_type, aggtype,
                                                               column_info)

        if after_column_name:
            sql = '%s AFTER %s' % (sql, after_column_name)

        if from_table_name:
            sql = '%s FROM %s' % (sql, from_table_name)

        if force_alter:
            sql = '%s PROPERTIES("force_alter"="true")' % sql
        result = self.execute(sql)

        if result != ():
            LOG.info(L("SCHEMA CHANGE fail.", database_name=database_name,
                       table_name=table_name))
            return False

        if is_wait_job:
            if not self.wait_table_schema_change_job(table_name, cluster_name=cluster_name):
                return False

        return True

    def wait_table_schema_change_job(self, table_name, database_name=None, cluster_name=None,
                                     state="FINISHED", timeout=1200):
        """
        等待schema change完成
        """
        database_name = self.database_name if database_name is None else database_name
        try_times = 0
        while try_times < 120 and timeout > 0:
            time.sleep(3)
            timeout -= 3
            schema_change_job_list = self.get_table_schema_change_job_list(
                table_name, database_name, cluster_name=cluster_name)
            if not schema_change_job_list or len(schema_change_job_list) == 0:
                try_times += 1
                continue
            last_job_state = palo_job.SchemaChangeJob(schema_change_job_list[-1]).get_state()
            LOG.info(L("GET LAST SCHEMA CHANGE JOB STATE", state=last_job_state))
            if last_job_state == state:
                LOG.info(L("GET SCHEMA CHANGE JOB STATE.", database_name=database_name,
                           state=palo_job.SchemaChangeJob(schema_change_job_list[-1]).get_state()))
                return True
            if last_job_state == 'CANCELLED' and state != 'CANCELLED':
                LOG.info(L("SCHEMA CHANGE fail.", state='CANCELLED',
                           msg=palo_job.SchemaChangeJob(schema_change_job_list[-1]).get_msg()))
                return False
            if state != 'FINISHED' and last_job_state == 'FINISHED':
                LOG.info(L("SCHEMA CHANGE FINISHED.", state='FINISHED'))
                return False
        LOG.warning(L("WAIT SCHEMA CHANGE TIMEOUT.", database_name=database_name))
        return False

    def get_table_schema_change_job_list(self, table_name, database_name=None, cluster_name=None):
        """
        获取指定table的所有schema change job信息
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'SHOW ALTER TABLE COLUMN FROM %s' % database_name
        database_schema_change_job_list = self.__execute_and_rebuild_meta_class(sql, palo_job.SchemaChangeJob)
        table_schema_change_job_list = []
        for schema_change_job in database_schema_change_job_list:
            if palo_job.SchemaChangeJob(schema_change_job).get_table_name() == table_name:
                table_schema_change_job_list.append(schema_change_job)
        return table_schema_change_job_list

    def cancel_schema_change(self, table_name, database_name=None):
        """
        取消rollup
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'CANCEL ALTER TABLE COLUMN FROM %s.%s' % (database_name, table_name)
        ret = self.execute(sql)
        if ret == ():
            LOG.info(L("CANCEL SCHEMA CHANGE Succ.", database_name=database_name,
                       table_name=table_name))
            return True
        else:
            LOG.info(L("CANCEL SCHEMA CHANGE Fail.", database_name=database_name,
                       table_name=table_name))
            return False

    def add_partition(self, table_name, partition_name, value,
                      distribute_type=None, bucket_num=None,
                      storage_medium=None, storage_cooldown_time=None,
                      database_name=None, partition_type=None):
        """
        增加分区
        增加分区的时候，只支持默认的表的分桶方式，不支持其他新的分桶方式,可修改bucket数量
        Args:
            table_name: table name
            partition_name: new partition name
            value: str or tuple, like: 'k1' or ('k1', 'k2'), (('1','2'),('3','4'))
            distribute_type: only hash, do not support random
            bucket_num: num
            storage_medium:
            storage_cooldown_time:
            database_name:

        Returns:
            True/except
        """
        database_name = self.database_name if database_name is None else database_name
        predicate = 'IN' if partition_type is not None and partition_type.upper() == 'LIST' else 'LESS THAN'
        if value != 'MAXVALUE':
            # 单列list or 多列range 
            if isinstance(value, tuple) and isinstance(value[0], str):
                value = '(%s)' % ','.join('"{0}"'.format(v) for v in value)
                value = value.replace('"MAXVALUE"', 'MAXVALUE')
            # 多列list 
            elif isinstance(value, tuple) and isinstance(value[0], tuple):
                in_val_list = []
                for multi_col_value in value:
                    in_val_list.append('(%s)' % ','.join('"{0}"'.format(v) for v in multi_col_value))
                value = '(%s)' % ','.join(in_val_list)
            # 单列range
            else:
                value = '("%s")' % (value)
        sql = 'ALTER TABLE %s.%s ADD PARTITION %s VALUES %s %s' % (
            database_name, table_name, partition_name, predicate, value)
        sql = '%s (' % (sql)
        if storage_medium is not None:
            sql = '%s "storage_medium"="%s",' % (sql, storage_medium)
        if storage_cooldown_time is not None:
            sql = '%s "storage_cooldown_time"="%s",' % (sql, storage_cooldown_time)
        if sql.endswith(' ('):
            sql = sql.rstrip(' (')
        else:
            sql = '%s )' % (sql.rstrip(','))
        if distribute_type:
            if distribute_type.upper() == 'RANDOM':
                discol = self.execute('desc %s.%s' % (database_name, table_name))[0][0]
                sql = '%s DISTRIBUTED BY HASH(%s)' % (sql, discol)
            else:
                sql = '%s DISTRIBUTED BY %s' % (sql, distribute_type)
        if bucket_num:
            sql = '%s BUCKETS %d' % (sql, bucket_num)

        result = self.execute(sql)

        if result != ():
            LOG.info(L("ADD PARTITION fail.", database_name=database_name,
                       table_name=table_name))
            return False

        return True

    def modify_partition(self, table_name, partition_name=None,
                         storage_medium=None, storage_cooldown_time=None, replication_num=None,
                         database_name=None, **kwargs):
        """
        修改分区的 storage_medium、storage_cooldown_time 和 replication_num 三个属性。
        对于单分区表，partition_name 同表名
        如果partition_name为none则为修改全表属性
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s' % (database_name, table_name)
        if partition_name is not None:
            if isinstance(partition_name, list):
                partition_name = '(' + ','.join(partition_name) + ')'
            sql = '%s MODIFY PARTITION %s' % (sql, partition_name)
        if storage_medium is not None:
            kwargs['storage_medium'] = storage_medium
        if storage_cooldown_time is not None:
            kwargs['storage_cooldown_time'] = storage_cooldown_time
        if replication_num is not None:
            kwargs['replication_num'] = replication_num
        property = ''
        for k, v in kwargs.items():
            property += ' "%s" = "%s",' % (k, v)
        property_str = property.strip(',')
        sql = '%s SET(%s)' % (sql, property_str)
        result = self.execute(sql)

        if result != ():
            LOG.info(L("MODIFY PARTITION fail.", database_name=database_name,
                       table_name=table_name))
            return False

        return True

    def drop_partition(self, table_name, partition_name, database_name=None):
        """
        删除分区
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s DROP PARTITION %s' % ( \
            database_name, table_name, partition_name)

        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L('', fe=str(self), error=e))
            return False

        return True

    def add_temp_partition(self, table_name, partition_name, value, distribute_type=None, bucket_num=None,
                           in_memory=None, replication_num=None, database_name=None, partition_type='RANGE'):
        """
        新建临时分区
        :param table_name:
        :param partition_name:
        :param value:
        :param distribute_type:
        :param bucket_num:
        :param in_memory:
        :param replication_num:
        :param database_name:
        :return:
        """
        database_name = self.database_name if database_name is None else database_name
        partitioninfo = PartitionInfo(partition_type=partition_type).get_partition_value(value)
        print(partitioninfo)
        sql = 'ALTER TABLE %s.%s ADD TEMPORARY PARTITION %s %s' % (database_name, table_name,
                                                                   partition_name, partitioninfo)
        sql = '%s (' % (sql)
        if in_memory is not None:
            sql = '%s "in_memory"="%s",' % (sql, in_memory)
        if replication_num is not None:
            sql = '%s "replication_num"="%s",' % (sql, replication_num)
        if sql.endswith(' ('):
            sql = sql.rstrip(' (')
        else:
            sql = '%s )' % (sql.rstrip(','))
        if distribute_type:
            if distribute_type.upper() == 'RANDOM':
                discol = self.execute('desc %s.%s' % (database_name, table_name))[0][0]
                sql = '%s DISTRIBUTED BY HASH(%s)' % (sql, discol)
            else:
                sql = '%s DISTRIBUTED BY %s' % (sql, distribute_type)
        if bucket_num:
            sql = '%s BUCKETS %d' % (sql, bucket_num)

        result = self.execute(sql)

        if result != ():
            LOG.info(L("ADD TEMP PARTITION fail.", database_name=database_name,
                       table_name=table_name))
            return False

        return True

    def drop_temp_partition(self, database_name, table_name, partition_name):
        """
        删除临时分区
        :param database_name:
        :param table_name:
        :param partition_name:
        :return:
        """
        self.use(database_name)
        sql = 'ALTER TABLE %s DROP TEMPORARY PARTITION %s' % (table_name, partition_name)

        result = self.execute(sql)

        if result != ():
            LOG.info(L("DROP TEMP PARTITION fail.", database_name=database_name,
                       table_name=table_name))
            return False

        return True

    def modify_temp_partition(self, database_name, table_name, target_partition_list, temp_partition_list,
                              strict_range=None, use_temp_partition_name=None):
        """
        修改临时分区
        :param database_name:
        :param table_name:
        :param target_partition_list:
        :param temp_partition_list:
        :param strict_range:
        :param use_temp_partition_name:
        :return:
        """
        self.use(database_name)
        target_partition = ','.join(target_partition_list)
        temp_partition = ','.join(temp_partition_list)
        sql = 'ALTER TABLE %s.%s REPLACE PARTITION (%s) WITH TEMPORARY PARTITION (%s)' % \
              (database_name, table_name, target_partition, temp_partition)

        if strict_range is not None or use_temp_partition_name is not None:
            sql = "%s PROPERTIES (" % sql
            value_list = []
            print(strict_range)
            if strict_range:
                value_list.append('"strict_range"="%s"' % strict_range)
            if use_temp_partition_name:
                value_list.append('"use_temp_partition_name"="%s"' % use_temp_partition_name)
            print(value_list)
            sql = "%s %s)" % (sql, ','.join(value_list))

        result = self.execute(sql)

        if result != ():
            LOG.info(L("ALTER TEMP PARTITION fail.", database_name=database_name,
                       table_name=table_name))
            return False

        return True

    def create_user(self, user, password=None, is_superuser=False, default_role=None):
        """
        create user
        """
        sql = "CREATE USER '%s'" % user
        if password is not None:
            sql = "%s IDENTIFIED BY '%s'" % (sql, password)
        if is_superuser:
            sql = "%s SUPERUSER" % sql
        if default_role:
            sql = "%s DEFAULT ROLE '%s'" % (sql, default_role)
        result = self.execute(sql)
        if result != ():
            LOG.error(L("CREATE USER fail.", user=user, password=password, \
                        is_superuser=is_superuser))
            return False
        return True

    def create_role(self, role_name):
        """
        create role
        """
        sql = 'CREATE ROLE %s' % role_name
        result = self.execute(sql)
        if result != ():
            LOG.error(L("CREATE ROLE fail.", sql=sql, msg=str(result)))
            return False
        return True

    def drop_role(self, role):
        """drop role"""
        sql = "DROP ROLE %s" % role
        result = self.execute(sql)
        if result != ():
            LOG.error(L("DROP ROLE fail", msg=str(result), role=role))
            return False
        return True

    def drop_user(self, user, if_exists=True):
        """
        drop user
        """
        if if_exists:
            sql = "DROP USER IF EXISTS '%s'" % user
        else:
            sql = "DROP USER '%s'" % user
        result = self.execute(sql)
        if result != ():
            LOG.error(L("DROP USER fail.", user=user))
            return False
        return True

    def clean_user(self, user):
        """clean user
        """
        try:
            self.drop_user(user)
        except:
            pass

    def set_password(self, user=None, password=None):
        """
        set password of user to password
        """
        if user is None:
            user = self.user
        if password is None:
            password = ''
        sql = "SET PASSWORD FOR '%s' = PASSWORD('%s')" % (user, password)

        result = self.execute(sql)
        if result != ():
            LOG.error(L("SET PASSWORD fail.", user=user, password=password))
            return False
        return True

    def alter_user(self, user, cpu_share=None, max_user_connections=None):
        """
        alter user
        """
        sql = "ALTER USER '%s'" % user
        if cpu_share is not None:
            sql = "%s MODIFY RESOURCE CPU_SHARE %d," % (sql, cpu_share)
        if max_user_connections is not None:
            sql = "%s MODIFY PROPERTY MAX_USER_CONNECTIONS %d," % (sql, max_user_connections)
        sql = sql.rstrip(",")
        result = self.execute(sql)
        if result != ():
            LOG.error(L("ALTER USER fail.", user=user, cpu_share=cpu_share,
                        max_user_connections=max_user_connections))
            return False
        return True

    def get_cpu_share(self, user):
        """
        get cpu share
        """
        result = self.show_resource(user)
        for info in result:
            if info[0] == user and info[1] == "CPU_SHARE":
                return int(info[2])
        return None

    def show_resource(self, user):
        """
        show resource
        """
        sql = "SHOW RESOURCE LIKE '%s'" % user
        result = self.execute(sql)
        if result == ():
            LOG.warning(L("SHOW RESOURCE fail.", user=user))
        return result

    def grant(self, user, privilege_list, database='', table='', catalog='',
              resource=None, is_role=False, identity='%'):
        """
        grant
        GRANT privilege_list ON grant_obj TO [user_identity] | [ROLE role_name]
        GRANT privilege_list ON RESOURCE grant_obj TO [user_identity] | [ROLE role_name]
        """
        sql = "GRANT {privilege_list} ON {grant_obj} TO {role_desc}{user}"
        if isinstance(privilege_list, list):
            privilege_list = ', '.join(privilege_list)
        if is_role:
            role_desc = "ROLE "
            user = '"%s"' % user
        else:
            role_desc = ''
            user = '"%s"@"%s"' % (user, identity)
        if resource:
            grant_obj = 'RESOURCE "%s"' % resource
        else:
            grant_obj = "%s.%s.%s" % (catalog, database, table)
            grant_obj = grant_obj.strip(".")
        result = self.execute(sql.format(privilege_list=privilege_list, grant_obj=grant_obj,
                                         role_desc=role_desc, user=user))
        if result != ():
            LOG.error(L("GRANT fail", user=user, privilege=privilege_list,
                        database_name=database))
            return False
        return True

    def revoke(self, user, privilege_list, database='', table='', catalog='',
               resource=None, is_role=False):
        """
        revoke
        REVOKE privilege_list ON db_name[.tbl_name] FROM [user_identity] | [ROLE role_name]
        REVOKE privilege_list ON RESOURCE resource_name FROM [user_identity] | [ROLE role_name]
        """
        if isinstance(privilege_list, list):
            privilege_list = ', '.join(privilege_list)
        sql = "REVOKE {privilege_list} ON {revoke_obj} FROM {role_desc}{user}"
        if is_role:
            role_desc = 'ROLE '
            user = '"%s"' % user
        else:
            role_desc = ''
        if resource is None:
            revoke_obj = "%s.%s.%s" % (catalog, database, table)
            revoke_obj = revoke_obj.strip('.')
        else:
            revoke_obj = 'RESOURCE %s' % resource
        result = self.execute(sql.format(privilege_list=privilege_list, revoke_obj=revoke_obj,
                                         role_desc=role_desc, user=user))
        if result != ():
            LOG.error(L("REVOKE fail", sql=sql, msg=str(result)))
            return False
        return True

    def get_grant(self, user=None, all=None):
        """show grant"""
        if user is None:
            sql = 'SHOW GRANTS'
        else:
            sql = 'SHOW GRANTS FOR `%s`' % user
        if all:
            sql = 'SHOW ALL GRANTS'
        result = self.__execute_and_rebuild_meta_class(sql, palo_job.GrantInfo)
        return result

    def is_master(self):
        """
        is master
        deprecated
        """
        sql = "SHOW FRONTENDS"
        fe_list = self.execute(sql)

        for fe in fe_list:
            if fe[palo_job.FrontendInfo.Host] == socket.gethostbyname(self.host):
                return fe[palo_job.FrontendInfo.IsMaster] == "true"
        return False

    def get_alive_backend_list(self):
        """
        获取alive backend
        """
        backend_list = self.get_backend_list()
        result = list()
        for backend in backend_list:
            if palo_job.BackendProcInfo(backend).get_alive() == "true":
                result.append(backend)
        LOG.info(L("GET ALIVE BACKEND", alive_be=result))
        return result

    def get_backend(self, backend_id):
        """
        获取backend
        """
        backend_list = self.get_backend_list()
        for backend in backend_list:
            if palo_job.BackendProcInfo(backend).get_backend_id() == str(backend_id):
                return backend
        LOG.warning(L("Get no backend by backend id", backend_id=backend_id))
        return None

    def get_backend_heartbeat_port(self, value=None, idx=None):
        """get be hearbeat port"""
        be_list = self.get_backend_list()
        if value is None:
            port = palo_job.BackendProcInfo(be_list[0]).get_heartbeatport()
        else:
            port = util.get_attr_condition_value(be_list, idx, value,
                                                 palo_job.BackendProcInfo.HeartbeatPort)
        return port

    def get_backend_list(self):
        """
        获取backend
        """
        sql = "SHOW BACKENDS"
        result = self.__execute_and_rebuild_meta_class(sql, palo_job.BackendProcInfo)
        return result

    def get_backend_id_list(self):
        """get backend id list"""
        backend_list = self.get_backend_list()
        id_list = list()
        for be in backend_list:
            id_list.append(be[0])
        return id_list

    def get_be_hostname_by_id(self, be_id):
        """
        获取backend hostname by id
        """
        be = self.get_backend(be_id)
        if not be:
            return None
        return palo_job.BackendProcInfo(be).get_hostname()

    def get_backend_host_list(self):
        """
        返回活动状态的backend的ip
        """
        backend_list = self.get_alive_backend_list()
        be_host_list = [palo_job.BackendProcInfo(backend).get_ip() for backend in backend_list]
        return tuple(be_host_list)

    def get_backend_host_ip(self):
        """get all be host ip"""
        res = []
        backend_list = self.get_backend_list()
        for backend in backend_list:
            cur_ip = palo_job.BackendProcInfo(backend).get_ip()
            res.append(cur_ip)
        return res

    def get_backend_host_name(self):
        """get all be host name"""
        backend_list = self.get_backend_list()
        return util.get_attr(backend_list, palo_job.BackendProcInfo.Host)

    def get_backend_host_port_list(self):
        """
        返回活动状态的backend的host:port
        metadata changed
        backends[3] is be hostname, backends[4] is be heartbeat port
        """
        backend_list = self.get_alive_backend_list()
        backend_host_port_list = []
        for backend in backend_list:
            backend_info = palo_job.BackendProcInfo(backend)
            backend_host_port = '%s:%s' % (backend_info.get_hostname(),
                                           backend_info.get_heartbeatport())
            backend_host_port_list.append(backend_host_port)
        return tuple(backend_host_port_list)

    def add_backend_list(self, backend_list):
        """
        增加backend
        disable
        """
        if not isinstance(backend_list, list):
            backend_list = [backend_list, ]
        for backend in backend_list:
            sql = 'ALTER SYSTEM ADD BACKEND "%s"' % (backend)
            result = self.execute(sql)
            if result != ():
                LOG.info(L('ADD BACKEND FAIL', backend=backend))
                return False
        return True

    def add_backend(self, host_name, port, tag_location=None):
        """
        增加backend
        """
        sql = 'ALTER SYSTEM ADD BACKEND "%s:%s"' % (host_name, port)
        if tag_location is not None:
            sql = '%s PROPERTIES("tag.location"="%s")' % (sql, tag_location)
        result = self.execute(sql)
        time.sleep(2)
        if result != ():
            LOG.info(L('ADD BACKEND FAIL', backend=host_name, port=port, tag_location=tag_location))
            return False
        LOG.info(L('ADD BACKEND SUCCESS', backend=host_name, port=port, tag_location=tag_location))
        return True

    def drop_backend_list(self, backend_list):
        """
        移除backend
        """
        if not isinstance(backend_list, list):
            backend_list = [backend_list, ]
        for backend in backend_list:
            sql = 'ALTER SYSTEM DROPP BACKEND "%s"' % (backend)
            result = self.execute(sql)
            if result != ():
                LOG.info(L('DROP BACKEND FAIL', backend=backend))
                return False
        time.sleep(2)
        return True

    def decommission_backend_list(self, backend_list):
        """be下线"""
        if not isinstance(backend_list, list):
            backend_list = [backend_list, ]
        for backend in backend_list:
            sql = 'ALTER SYSTEM DECOMMISSION BACKEND "%s"' % (backend)
            result = self.execute(sql)
            if result != ():
                LOG.info(L('DECOMMISSION BACKEND FAIL', backend=backend))
                return False
        return True

    def add_fe_list(self, fe_list, type='OBSERVER'):
        """
        增加FE
        type: OBSERVER, REPLICA
        """
        if not isinstance(fe_list, list):
            fe_list = [fe_list, ]
        for fe in fe_list:
            sql = 'ALTER SYSTEM ADD %s "%s"' % (type, fe)
            result = self.execute(sql)
            if result != ():
                LOG.info(L('ADD FE FAILED', fe=fe))
                return False
        return True

    def drop_fe_list(self, fe_list, type='OBSERVER'):
        """
        增加FE
        type: OBSERVER, REPLICA
        """
        if not isinstance(fe_list, list):
            fe_list = [fe_list, ]
        for fe in fe_list:
            sql = 'ALTER SYSTEM DROP %s "%s"' % (type, fe)
            result = self.execute(sql)
            if result != ():
                LOG.info(L('DROP FE FAILED', fe=fe))
                return False
        return True

    def get_fe_list(self):
        """
        获取FE list
        """
        sql = "SHOW FRONTENDS"
        result = self.execute(sql)
        return result

    def get_fe_host_port_list(self, type=None):
        """
        返回fe的host:port
        type: OBSERVER, REPLICA
        """
        fe_list = self.get_fe_list()
        fe_host_port_list = []
        for fe in fe_list:
            if type is not None and fe[2] != type:
                continue
            fe_host_port = '%s:%s' % (fe[0], fe[1])
            fe_host_port_list.append(fe_host_port)
        return tuple(fe_host_port_list)

    def get_master(self):
        """
        返回master的host:port
        """
        fe_list = self.get_fe_list()
        for fe in fe_list:
            fe_info = palo_job.FrontendInfo(fe)
            if fe_info.get_ismaster() == "true":
                fe_host_port = "%s:%s" % (fe_info.get_host(), fe_info.get_httpport())
                return fe_host_port
        return None

    def get_master_host(self):
        """
        返回master的host:port
        """
        fe_list = self.get_fe_list()
        for fe in fe_list:
            fe_info = palo_job.FrontendInfo(fe)
            if fe_info.get_ismaster() == "true":
                return fe_info.get_host()
        return None

    def get_fe_LastHeartbeat(self, fe_ip):
        """
        返回fe_LastHeartbeat
        """
        fe_list = self.get_fe_list()
        for fe in fe_list:
            if palo_job.FrontendInfo(fe).get_IP() == str(fe_ip):
                return fe[palo_job.FrontendInfo(fe).get_LastHeartbeat()]
        LOG.warning(L("Get no fe by fe id", fe_id=fe_ip))
        return None

    def get_fe_host(self):
        """get fe host list"""
        fe_list = self.get_fe_list()
        fe_host = list()
        for fe in fe_list:
            fe_host.append(fe[1])
        return fe_host

    def recover_database(self, database_name):
        """
        恢复database
        """
        sql = "RECOVER DATABASE %s" % database_name
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("recover database error", fe=str(self), database_name=database_name, \
                        error=e))
            return False
        return True

    def recover_table(self, table_name, database_name=None):
        """
        恢复database
        """
        database_name = self.database_name if database_name is None else database_name
        sql = "RECOVER TABLE "
        if database_name is not None:
            sql = '%s%s.' % (sql, database_name)
        sql = '%s%s' % (sql, table_name)
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("recover table error", fe=str(self), database_name=database_name, \
                        table_name=table_name, error=e))
            return False
        return True

    def recover_partition(self, table_name, partition_name, database_name=None):
        """
        恢复database
        """
        database_name = self.database_name if database_name is None else database_name
        sql = "RECOVER PARTITION %s FROM " % (partition_name)
        if database_name is not None:
            sql = '%s%s.' % (sql, database_name)
        sql = '%s%s' % (sql, table_name)
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("recover partition error", fe=str(self), database_name=database_name, \
                        table_name=table_name, partition_name=partition_name, error=e))
            return False
        return True

    def rename_database(self, new_database_name, old_database_name=None):
        """
        重命名数据库
        """
        if old_database_name is None:
            old_database_name = self.database_name
        sql = 'ALTER DATABASE %s RENAME %s' % (old_database_name, new_database_name)
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("rename database error", fe=str(self),
                        new_database_name=new_database_name, error=e))
            return False
        return True

    def rename_table(self, new_table_name, old_table_name, database_name=None):
        """
        重命名数据库
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s RENAME %s' % (database_name, old_table_name, new_table_name)
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("rename table error", fe=str(self), error=e))
            return False
        return True

    def rename_rollup(self, new_index_name, old_index_name, table_name, database_name=None):
        """
        重命名数据库
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s RENAME ROLLUP %s %s' % (database_name, table_name,
                                                         old_index_name, new_index_name)
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("rename rollup error", fe=str(self), error=e))
            return False
        return True

    def rename_partition(self, new_partition_name, old_partition_name,
                         table_name, database_name=None):
        """
        重命名数据库
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'ALTER TABLE %s.%s RENAME PARTITION %s %s' % (database_name, table_name,
                                                            old_partition_name, new_partition_name)
        try:
            self.execute(sql)
        except PaloException as e:
            LOG.error(L("rename partition error", fe=str(self), error=e))
            return False
        return True

    def show_databases(self, database_name=None):
        """show databases
        """
        sql = 'SHOW DATABASES'
        if database_name:
            sql = 'SHOW DATABASES LIKE "%s"' % database_name
        return self.execute(sql)

    def show_tables(self, table_name=None):
        """show tables
        """
        sql = 'SHOW TABLES'
        if table_name:
            sql = 'SHOW TABLES LIKE "%s"' % table_name
        return self.execute(sql)

    def show_partitions(self, table_name, database_name=None):
        """show partitions
        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'SHOW PARTITIONS FROM %s.%s' % (database_name, table_name)
        result = self.__execute_and_rebuild_meta_class(sql, palo_job.PartitionInfo)
        return self.execute(sql)

    def show_loading_job_state(self, database_name=None, state='LOADING'):
        """show loading state job"""
        ret = self.show_load(database_name=database_name, state=state)
        return ret

    def show_load(self, database_name=None, label=None, state=None, order_by=None, limit=None,
                  offset=None):
        """
        SHOW LOAD
        [FROM db_name]
        [
            WHERE
            [LABEL [ = "your_label" | LIKE "label_matcher"]]
            [STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
        ]
        [ORDER BY ...]
        [LIMIT limit][OFFSET offset];

        Returns:
            load_job

        """
        sql = 'SHOW LOAD'
        if database_name:
            sql = '%s FROM %s' % (sql, database_name)
        if label:
            sql = '%s WHERE label = "%s"' % (sql, label)
        if state:
            sql = '%s WHERE STATE="%s"' % (sql, state)
        if order_by:
            sql = '%s ORDER BY %s' % (sql, order_by)
        if limit:
            sql = '%s LIMIT %s' % (sql, limit)
        if offset:
            sql = '%s OFFSET %s' % (sql, offset)
        return self.execute(sql)

    def desc_table(self, table_name, database_name=None, is_all=False):
        """desc table"""
        database_name = self.database_name if database_name is None else database_name
        if is_all:
            sql = 'DESC %s.%s all' % (database_name, table_name)
        else:
            sql = 'DESC %s.%s' % (database_name, table_name)
        return self.execute(sql)

    def show_schema_change_job(self, database_name=None, table_name=None, state=None):
        """show schema change job"""
        database_name = self.database_name if database_name is None else database_name
        where_list = []
        if table_name:
            where_list.append('TableName = "%s"' % table_name)
        if state:
            where_list.append('State= "%s"' % state)

        if len(where_list) == 0:
            sql = 'SHOW ALTER TABLE COLUMN FROM %s' % database_name
            return self.execute(sql)
        else:
            sql = 'SHOW ALTER TABLE COLUMN FROM %s WHERE %s' % (database_name, ' AND '.join(where_list))
            return self.execute(sql)

    def show_rollup_job(self, database_name=None, table_name=None):
        """show schema change job"""
        database_name = self.database_name if database_name is None else database_name
        sql = 'SHOW ALTER TABLE ROLLUP FROM %s' % database_name
        if table_name is None:
            return self.execute(sql)
        else:
            sql = '%s WHERE TableName = "%s"' % (sql, table_name)
            sc_job = self.execute(sql)
            return sc_job

    def set_properties(self, kv_list, user=None):
        """set properties
        """
        for_user = ''
        if user is not None:
            for_user = 'FOR "%s" ' % user
        properties = ', '.join(kv_list) if not isinstance(kv_list, str) else kv_list
        sql = 'SET PROPERTY %s%s' % (for_user, properties)
        result = self.execute(sql)
        if result != ():
            return False
        return True

    def set_max_user_connections(self, max_connections, user=None):
        """set max user connections
        """
        kv = '"max_user_connections" = "%s"' % max_connections
        return self.set_properties(kv, user)

    def set_resource_cpu_share(self, cpu_share, user=None):
        """set resource cpu share
        """
        kv = '"resource.cpu_share" = "%s"' % cpu_share
        return self.set_properties(kv, user)

    def set_quota_low(self, quota_low, user=None):
        """set quota low
        """
        kv = '"quota.low" = "%s"' % quota_low
        return self.set_properties(kv, user)

    def set_quota_normal(self, quota_normal, user=None):
        """set quota normal
        """
        kv = '"quota.normal" = "%s"' % quota_normal
        return self.set_properties(kv, user)

    def set_quota_high(self, quota_high, user=None):
        """set quota high
        """
        kv = '"quota.high" = "%s"' % quota_high
        return self.set_properties(kv, user)

    def set_load_cluster_hadoop_palo_path(self, cluster_name, hadoop_palo_path, user=None):
        """set load cluster hadoop palo path
        """
        kv = '"load_cluster.%s.hadoop_palo_path" = "%s"' % (cluster_name, hadoop_palo_path)
        return self.set_properties(kv, user)

    def set_load_cluster_hadoop_http_port(self, cluster_name, hadoop_http_port, user=None):
        """set load cluster hadoop http port
        """
        kv = '"load_cluster.%s.hadoop_http_port" = "%s"' % (cluster_name, hadoop_http_port)
        return self.set_properties(kv, user)

    def set_load_cluster_hadoop_configs(self, cluster_name, hadoop_configs, user=None):
        """set load cluster hadoop configs
        """
        kv = '"load_cluster.%s.hadoop_configs" = "%s"' % (cluster_name, hadoop_configs)
        return self.set_properties(kv, user)

    def set_load_cluster(self, cluster_name, hadoop_configs, hadoop_palo_path,
                         hadoop_http_port=None, user=None):
        """set load cluster
        """
        kv_1 = '"load_cluster.%s.hadoop_configs" = "%s"' % (cluster_name, hadoop_configs)
        kv_2 = '"load_cluster.%s.hadoop_palo_path" = "%s"' % (cluster_name, hadoop_palo_path)
        kv_list = [kv_1, kv_2]
        if hadoop_http_port is not None:
            kv_3 = '"load_cluster.%s.hadoop_http_port" = "%s"' % (cluster_name, hadoop_http_port)
            kv_list.append(kv_3)
        return self.set_properties(kv_list, user)

    def set_default_load_cluster(self, cluster_name, user=None):
        """set default load cluster
        """
        kv = '"default_load_cluster" = "%s"' % cluster_name
        return self.set_properties(kv, user)

    def remove_default_load_cluster(self, user=None):
        """remove default load cluster
        """
        kv = '"default_load_cluster" = ""'
        return self.set_properties(kv, user)

    def remove_load_cluster(self, cluster_name, user=None):
        """remove load cluster hadoop configs
        """
        kv = '"load_cluster.%s" = ""' % cluster_name
        return self.set_properties(kv, user)

    def remove_load_cluster_hadoop_configs(self, cluster_name, user=None):
        """remove load cluster hadoop configs
        """
        kv = '"load_cluster.%s.hadoop_configs" = ""' % cluster_name
        return self.set_properties(kv, user)

    def remove_load_cluster_hadoop_http_port(self, cluster_name, user=None):
        """remove load cluster hadoop http port
        """
        kv = '"load_cluster.%s.hadoop_http_port" = ""' % cluster_name
        return self.set_properties(kv, user)

    def remove_load_cluster_hadoop_palo_path(self, cluster_name, user=None):
        """remove load cluster hadoop palo path
        """
        kv = '"load_cluster.%s.hadoop_palo_path" = ""' % cluster_name
        return self.set_properties(kv, user)

    def gen_hadoop_configs(self, fs_default_name=None, mapred_job_tracker=None,
                           hadoop_job_ugi=None, mapred_job_priority=None):
        """gen hadoop configs
        """
        configs = ''
        if fs_default_name is not None:
            configs += 'fs.default.name=%s;' % (fs_default_name)
        if mapred_job_tracker is not None:
            configs += 'mapred.job.tracker=%s;' % (mapred_job_tracker)
        if hadoop_job_ugi is not None:
            configs += 'hadoop.job.ugi=%s;' % (hadoop_job_ugi)
        if mapred_job_priority is not None:
            configs += 'mapred.job.priority=%s;' % (mapred_job_priority)
        configs = configs.rstrip(';')
        return configs

    def show_property(self, key=None, user=None):
        """show property
        """
        for_user = '' if user is None else ' FOR "%s"' % user
        property_key = '' if key is None else ' LIKE "%s" ' % key
        sql = 'SHOW PROPERTY%s%s' % (for_user, property_key)
        return self.execute(sql)

    def show_max_user_connections(self, user=None):
        """show max user connections
           ((u'max_user_connections', u'10'),)
        """
        result = self.show_property('max_user_connections', user)
        return int(result[0][1])

    def show_resource_cpu_share(self, user=None):
        """show resource cpu share
        ((u'resource.cpu_share', u'1000'),
        (u'resource.hdd_read_iops', u'80'),
        (u'resource.hdd_read_mbps', u'30'),
        (u'resource.io_share', u'1000'),
        (u'resource.ssd_read_iops', u'1000'),
        (u'resource.ssd_read_mbps', u'30'))
        """
        result = self.show_property('resource', user)
        for item in result:
            if item[0] == 'resource.cpu_share':
                return int(item[1])

    def show_quota_low(self, user=None):
        """show quota low
        """
        result = self.show_property('quota', user)
        for item in result:
            if item[0] == 'quota.low':
                return int(item[1])

    def show_quota_normal(self, user=None):
        """show quota normal
        """
        result = self.show_property('quota', user)
        for item in result:
            if item[0] == 'quota.normal':
                return int(item[1])

    def show_quota_high(self, user=None):
        """show quota high
        """
        result = self.show_property('quota', user)
        for item in result:
            if item[0] == 'quota.high':
                return int(item[1])

    def show_load_cluster(self, user=None):
        """show load cluster
        """
        result = self.show_property('%load_cluster%', user)
        load_cluster_dict = {}
        for k, v in result:
            load_cluster_dict[k] = v
        return load_cluster_dict

    def add_whitelist(self, user, whitelist):
        """
        add whitelist
        """
        sql = 'ALTER USER %s ADD WHITELIST "%s"' % (user, whitelist)
        return self.execute(sql) == ()

    def delete_whitelist(self, user, whitelist):
        """
        delete whitelist
        """
        sql = 'ALTER USER %s DELETE WHITELIST "%s"' % (user, whitelist)
        return self.execute(sql) == ()

    def show_whitelist(self, user):
        """
        show whitelist
        """
        sql = 'SHOW WHITELIST'
        result = self.execute(sql)
        for r in result:
            if r[0] == user:
                return r[1]
        return None

    def clean_whitelist(self, user):
        """
        clean whitelist
        """
        whitelist = self.show_whitelist(user)
        if whitelist is None:
            return
        for w in whitelist.split(','):
            if w:
                self.delete_whitelist(user, w)

    def create_cluster(self, cluster_name, instance_num, password=''):
        """
        create cluster
        """
        sql = 'CREATE CLUSTER %s PROPERTIES ( "instance_num" = "%d") ' \
              'IDENTIFIED BY "%s"' % (cluster_name, instance_num, password)
        self.execute(sql)

    def enter(self, cluster_name):
        """
        create cluster
        """
        sql = 'ENTER %s' % (cluster_name)
        self.execute(sql)
        self.cluster_name = cluster_name

    def clean_cluster(self, cluster_name=None):
        """
        clean cluster
        """
        if not cluster_name:
            cluster_name_list = self.get_cluster_list()
            for cluster_name in cluster_name_list:
                if cluster_name == 'default_cluster':
                    continue
                self.enter(cluster_name)
                self.clean()
                self.drop_cluster(cluster_name)
        else:
            try:
                self.enter(cluster_name)
                self.clean()
                self.drop_cluster(cluster_name)
            except:
                pass

    def drop_cluster(self, cluster_name):
        """
        drop cluster
        """
        sql = 'DROP CLUSTER %s' % (cluster_name)
        self.execute(sql)

    def get_cluster_list(self):
        """
        show cluster
        """
        sql = 'SHOW CLUSTERS'
        records = self.execute(sql)
        return tuple([r[0] for r in records])

    def alter_cluster(self, cluster_name, instance_num):
        """
        alter cluster
        """
        sql = 'ALTER CLUSTER %s PROPERTIES ( "instance_num" = "%d" )' \
              % (cluster_name, instance_num)
        self.execute(sql)

    def link(self, src_cluster_name, src_database_name, dst_cluster_name, dst_database_name):
        """
        link
        """
        sql = 'LINK DATABASE %s.%s %s.%s' % (src_cluster_name,
                                             src_database_name, dst_cluster_name, dst_database_name)
        self.execute(sql)

    def migrate(self, src_cluster_name, src_database_name, dst_cluster_name, dst_database_name):
        """
        migrate
        """
        sql = 'MIGRATE DATABASE %s.%s %s.%s' % (src_cluster_name,
                                                src_database_name, dst_cluster_name, dst_database_name)
        self.execute(sql)

    def get_migrate_status(self, src_cluster_name, src_database_name,
                           dst_cluster_name, dst_database_name):
        """
        获取指定状态的导入任务信息
        """
        sql = 'SHOW MIGRATIONS'
        migrate_list = self.execute(sql)
        print(migrate_list)
        for it in migrate_list:
            if it[1] == '%s:%s' % (src_cluster_name, src_database_name) \
                    and it[2] == '%s:%s' % (dst_cluster_name, dst_database_name):
                return it[3]
        return None

    def wait_migrate(self, src_cluster_name, src_database_name,
                     dst_cluster_name, dst_database_name):
        """
        wait migrate
        """
        time.sleep(5)
        ret = self.get_migrate_status(src_cluster_name, src_database_name,
                                      dst_cluster_name, dst_database_name)
        if not ret:
            return False
        while True:
            ret = self.get_migrate_status(src_cluster_name, src_database_name,
                                          dst_cluster_name, dst_database_name)
            if ret == '100%':
                return True
            time.sleep(1)

    def add_broker(self, broker_name, host_port):
        """
        add broker
        """
        sql = 'ALTER SYSTEM ADD BROKER %s "%s"' % (broker_name, host_port)
        return self.execute(sql) == ()

    def drop_broker(self, broker_name, host_port):
        """
        add broker
        """
        sql = 'ALTER SYSTEM DROP BROKER %s "%s"' % (broker_name, host_port)
        return self.execute(sql) == ()

    def get_broker_list(self):
        """
        show whitelist
        """
        sql = 'SHOW PROC "/brokers"'
        result = self.execute(sql)
        return result

    def get_broker_start_update_time(self):
        """show broker_start_update_time"""
        res = self.get_broker_list()
        if res:
            LOG.info(L("get_broker_start_update_time", start_time=palo_job.BrokerInfo(res[0]).get_last_start_time(),
                       update_time=palo_job.BrokerInfo(res[0]).get_last_update_time()))
            return palo_job.BrokerInfo(res[0]).get_last_start_time()
        return None

    def drop_all_broker(self, broker_name):
        """
        add broker
        """
        sql = 'ALTER SYSTEM DROP ALL BROKER %s"' % broker_name
        return self.execute(sql) == ()

    def export(self, table_name, to_path, broker_info=None,
               partition_name_list=None, property_dict=None,
               database_name=None, where=None):
        """export data.

        Args:
            table_name: table name, str
            to_path: export path, str
            broker_info: broker info, BrokerInfo
            partition_name_list: name list of patitions, str list
            property_dict: properties, dict str->str
            database_name: database name, str
            where: str, k1>0
        Returns:
            True if succeed
        """
        sql = 'EXPORT TABLE'
        if not database_name:
            database_name = self.database_name
        sql = '%s %s.%s' % (sql, database_name, table_name)
        if partition_name_list:
            sql = '%s PARTITION (%s)' % (sql, ','.join(partition_name_list))
        if where:
            sql = '%s WHERE %s' % (sql, where)
        sql = '%s TO "%s"' % (sql, to_path)
        if property_dict:
            p = ''
            for d in property_dict:
                p = '%s "%s"="%s",' % (p, d, property_dict[d])
            p = p.rstrip(",")
            sql = '%s PROPERTIES(%s)' % (sql, p)
        if broker_info:
            sql = '%s %s' % (sql, str(broker_info))

        return self.execute(sql) == ()

    def get_export_status(self, database_name=None):
        """get export status.

        Args:
            database_name: database name, str
        Returns:
            True if succeed
        """
        sql = 'SHOW EXPORT'
        if database_name:
            sql = '%s FROM %s' % (sql, database_name)
        sql = '%s %s' % (sql, 'ORDER BY JOBID LIMIT 1')
        return self.execute(sql)

    def show_export(self, database_name=None, state=None, export_job_id=None,
                    order_by=None, limit=None):
        """
        Args:
            database_name: database name
            state: export status filter, PENDING|EXPORTIING|FINISHED|CANCELLED
            export_job_id: export job id filter
            order_by: column, str 'starttime'
            limit: num, int
        Returns:
            export job
        """
        sql = 'SHOW EXPORT'
        if database_name:
            sql = '%s FROM %s' % (sql, database_name)
        if state:
            sql = '%s WHERE STATE="%s"' % (sql, state)
            if export_job_id:
                sql = '%s AND ID="%s"' % (sql, export_job_id)
        elif export_job_id:
            sql = '%s WHERE ID="%s"' % (sql, export_job_id)
        if order_by:
            sql = '%s ORDER BY %s' % (sql, order_by)
        if limit:
            sql = '%s LIMIT %s' % (sql, limit)
        return self.execute(sql)

    def wait_export(self, database_name=None):
        """wait export.

        Args:
            database_name: database name, str
        Returns:
            True if succeed
        """
        timeout = 1200
        while timeout > 0:
            r = self.get_export_status(database_name)
            export_job = palo_job.ExportJob(r[0])
            if export_job.get_state() == 'FINISHED':
                LOG.info(L("export succ.", state='FINISHED', database_name=database_name))
                return True
            elif export_job.get_state() == 'CANCELLED':
                LOG.warning(L("export fail.", state='CANCELLED', database_name=database_name,
                           msg=export_job.get_error_msg()))
                return False
            else:
                time.sleep(1)
                timeout -= 1
        LOG.warning(L("export timeout.", timeout=timeout, database_name=database_name))
        return False

    def create_external_table(self, table_name, column_list, engine, property,
                              broker_property=None, database_name=None, set_null=False):
        """Create broker table.

        Args:
            table_name: table name, str
            column_list: list of columns
            engine: str, it should be olap, mysql, elasticsearch, broker
            property: map,
                if broker engine e.g.:
                PROPERTIES (
                "broker_name" = "broker_name",
                "paths" = "file_path1[,file_path2]",
                "column_separator" = "value_separator"
                "line_delimiter" = "value_delimiter"
                |"format" = "parquet"
                )
                mysql engine e.g.:
                PROPERTIES (
                "host" = "mysql_server_host",
                "port" = "mysql_server_port",
                "user" = "your_user_name",
                "password" = "your_password",
                "database" = "database_name",
                "table" = "table_name"
                )
                es engine e.g.:
                PROPERTIES (
                "hosts" = "http://",
                "user" = "root",
                "password" = "",
                "index" = "new_data", "type" = "mbtable" )
            broker_property: broker property, broker maybe hdfs, bos, afs.
            set_null: if column can be null
        Returns:
            True if succeed
        """
        database_name = self.database_name if database_name is None else database_name
        # table name
        sql = 'CREATE EXTERNAL TABLE %s.%s (' % (database_name, table_name)
        # columns
        for column in column_list:
            sql = '%s %s,' % (sql, util.column_to_no_agg_sql(column, set_null))
        sql = '%s ) ENGINE=%s' % (sql.rstrip(','), engine)
        # property
        sql = '%s PROPERTIES %s' % (sql, util.convert_dict2property(property))
        # broker_property
        if isinstance(broker_property, BrokerInfo):
            sql = '%s BROKER PROPERTIES %s' % (sql, broker_property.get_property())
        elif isinstance(broker_property, str):
            sql = '%s BROKER PROPERTIES (%s)' % (sql, broker_property)
        elif isinstance(broker_property, dict):
            sql = '%s BROKER PROPERTIES %s' % (sql, util.convert_dict2property(broker_property))
        else:
            pass
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L('CREATE EXTERNAL TABLE fail.', database_name=database_name,
                       table_name=table_name))
            return False
        LOG.info(L('CREATE EXTERNAL TABLE succ.', database_name=database_name,
                   table_name=table_name))
        return True

    def stream_load(self, table_name, data_file, database_name=None, host=None, port=None,
                    user=None, password=None, cluster_name=None, is_wait=True, max_filter_ratio=None,
                    load_label=None, column_name_list=None, timeout=300, column_separator=None,
                    partition_list=None, where_filter=None, time_zone=None, **kwargs):
        """
        Args:
            table_name: 必填,表名
            data_file:  必填,上传数据
            user: 可选,palo的用户
            password: 可选, palo用户的密码
            database_name: 可选, 数据库名,默认为当前client的数据库
            cluster_name: 可选, cluster,默认为default_cluster.不建议使用
            host: 可选，默认为当前client连接的fe
            port: 可选，默认为当前client的http_port
            -H参数包括：
            max_filter_ratio: 可选, 最大容忍可过滤的数据比例0-1
            load_label/label: 可选,导入的标签
            column_name_list/columns: 可选, 指定导入文件中的列和table中的列的对应关系
            timeout: 可选,连接的超时时间
            column_separator: 可选, 列的分割符
            partition_list/partitions: 可选, 指定本次导入的分区
            where_filter/where: 可选,用户抽取部分数据
            time_zone/timezone: 可选,用户设置时区
            strict_mode: 用户指定此次导入是否开启严格模式，默认为false
            exec_mem_limit: 导入内存限制。默认为 2GB。单位为字节
            format: 指定导入数据格式，默认是csv，支持json格式
            jsonpaths: 导入json方式分为：简单模式和精准模式
            strip_outer_array: 为true表示json数据以数组对象开始且将数组对象中进行展平，默认值是false
            json_root: json_root为合法的jsonpath字符串，用于指定json document的根节点，默认值为""
            控制参数：
            is_wait: 当返回结果为publish timeout时，是否等待事务visible
          
        Returns:
            0 fail/label already exist
            1 success
            2 publish timeout
        """
        database_name = database_name if database_name is not None else self.database_name
        host = host if host is not None else self.host
        port = port if port is not None else self.http_port
        user = user if user is not None else self.user
        password = password if password is not None else self.password
        uri = "http://%s:%s/api/%s/%s/_stream_load" % (host, port, database_name, table_name)

        buf = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, uri)
        c.setopt(c.WRITEFUNCTION, buf.write)
        head = dict()
        head['Content-length'] = os.path.getsize(data_file)
        head['Transfer-Encoding'] = ''
        head['Expect'] = '100-continue'
        file = open(data_file, "rb")
        if load_label:
            head['label'] = load_label
        if column_name_list:
            head['columns'] = ','.join(column_name_list)
        if column_separator:
            head['column_separator'] = column_separator
        if max_filter_ratio:
            head['max_filter_ratio'] = max_filter_ratio
        if partition_list:
            head['partitions'] = ','.join(partition_list)
        if where_filter:
            head['where'] = where_filter
        if time_zone:
            head['timezone'] = time_zone
        if timeout:
            head['timeout'] = timeout
        print(data_file)
        head.update(kwargs)
        print(head)
        param = ''
        for k, v in head.items():
            param = '%s -H "%s:%s"' % (param, k, v)
        curl_cmd = 'curl --location-trusted -u %s:%s %s -T %s %s' % (user, password, param, data_file, uri)
        LOG.info(L('STREAM LOAD CURL CMD.', cmd=curl_cmd))
        print(curl_cmd)
        # 设置-H参数
        c.setopt(pycurl.HTTPHEADER, [k + ': ' + str(v) for k, v in head.items()])
        # basic认证
        c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
        c.setopt(pycurl.USERNAME, user)
        c.setopt(pycurl.PASSWORD, password)
        # 上传文件
        c.setopt(pycurl.PUT, 1)
        c.setopt(pycurl.UPLOAD, 1)
        c.setopt(pycurl.READDATA, file)
        # 重定向，--location-trusted
        c.setopt(pycurl.UNRESTRICTED_AUTH, 1)
        c.setopt(pycurl.FOLLOWLOCATION, True)
        # 连接超时
        c.setopt(pycurl.TIMEOUT, 30)
        LOG.info(L('STREAM LOAD.', head=head, url=uri, data_file=data_file))
        try:
            c.perform()
            msg = buf.getvalue()
            print(msg)
            LOG.info(L('STREAM LOAD ret.', ret=msg))
            if c.getinfo(c.HTTP_CODE) == 200:
                ret = json.loads(msg)
                status = ret.get("Status")
                txn_id = ret.get("TxnId")
                if status == 'Success':
                    stream_load_ret = 1
                    self.wait_txn(txn_id, database_name)
                elif status == 'Publish Timeout':
                    stream_load_ret = 2
                    if is_wait:
                        self.wait_txn(txn_id, database_name)
                else:
                    # Label Already Exists/Fail
                    url = ret.get("ErrorURL")
                    if url:
                        sql = 'show load warnings on "%s"' % url
                        ret = self.execute(sql)
                        print(ret[0])
                    stream_load_ret = 0
            else:
                stream_load_ret = 0
        except Exception as e:
            stream_load_ret = 0
            print(str(e))
            LOG.info(L('STREAM LOAD FAILED.', msg=str(e)))
        finally:
            buf.close()
            file.close()
            c.close()
            return stream_load_ret

    def insert_select(self, table_name, select_query, column_name_list=None, is_streaming=True, database_name=None):
        """insert"""
        if database_name is not None:
            sql = 'INSERT INTO %s.%s' % (database_name, table_name)
        else:
            sql = 'INSERT INTO %s' % table_name
        if column_name_list:
            sql = '%s (%s)' % (sql, ','.join(column_name_list))
        sql = '%s %s' % (sql, select_query)
        ret = self.execute(sql)
        return ret == ()

    def get_column(self, column_name, table_name, database_name=None):
        """get column"""
        column_tuple = self.desc_table(table_name, database_name)
        for column in column_tuple:
            if column_name == palo_job.DescInfo(column).get_field():
                return column_name
        return None

    def get_column_info(self, column_name, table_name, database_name=None):
        """get column info"""
        column_tuple = self.desc_table(table_name, database_name)
        for column in column_tuple:
            if column_name == palo_job.DescInfo(column).get_field():
                return column
        return None

    def get_all_columns(self, table_name, database_name=None):
        """get table columns"""
        column_tuple = self.desc_table(table_name, database_name)
        column_name_list = list()
        for column in column_tuple:
            column_name = palo_job.DescInfo(column).get_field()
            column_name_list.append(column_name)
        return column_name_list

    def create_repository(self, repo_name, broker_name, repo_location, repo_properties,
                          is_read_only=False):
        """
        Args:
            repo_name: string, repository name
            broker_name: string, broker name
            repo_location: string, repository location path
            repo_properties: dict, property of repository location
                            "bos_endpoint" = "http://gz.bcebos.com",
                            "bos_accesskey" = "XXXXXXXXXXXXXXXXXXXX",
                            "bos_secret_accesskey"="XXXXXXXXXXXXXXXXX"

            properties = dict()
            properties["bos_endpoint"] = "http://gz.bcebos.com"
            properties["bos_accesskey"] = "XXXXXXXXXXXXXXXXXXX"
            properties["bos_secret_accesskey"] = "XXXXXXXXXXXXXXXXXXX"
        Returns:
            True create repo success
            False create repo failed
            need to check
        """
        sql = 'CREATE {readonly} REPOSITORY {repo_name} WITH BROKER {broker_name} \
               ON LOCATION "{repo_location}" PROPERTIES ({repo_properties})'
        if isinstance(repo_properties, dict):
            property = ''
            for k, v in repo_properties.items():
                property += ' "%s" = "%s",' % (k, v)
            p = property.strip(',')
        elif isinstance(repo_properties, str):
            p = repo_properties
        else:
            return False
        if is_read_only:
            readonly = 'READ ONLY'
        else:
            readonly = ''
        s = sql.format(readonly=readonly, repo_name=repo_name, broker_name=broker_name,
                       repo_location=repo_location, repo_properties=p)
        try:
            ret = self.execute(s)
        except Exception as e:
            logging.exception(e)
            LOG.info(L('CREATE REPO fail.', repository_name=repo_name, msg=str(e)))
            return False
        if ret == ():
            LOG.info(L('CREATE REPO succ.', repository_name=repo_name))
            return True
        else:
            LOG.info(L('CREATE REPO fail.', repository_name=repo_name))
            return False

    def drop_repository(self, repo_name):
        """

        Args:
            repo_name: string, repository name to be dropped

        Returns:
            True drop repo success
            False drop repo failure

        """
        sql = 'DROP REPOSITORY {repo_name}'
        s = sql.format(repo_name=repo_name)
        try:
            ret = self.execute(s)
        except Exception as e:
            logging.exception(e)
            LOG.info(L('DROP REPO fail.', repository_name=repo_name, msg=str(e)))
            return False
        if ret == ():
            LOG.info(L('DROP REPO succ.', repository_name=repo_name))
            return True
        else:
            LOG.info(L('DROP REPO fail.', repository_name=repo_name))
            return False

    def show_repository(self):
        """
        Returns: existed repo

        RepoId：     唯一的仓库ID
        RepoName：   仓库名称
        CreateTime： 第一次创建该仓库的时间
        IsReadOnly： 是否为只读仓库
        Location：   仓库中用于备份数据的根目录
        Broker：     依赖的 Broker
        ErrMsg：     Palo 会定期检查仓库的连通性，如果出现问题，这里会显示错误信息
        """
        sql = 'SHOW REPOSITORIES'
        ret = self.execute(sql)
        return ret

    def get_repository(self, repo_name=None, repo_info=False):
        """
        Args:
            repo_name: string, repo name

        Returns:
            if repo_name exists return
            (RepoId, RepoName, CreateTime, IsReadOnly, Location, Broker, ErrMsg)
            else return None
        """
        r = self.show_repository()
        if repo_name is None:
            return util.get_attr(r, palo_job.RepoInfo.RepoName)
        for repo in r:
            if repo_name == repo[palo_job.RepoInfo.RepoName]:
                if not repo_info:
                    return repo
                else:
                    return palo_job.RepoInfo(repo)
        return None

    def backup(self, snapshot_label, backup_list, repo_name, database_name=None, type=None,
               timeout=1800, is_wait=False):
        """

        Args:
            snapshot_label: string, snapshot name
            backup_list: list, table/partition to backup, ['table', 'table2 PARTITION (p1, p2)']
            repo_name: string, repo name
            database_name: string, db name, if none, use default db
            type: string, only support full, full backup
            timeout: int, timeout
            is_wait: False/True, if wait backup job finish

        Returns:
            True backup success
            False backup failure

        """
        sql = 'BACKUP SNAPSHOT {db}.{snapshot_label} TO {repo} on ({backup}) {property}'
        b = ','.join(backup_list)
        property = 'PROPERTIES({property})'
        database_name = self.database_name if database_name is None else database_name
        if type is None and timeout is None:
            property = ''
        else:
            p = list()
            if type and type.upper() == 'FULL':
                p.append('"type"="full"')
            if timeout:
                p.append('"timeout"="%s"' % timeout)
            property = property.format(property='e,'.join(p))
        s = sql.format(db=database_name, snapshot_label=snapshot_label, repo=repo_name, backup=b,
                       property=property)
        try:
            ret = self.execute(s)
        except Exception as e:
            logging.exception(e)
            LOG.info(L('BACKUP SNAPSHOT FAILED.', snapshot_label=snapshot_label, msg=str(e)))
            msg = 'Can only run one backup or restore job of a database at same time'
            if msg in str(e):
                ret = self.show_backup(database_name)
                LOG.info(L("Currrent backup jobs", ret=ret))
                ret = self.show_restore(database_name)
                LOG.info(L('Currrent restore job', ret=ret))
            return False
        if ret != ():
            LOG.info(L('BACKUP SNAPSHOT FAILED.', snapshot_label=snapshot_label))
            return False
        if is_wait is False:
            LOG.info(L('BACKUP SNAPSHOT SUCCEEDED.', snapshot_label=snapshot_label))
            return True
        else:
            ret = self.wait_backup_job(snapshot_label)
            return ret

    def wait_backup_job(self, label=None, database_name=None):
        """
        Args:
            label: if label is None, wait all backup job finish, else wait label job finish(backup job can not run at the same time in a db)

        Returns:
            True if backup job is Finished state
            False if backup job is Cancelled state

        """
        ret = self.show_backup(database_name)
        if ret == ():
            return True
        flag = False
        while label is None and flag is False:
            flag = True
            for backup_job in ret:
                s = backup_job[palo_job.BackupJob.State]
                if s != 'FINISHED' and s != 'CANCELLED':
                    flag = False
            time.sleep(1)
            ret = self.show_backup(database_name)
        if flag is True:
            LOG.info(L('BACKUP JOB ALL FINISHED, NO JOB RUNNING.'))
            return True
        timeout = 3600
        while timeout > 0:
            for backup_job in ret:
                if label == backup_job[palo_job.BackupJob.SnapshotName]:
                    s = backup_job[palo_job.BackupJob.State]
                    if s == 'FINISHED':
                        LOG.info(L('BACKUP SNAPSHOT FINISEHD.', snapshot_label=label))
                        return True
                    elif s == 'CANCELLED':
                        LOG.info(L('BACKUP SNAPSHOT CANCELLED.', snapshot_label=label,
                                   msg=backup_job[palo_job.BackupJob.Status]))
                        return False
                time.sleep(3)
            ret = self.show_backup(database_name)
            timeout -= 1
        LOG.info(L('BACKUP JOB WAIT TIMEOUT.', snapshot_label=label))
        return False

    def show_backup(self, database_name=None):
        """

        Args:
            database_name: string, database name

        Returns:
            ((), (),...)

        """
        if database_name is None:
            sql = 'SHOW BACKUP'
        else:
            sql = 'SHOW BACKUP FROM {db}'.format(db=database_name)
        ret = self.__execute_and_rebuild_meta_class(sql, palo_job.BackupJob)
        return ret

    def cancel_backup(self, database_name=None):
        """

        Args:
            database_name: string, database name

        Returns:
            True if cancel backup job success
            False if cancel backup job failure

        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'CANCEL BACKUP FROM {db}'.format(db=database_name)
        ret = self.execute(sql)
        if ret == ():
            LOG.info(L('CANCEL BACKUP SUCCESS.', database_name=database_name))
            return True
        else:
            LOG.info(L('CANCEL BACKUP FAILURE.', database_name=database_name))
            return False

    def restore(self, snapshot_name, repo_name, restore_list, database_name=None,
                replication_num=None, timeout=1800, is_wait=False):
        """

        Args:
            snapshot_name: string, snapshot name
            repo_name: string, repo name
            restore_list: list, table and partition and rename, ['table1', 'table2 PARTITION (p1, p2)', 'table3 PARTITION (p1, p2) AS table_rename']
            database_name: string, database name
            replication_num: int, replication number
            timeout: int, timeout
            is_wait: True/False, if wait restore job finished

        Returns:
            True if restore succeeded
            False if restore failed

        """
        sql = 'RESTORE SNAPSHOT {db}.{snapshot_name} from {repo_name} on ({restore_list}) ' \
              'PROPERTIES("backup_timestamp"="{timestamp}"{property})'
        database_name = self.database_name if database_name is None else database_name
        r = ','.join(restore_list)
        p = ''
        if replication_num:
            p = '%s, "replication_num"="%d"' % (p, replication_num)
        if timeout:
            p = '%s, "timeout"="%d"' % (p, timeout)
        backup_timestamp = self.__get_backup_timestamp(repo_name, snapshot_name)
        if not backup_timestamp:
            LOG.info(L('get timestamp error when restore.', snapshot=snapshot_name, repo=repo_name))
            return False
        s = sql.format(db=database_name, snapshot_name=snapshot_name, repo_name=repo_name,
                       restore_list=r, timestamp=backup_timestamp, property=p)
        try:
            ret = self.execute(s)
        except Exception as e:
            logging.exception(e)
            LOG.info(L('RESTORE SNAPSHOT FAILED.', snapshot_name=snapshot_name, msg=str(e)))
            msg = 'Can only run one backup or restore job of a database at same time'
            if msg in str(e):
                ret = self.show_restore(database_name)
                LOG.info(L('Currrent restore job', ret=ret))
                ret = self.show_backup(database_name)
                LOG.info(L('Currrent backup job', ret=ret))
            return False
        if ret != ():
            LOG.info(L('RESTORE SNAPSHOT FAILED.', snapshot_name=snapshot_name))
            return False
        if is_wait is False:
            LOG.info(L('RESTORE SNAPSHOT SUCCEED.', snapshot_name=snapshot_name))
            return True
        else:
            r = self.wait_restore_job(database_name)
            return r

    def show_restore(self, database_name=None):
        """

        Args:
            database_name: string, database name

        Returns:
            ((), (), ())

        """
        if database_name is None:
            sql = 'SHOW RESTORE'
        else:
            sql = 'SHOW RESTORE FROM %s' % database_name
        ret = self.__execute_and_rebuild_meta_class(sql, palo_job.RestoreJob)
        return ret

    def wait_restore_job(self, database_name=None):
        """wait restore job finished"""
        ret = self.show_restore(database_name)
        if ret == ():
            return True
        flag = False
        while flag is False:
            flag = True
            for restore_job in ret:
                s = restore_job[palo_job.RestoreJob.State]
                if s != 'FINISHED' and s != 'CANCELLED':
                    flag = False
            time.sleep(3)
            ret = self.show_restore(database_name)

        LOG.info(L('RESTORE JOB FINISHED.', state=ret[-1][palo_job.RestoreJob.State],
                   status=ret[-1][palo_job.RestoreJob.Status]))
        return 'FINISHED' == ret[-1][palo_job.RestoreJob.State]

    def cancel_restore(self, database_name=None):
        """

        Args:
            database_name: string, database name

        Returns:
            True if cancel restore job succeed
            False if cancel resotre job fail

        """
        database_name = self.database_name if database_name is None else database_name
        sql = 'CANCEL RESTORE FROM %s' % database_name
        ret = self.execute(sql)
        if ret == ():
            LOG.info(L('CANCEL RESTORE JOB SUCCEED.', database_name=database_name))
            return True
        else:
            LOG.info(L('CANCEL RESTORE JOB FAIL.', database_name=database_name))
            return False

    def show_snapshot(self, repo_name, snapshot=None, timestamp=None):
        """
        Args:
            repo_name: string, repo name
            snapshot: string, snapshot name(label)
            timestamp: string, backup timestamp

        Returns:
            ((), ())
        """
        sql = 'SHOW SNAPSHOT ON %s' % repo_name
        if snapshot is None and timestamp is None:
            r = self.execute(sql)
            return r
        sql = '%s WHERE' % sql
        if snapshot:
            sql = '%s SNAPSHOT = "%s"' % (sql, snapshot)
            if timestamp:
                sql = '%s AND TIMESTAMP = "%s"' % (sql, timestamp)
        elif timestamp:
            sql = '%s TIMESTAMP = "%s"' % (sql, timestamp)
        r = self.execute(sql)
        return r

    def __get_backup_timestamp(self, repo_name, snapshot_name):
        """
        Args:
            snapshot_name: string, snapshot name (label)

        Returns:
            timestamp: string
        """
        r = self.show_snapshot(repo_name, snapshot=snapshot_name)
        if len(r) == 1:
            return r[0][palo_job.SnapshotInfo.Timestamp]
        else:
            return None

    def routine_load(self, table_name, routine_load_job_name, routine_load_property,
                     database_name=None, data_source='KAFKA'):
        """
        Args:
            table_name: string, table name
            routine_load_job_name: string, routine load job name
            routine_load_property: 
            database_name: RoutineLoadProperty class, routine load properties: load property, job property, data source property
            data_source: string, data source like KAFKA

        Returns:
            if create routine load ok
        """
        create_sql = 'CREATE ROUTINE LOAD {routine_load_job_name} ON {table_name} ' \
                     '{load_property} {job_property} FROM {data_source} {data_source_property}'
        if database_name is not None:
            routine_load_job_name = '%s.%s' % (database_name, routine_load_job_name)
        if not isinstance(routine_load_property, RoutineLoadProperty):
            LOG.info(L('CREATE ROUTINE LOAD ERROR. routine load property should be class RoutineLoadProperty'))
        sql = create_sql.format(routine_load_job_name=routine_load_job_name, table_name=table_name,
                                load_property=routine_load_property.load_property,
                                job_property=routine_load_property.job_property, data_source=data_source,
                                data_source_property=routine_load_property.data_source_property)
        try:
            ret = self.execute(sql)
            LOG.info(L('CREATE ROUTINE LOAD JOB OK.'))
            return ret == ()
        except Exception as e:
            LOG.info(L('CREATE ROUTINE LOAD JOB ERROR.', msg=str(e)))
            return False

    def pause_routine_load(self, routine_load_job_name, database_name=None):
        """pause routine load job"""
        if database_name is None:
            sql = 'PAUSE ROUTINE LOAD FOR %s' % routine_load_job_name
        else:
            sql = 'PAUSE ROUTINE LOAD FOR %s.%s' % (database_name, routine_load_job_name)
        try:
            ret = self.execute(sql)
            LOG.info(L('PAUSE ROUTINE LOAD OK', name=routine_load_job_name))
            return ret == ()
        except Exception as e:
            LOG.info(L('PAUSE ROUTINE LOAD ERROR', name=routine_load_job_name, msg=str(e)))
            return False

    def resume_routine_load(self, routine_load_job_name, database_name=None):
        """resume routine load"""
        if database_name is None:
            sql = 'RESUME ROUTINE LOAD FOR %s' % routine_load_job_name
        else:
            sql = 'RESUME ROUTINE LOAD FOR %s.%s' % (database_name, routine_load_job_name)
        try:
            ret = self.execute(sql)
            LOG.info(L('RESUME ROUTINE LOAD OK', name=routine_load_job_name))
            return ret == ()
        except Exception as e:
            LOG.info(L('RESUME ROUTINE LOAD ERROR', name=routine_load_job_name, msg=str(e)))
            return False

    def stop_routine_load(self, routine_load_job_name, database_name=None):
        """stop routine load"""
        if database_name is None:
            job_name = routine_load_job_name
        else:
            job_name = '%s.%s' % (database_name, routine_load_job_name)
        sql = 'STOP ROUTINE LOAD FOR %s' % job_name
        try:
            ret = self.execute(sql)
            LOG.info(L('STOP ROUTINE LOAD OK', name=routine_load_job_name))
            show = self.execute('SHOW ALL ROUTINE LOAD FOR %s' % job_name)
            LOG.info(L('SHOW STOPPED ROUTINE LOAD', ret=show))
            return ret == ()
        except Exception as e:
            LOG.info(L('STOP ROUTINE LOAD ERROR', name=routine_load_job_name, msg=str(e)))
            show = self.execute('SHOW ALL ROUTINE LOAD FOR %s' % job_name)
            LOG.info(L('SHOW STOPPED ROUTINE LOAD', ret=show))
            return False

    def show_routine_load(self, routine_load_job_name=None, database_name=None, is_all=False):
        """show routine load"""
        if is_all is False:
            all_word = ''
        else:
            all_word = 'ALL'
        sql = 'SHOW ROUTINE LOAD'
        if routine_load_job_name is None:
            routine_load_job_name = ''
            sql = 'SHOW {all} ROUTINE LOAD'.format(all=all_word)
        else:
            if database_name is not None:
                routine_load_job_name = '%s.%s' % (database_name, routine_load_job_name)
            else:
                routine_load_job_name = routine_load_job_name
            sql = 'SHOW {all} ROUTINE LOAD FOR {routine_load_job}'.format(all=all_word,
                                                                          routine_load_job=routine_load_job_name)
        try:
            ret = self.execute(sql)
            return ret
        except Exception as e:
            LOG.info(L('SHOW ROUTINE LOAD ERROR', msg=str(e)))
            return None

    def show_routine_load_task(self, routine_load_job_name):
        """show routine load task"""
        sql = 'SHOW ROUTINE LOAD TASK WHERE JOBNAME="%s"' % routine_load_job_name
        ret = self.execute(sql)
        return ret

    def get_routine_load_state(self, routine_load_job_name, database_name=None):
        """get routine load state"""
        ret = self.show_routine_load(routine_load_job_name, database_name=database_name)
        if ret == () or ret is None:
            ret = self.show_routine_load(routine_load_job_name, database_name=database_name,
                                         is_all=True)
            if ret == () or ret is None:
                return None
        LOG.info(L('GET ROUTINE LOAD STATE', state=palo_job.RoutineLoadJob(ret[0]).get_state()))
        return palo_job.RoutineLoadJob(ret[0]).get_state()

    def wait_routine_load_state(self, routine_load_job_name, state='RUNNING', timeout=600, database_name=None):
        """

        Args:
            routine_load_job_name: string, routine load job name
            state: string, 'NEED_SCHEDUL', 'PAUSE', 'RUNNING', 'STOPPED'
            timeout: int, timeout time

        Returns:

        """
        while timeout > 0:
            job_state = self.get_routine_load_state(routine_load_job_name, database_name=database_name)
            if job_state == state:
                return True
            else:
                time.sleep(1)
                timeout -= 1

    def show_tablet(self, table_name=None, database_name=None, tablet_id=None, partition_list=None):
        """
        SHOW TABLETS
        [FROM [db_name.]table_name | tablet_id] [partiton(partition_name_1, partition_name_1)]
        [where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
        [order by order_column]
        [limit [offset,]size]
        """
        if table_name is not None and tablet_id is not None:
            return None
        if table_name:
            if database_name is not None:
                table_name = '%s.%s' % (database_name, table_name)
            sql = 'SHOW TABLETS FROM %s' % table_name
            if partition_list:
                sql = '%s PARTITION (%s)' % (sql, ','.join(partition_list))
        else:
            sql = 'SHOW TABLET %s' % tablet_id
        ret = self.execute(sql)
        return ret

    def explain_query(self, sql):
        """explain sql"""
        sql = 'EXPLAIN %s' % sql
        ret = self.execute(sql)
        return ret

    def show_txn(self, txn_id, database_name=None):
        """help show TRANSACTION for more msg"""
        if database_name is None:
            sql = "SHOW TRANSACTION WHERE id = %s" % txn_id
        else:
            sql = "SHOW TRANSACTION FROM %s WHERE id = %s" % (database_name, txn_id)
        ret = self.execute(sql)
        return ret

    def wait_txn(self, txn_id, database_name=None, status='VISIBLE', timeout=600):
        """wait txn util the status"""
        while timeout > 0:
            txn = self.show_txn(txn_id, database_name)
            txn_status = palo_job.TransactionInfo(txn[0]).get_transaction_status()
            if txn_status == status:
                LOG.info(L('GET TXN STATUS', txn_id=txn_id, status=txn_status))
                return True
            elif txn_status == 'VISIBLE' or txn_status == 'ABORT':
                LOG.info(L('GET TXN STATUS', txn_id=txn_id, status=txn_status))
                return False
            else:
                time.sleep(1)
                timeout -= 1
        LOG.info(L('GET TXN STATUS TIMEOUT', txn_id=txn_id))
        return False

    def create_bitmap_index_table(self, table_name, bitmap_index_name, index_column_name,
                                  storage_type=None, database_name=None, create_format=1,
                                  is_wait=False, cluster_name=None):
        """
        Create a bitmap index
        """
        database_name = self.database_name if database_name is None else database_name
        if create_format == 1:
            sql = 'ALTER TABLE %s.%s ADD INDEX %s (%s) USING BITMAP' % (database_name, table_name, \
                                                                        bitmap_index_name, index_column_name)
        elif create_format == 2:
            sql = 'CREATE INDEX %s ON %s.%s (%s) USING BITMAP' % (bitmap_index_name, database_name, \
                                                                  table_name, index_column_name)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L("CREATE BITMAP INDEX fail.", database_name=database_name, \
                       table_name=table_name, \
                       bitmap_index_name=bitmap_index_name))
            return False
        ret = True
        if is_wait:
            ret = self.wait_table_schema_change_job(table_name, cluster_name=cluster_name,
                                                    database_name=database_name)
        LOG.info(L("CREATE BITMAP INDEX succ.", database_name=database_name, \
                   table_name=table_name, \
                   bitmap_index_name=bitmap_index_name))
        return ret

    def drop_bitmap_index_table(self, table_name, bitmap_index_name,
                                storage_type=None, database_name=None, create_format=1,
                                is_wait=False, cluster_name=None):
        """
        Drop a bitmap index
        """
        database_name = self.database_name if database_name is None else database_name
        if create_format == 1:
            sql = 'ALTER TABLE %s.%s DROP INDEX %s' % (database_name, table_name, bitmap_index_name)
        elif create_format == 2:
            sql = 'DROP INDEX %s ON %s.%s' % (bitmap_index_name, database_name, table_name)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L("DROP BITMAP INDEX fail.", database_name=database_name, \
                       table_name=table_name, \
                       bitmap_index_name=bitmap_index_name))
            return False
        ret = True
        if is_wait:
            ret = self.wait_table_schema_change_job(table_name, cluster_name=cluster_name,
                                                    database_name=database_name)
        LOG.info(L("DROP BITMAP INDEX succ.", database_name=database_name, \
                   table_name=table_name, \
                   bitmap_index_name=bitmap_index_name))
        return ret

    def get_bitmap_index_list(self, table_name, database_name=None):
        """
        Get index list from table
        """
        database_name = self.database_name if database_name is None else database_name
        sql = "SHOW INDEX FROM %s.%s" % (database_name, table_name)
        ret = self.execute(sql)
        return ret

    def is_exists_index_in_table(self, index_name, index_name_col, table_name, database_name=None):
        """
        return True if index exists in table, else return False
        """
        database_name = self.database_name if database_name is None else database_name
        index_list = self.get_bitmap_index_list(table_name, database_name)
        if not index_list:
            return False
        for each_index_info in index_list:
            job_get_index = palo_job.TableIndexInfo(each_index_info)
            job_index_name = job_get_index.get_key_name()
            job_index_column = job_get_index.get_column_name()
            job_index_type = job_get_index.get_index_type()
            if index_name == job_index_name and index_name_col == job_index_column and \
                    job_index_type == "BITMAP":
                return True
        return False

    def select_into(self, query, output_file, broker, property=None, format_as=None):
        """
        broker is BrokerInfo class
        property: dict, for csv, eg.{"column_separator": ",", "line_delimiter": "\n", "max_file_size": "100MB"}
        query_stmt
        INTO OUTFILE "file:///path/to/file_prefix"
        FORMAT AS CSV|PARQUET
        PROPERTIES
        (broker_propterties & other_properties);
        eg:
        SELECT * FROM tbl
        INTO OUTFILE "hdfs:/path/to/result_"
        FORMAT AS CSV
        PROPERTIELS
        (
        "broker.name" = "my_broker",
        "broker.hadoop.security.authentication" = "kerberos",
        "broker.kerberos_principal" = "doris@YOUR.COM",
        "broker.kerberos_keytab" = "/home/doris/my.keytab"
        "column_separator" = ",",
        "line_delimiter" = "\n",
        "max_file_size" = "100MB"
        );
        """
        sql = '{query} INTO OUTFILE "{outfile}" {format_as} PROPERTIES ({properties}) '
        if format_as is None:
            format_as = ''
        else:
            format_as = 'FORMAT AS %s' % format_as
        broker_property = broker.to_select_into_broker_property_str()
        if property is not None:
            into_properties = ''
            for k, v in property.items():
                into_properties += ' "%s" = "%s",' % (k, v)
            p = into_properties.strip(',')
            property = broker_property + ',' + p
        else:
            property = broker_property
        sql = sql.format(query=query, outfile=output_file, format_as=format_as,
                         properties=property)
        LOG.info(L('SELECT INTO.', sql=sql))
        rows, ret = self.execute(sql, True)
        LOG.info(L('SELECT INTO ret.', ret=ret))
        return ret

    def set_variables(self, k, v, is_global=False):
        """
        set variables
        如果为global需要重新connect，show的时候才会看到新的设置
        """
        if is_global:
            sql = 'SET GLOBAL %s=%s' % (k, v)
        else:
            sql = 'SET %s=%s' % (k, v)
        ret = self.execute(sql)
        return ret == ()

    def show_variables(self, prefix=None):
        """
        show variables
        """
        if prefix:
            sql = 'SHOW VARIABLES LIKE "%%%s%%"' % prefix
        else:
            sql = 'SHOW VARIABLES'
        ret = self.execute(sql)
        return ret

    def wait_routine_load_commit(self, routine_load_job_name, committed_expect_num, timeout=600):
        """wait task committed"""
        print('expect commited rows: %s\n' % committed_expect_num)
        while timeout > 0:
            ret = self.show_routine_load(routine_load_job_name)
            routine_load_job = palo_job.RoutineLoadJob(ret[0])
            loaded_rows = routine_load_job.get_loaded_rows()
            print(loaded_rows)
            if str(loaded_rows) == str(committed_expect_num):
                time.sleep(3)
                return True
            timeout -= 3
            time.sleep(3)
        return False

    def enable_feature_batch_delete(self, table_name, database_name=None, is_wait=True):
        """enable feature batch delete"""
        if database_name is None:
            sql = 'ALTER TABLE %s ENABLE FEATURE "BATCH_DELETE"' % table_name
        else:
            sql = 'ALTER TABLE %s.%s ENABLE FEATURE "BATCH_DELETE"' % (database_name, table_name)
        ret = self.execute(sql)
        if is_wait:
            ret = self.wait_table_schema_change_job(table_name, database_name)
            return ret
        return ret == ()

    def truncate(self, table_name, partition_list=None, database_name=None):
        """truncate table / partition"""
        if database_name is None:
            sql = 'TRUNCATE TABLE %s' % table_name
        else:
            sql = 'TRUNCATE TABLE %s.%s' % (database_name, table_name)
        if partition_list is not None:
            sql = '%s PARTITION (%s)' % (sql, ','.join(partition_list))
        ret = self.execute(sql)
        return ret == ()

    def commit(self):
        """commit"""
        ret = self.execute('COMMIT')
        return ret == ()

    def begin(self):
        """begin"""
        ret = self.execute('BEGIN')
        return ret == ()

    def rollback(self):
        """rollback"""
        ret = self.execute('ROLLBACK')
        return ret == ()

    def update(self, table_name, set_list, where_clause=None, database_name=None):
        """
        ref: UPDATE table_reference SET assignment_list [WHERE where_condition]
        table_name: str
        set_list: ['k1=2', 'k3=k3+1']
        where_clause: str or ['k1 > 0'], 当是list的时候，使用and进行连接
        """
        if database_name is not None:
            table_name = '%s.%s' % (database_name, table_name)
        if isinstance(set_list, list):
            set_ref = ','.join(set_list)
        else:
            set_ref = set_list

        if where_clause is not None:
            if isinstance(where_clause, str):
                where_ref = 'WHERE %s' % where_clause
            elif isinstance(where_clause, list):
                where_ref = 'WHERE %s' % ' AND '.join(where_clause)
            # else: pass
        else:
            where_ref = ''

        sql = 'UPDATE {tb} SET {set_ref} {where_ref}'.format(tb=table_name,
                                                             set_ref=set_ref,
                                                             where_ref=where_ref)
        ret = self.execute(sql)
        return ret == ()

    def admin_show_config(self, key=None):
        """ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"]"""
        if key is None:
            sql = 'ADMIN SHOW FRONTEND CONFIG'
        else:
            sql = 'ADMIN SHOW FRONTEND CONFIG LIKE "%s"' % key
        return self.execute(sql)

    def show_dynamic_partition_tables(self, database_name=None):
        """
        get dynamic partition table families
        """
        if database_name is None:
            sql = "SHOW DYNAMIC PARTITION TABLES"
            result = self.execute(sql)
            return result
        sql = "SHOW DYNAMIC PARTITION TABLES FROM %s" % database_name
        result = self.execute(sql)
        return result

    def get_comment(self, database_name, table_name):
        """
        get table comment
        """
        sql = "select TABLE_COMMENT from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" \
              % (database_name, table_name)
        ret = self.execute(sql)
        return ret[0][0]

    def get_column_comment(self, table_name, column_name):
        """
        get column comment
        """
        sql = "show full columns from %s" % table_name
        columns = self.execute(sql)
        for column in columns:
            if column[0] == column_name:
                return column[8]

    def create_sync_job(self, table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip,
                        columns=None, partitions=None, canal_port='11111', destination='example', batch_size='8192',
                        username='', password='', is_wait=True):
        """
        create sync job
        table_name: str or list，导入到doris的表 eg: 'table_1', ['table_1', 'table_2']
        database_name: str，binlog任务所在的doris数据库
        mysql_table_name: str or list，导入的MySQL表，需与doris表一一对应 eg:['mysql_table_1', 'mysql_table_2']
        mysql_database_name: list，导入的MySQL表的数据库
        columns: list or None，指定列映射，eg: ['k1', 'k2'], [['k1', 'k2'], ['k2', 'v1']] 
        partitions: list or None，指定导入分区，eg: ['p1', 'p2'], [[], ['p1', 'p3']]
        
        """
        if not isinstance(table_name, list):
            table_name = [table_name, ]
        if not isinstance(mysql_table_name, list):
            mysql_table_name = [mysql_table_name, ]
        if not isinstance(mysql_database_name, list):
            mysql_database_name = [mysql_database_name, ]
        if columns is not None and not isinstance(columns[0], list):
            columns = [columns]
        if partitions is not None and not isinstance(partitions[0], list):
            partitions = [partitions]
        if len(table_name) != len(mysql_table_name):
            LOG.info(L('doris tables are not corresponding to mysql tables'))
            return False
        channel_desc = ""
        for i in range(len(table_name)):
            column_desc = ''
            if columns is not None and columns[i] is not []:
                for column in columns[i]:
                    column_desc = '%s%s,' % (column_desc, column)
                column_desc = '(%s)' % column_desc[:-1]
            partition_desc = ''
            if partitions is not None and partitions[i] is not []:
                if len(partitions[i]) > 1:
                    for partition in partitions[i]:
                        partition_desc = '%s%s,' % (partition_desc, partition)
                    partition_desc = 'PARTITIONS (%s)' % partition_desc[:-1]
                else:
                    partition_desc = 'PARTITION (%s)' % partitions[i][0]
            else:
                partition_desc = ''
            channel_desc = "%s FROM %s.%s INTO %s %s %s," % (channel_desc, mysql_database_name[i], \
                                                             mysql_table_name[i], table_name[i], partition_desc,
                                                             column_desc)
        sql = "CREATE SYNC %s.%s (%s)" \
              "FROM BINLOG (" \
              "'type' = 'canal'," \
              "'canal.server.ip' = '%s'," \
              "'canal.server.port' = '%s'," \
              "'canal.destination' = '%s'," \
              "'canal.batchSize' = '%s'," \
              "'canal.username' = '%s'," \
              "'canal.password' = '%s')" % (database_name, job_name, channel_desc[:-1], canal_ip, canal_port, \
                                            destination, batch_size, username, password)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L('CREATE SYNC JOB fail.', job_name=job_name, database_name=database_name))
            return False
        if is_wait:
            ret = self.wait_binlog_state(job_name)
            if not ret:
                LOG.info(L('CREATE SYNC JOB fail.', database_name=database_name, job_name=job_name))
                return False
        LOG.info(L('CREATE SYNC JOB succ.', database_name=database_name, job_name=job_name))
        return True

    def wait_binlog_state(self, job_name, state='RUNNING'):
        """
        wait modify binlog job state
        """
        job_state = self.get_sync_job_state(job_name)
        if job_state == state:
            LOG.info(L('SYNC JOB %s' % state))
            return True
        timeout = 10
        while timeout > 0:
            time.sleep(2)
            job_state = self.get_sync_job_state(job_name)
            if job_state == state:
                LOG.info(L('SYNC JOB %s' % state))
                return True
            else:
                timeout -= 1
        LOG.info(L('SYNC JOB STATE ERROR', EXPECTED=state, ACTUAL=job_state))
        return False

    def pause_sync_job(self, job_name, database_name=None, is_wait=True):
        """
        pause sync job
        """
        if database_name is None:
            sql = 'PAUSE SYNC JOB %s' % job_name
        else:
            sql = 'PAUSE SYNC JOB %s.%s' % (database_name, job_name)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L('PAUSE SYNC JOB fail.', job_name=job_name, database_name=database_name))
            return False
        if is_wait:
            ret = self.wait_binlog_state(job_name, 'PAUSED')
            if not ret:
                LOG.info(L('PAUSE SYNC JOB fail.', database_name=database_name, job_name=job_name))
                return False
        LOG.info(L('PAUSE SYNC JOB succ.', database_name=database_name, job_name=job_name))
        return True

    def resume_sync_job(self, job_name, database_name=None, is_wait=True):
        """
        resume sync job
        """
        if database_name is None:
            sql = 'RESUME SYNC JOB %s' % job_name
        else:
            sql = 'RESUME SYNC JOB %s.%s' % (database_name, job_name)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L('RESUME SYNC JOB fail.', job_name=job_name, database_name=database_name))
            return False
        if is_wait:
            ret = self.wait_binlog_state(job_name)
            if not ret:
                LOG.info(L('RESUME SYNC JOB fail.', database_name=database_name, job_name=job_name))
                return False
        LOG.info(L('RESUME SYNC JOB succ.', database_name=database_name, job_name=job_name))
        return True

    def stop_sync_job(self, job_name, database_name=None, is_wait=True):
        """
        stop sync job
        """
        if database_name is None:
            sql = 'STOP SYNC JOB %s' % job_name
        else:
            sql = 'STOP SYNC JOB %s.%s' % (database_name, job_name)
        ret = self.execute(sql)
        if ret != ():
            LOG.info(L('STOP SYNC JOB fail.', job_name=job_name, database_name=database_name))
            return False
        if is_wait:
            ret = self.wait_binlog_state(job_name, 'CANCELLED')
            if not ret:
                LOG.info(L('STOP SYNC JOB fail.', database_name=database_name, job_name=job_name))
                return False
        LOG.info(L('STOP SYNC JOB succ.', database_name=database_name, job_name=job_name))
        return True

    def get_sync_job_state(self, job_name):
        """
        get sync job state
        """
        sync_job_list = self.show_sync_job()
        condition_col_idx = palo_job.SyncJobInfo.JobName
        retrun_clo_idx = palo_job.SyncJobInfo.State
        return util.get_attr_condition_value(sync_job_list, condition_col_idx, job_name, retrun_clo_idx)

    def show_sync_job(self, database_name=None):
        """
        get sync job information
        """
        if database_name is None:
            sql = "SHOW SYNC JOB"
            result = self.execute(sql)
            return result
        sql = "SHOW SYNC JOB FROM %s" % database_name
        result = self.execute(sql)
        return result

    def set_frontend_config(self, config, value):
        """
        admin set frontend config
        """
        sql = 'ADMIN SET FRONTEND CONFIG ("%s" = "%s")' % (config, value)
        return self.execute(sql)

    def get_partition_replica_allocation(self, table_name, partition_name, database_name=None):
        """
        get table family replica allocation
        """
        partition = self.get_partition(table_name, partition_name, database_name)
        if partition:
            return partition[palo_job.PartitionInfo.ReplicaAllocation]
        else:
            return None

    def modify_resource_tag(self, host_name, port, tag_location):
        """
        tag_location: str eg. grout_a, dict example: {'tag.location': 'a', 'tag.compute': 'b', ...}
        修改be标签
        """
        if isinstance(tag_location, str):
            sql = "ALTER SYSTEM MODIFY BACKEND '%s:%s' SET ('tag.location'='%s')" % (host_name, port, tag_location)
        elif isinstance(tag_location, dict):
            sql = "ALTER SYSTEM MODIFY BACKEND '%s:%s' SET %s" % (host_name, port,
                                                                  util.convert_dict2property(tag_location))
        else:
            return None
        result = self.execute(sql)
        time.sleep(2)
        if result != ():
            LOG.info(L('MODIFY BACKEND FAIL', backend=host_name, port=port, tag_location=tag_location))
            return False
        return True

    def get_replica_backend_id(self, table_name):
        """get BackendId from replica status"""
        sql = "ADMIN SHOW REPLICA STATUS FROM %s" % table_name
        replica_status = self.execute(sql)
        column_idx = palo_job.ReplicaStatus.BackendId
        return util.get_attr(replica_status, column_idx)

    def admin_check_tablet(self, tablet_id_list):
        """admin check tablet"""
        sql = "ADMIN CHECK TABLE (%s) PROPERTIES('type'='consistency')" % ','.join(tablet_id_list)
        self.execute(sql)

    def admin_repair_table(self, table_name, partition_list=None):
        """admin repair table"""
        sql = "ADMIN REPAIR TABLE %s" % table_name
        if partition_list is not None:
            sql = '%s PARTITION (%s)' % (sql, ','.join(partition_list))
        self.execute(sql)

    def admin_diagnose_tablet(self, tablet_id):
        """admin diagnose tablet"""
        sql = "ADMIN DIAGNOSE TABLET %s" % tablet_id
        ret = self.execute(sql)
        return ret

    def get_resource_tag(self, host_ip):
        """通过be的IP获取be的标签信息"""
        backend_list = self.get_backend_list()
        for backend in backend_list:
            be = palo_job.BackendProcInfo(backend)
            if be.get_ip() == host_ip:
                return be.get_tag()
        return None

    def get_resource_tag_by_id(self, be_id):
        """获取backend tag by id"""
        be = self.get_backend(be_id)
        if not be:
            return None
        return palo_job.BackendProcInfo(be).get_tag()

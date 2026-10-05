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
  * @file test_sys_time_zone.py
  * 
  **************************************************************************/
"""

import os
import sys
import time
import datetime
import pytest

from data import partition as DATA
sys.path.append("../")
from lib import palo_config
from lib import palo_client
import pymysql
from lib import util
from lib import palo_job
from lib import common

config = palo_config.config
broker_name = config.broker_name
broker_property = config.broker_property
broker_info = palo_config.broker_info
repo_location = config.repo_location

LOG = palo_client.LOG
L = palo_client.L
repo_name = "backup_restore_repo_zone"


def setup_module():
    """
    setUp
    """
    global Palo_client
    global mysql_cursor
    Palo_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, 
                                         config.palo_db, config.fe_user, config.fe_password)
    Palo_client.init()
    master = Palo_client.get_master_host()
    Palo_client = common.get_client(master)

    connect = pymysql.connect(host=config.mysql_host, port=config.mysql_port,\
          user=config.mysql_user, passwd=config.mysql_password)
    mysql_cursor = connect.cursor()
    try:
        ret = Palo_client.create_repository(repo_name, broker_name, repo_location=repo_location,
              repo_properties=broker_property)
        assert ret, "palo create_repository failed"
    except Exception as e:
        print(str(e))
    sql = "set time_zone = '+08:00'"
    check2(sql)


def mysql_execute(sql, db=None):
    """
    连接mysql执行语句
    """
    try:
        if db is None:
            db = os.environ["MYSQL_DB"]
        mysql_cursor.execute("use %s" % db)
        mysql_cursor.execute(sql)
        LOG.info(L('mysql execute sql', db=db, sql=sql))
        return mysql_cursor.fetchall()
    except Exception as error:
        LOG.error(L('error', error=error))
        assert False, "execute error. %s" % str(error)


def get_timestamp(res):
    """将字符串转为时间戳, '18:01:10' -> 数字时间戳，这样好比较大小，不存在两个时间差1s，分钟对应的数值差1"""
    res1 = str(res[0][0])  
    timeArray = time.strptime(res1, "%H:%M:%S")
    timestamp = time.mktime(timeArray)
    LOG.info(L('get_timestamp, input, output', res=res, timestamp=timestamp))
    return timestamp


def check2(psql, msql=None):
    """palo & mysql execute sql
    :sql: execute sql
    :msql: mysql 执行的sql
    """
    if not msql:
        msql = psql
    LOG.info(L('palo sql', palo_sql=psql))
    res1= Palo_client.execute(psql)
    LOG.info(L('mysql sql', mysql_sql=msql))
    res2 = mysql_execute(msql)
    if "CURTIME" in psql:
        ##可能会差1s左右，故转为时间戳个数
        timestamp_res1 = get_timestamp(res1)
        timestamp_res2 = get_timestamp(res2)
        LOG.info(L('CURTIME sql : palo res; mysql res', \
            timestamp_res1=timestamp_res1, timestamp_res2=timestamp_res2))
        assert abs(timestamp_res1 - timestamp_res2) <= 1, "palo %s, mysql %s, \
            excepted res1 - res2 <= 1" % (timestamp_res1, timestamp_res2)
    else:    
        util.check(res1, res2)


def show_be_broker_time():
    """show be,broker time,把be、broker里和时间相关的变量,非空则放到list里，逐条比较是否相等
    show be：LastStartTime：上次启动的时间，不会一直变,格式2019-09-04 20:54:55 
    show broker：LastStartTime：上次启动的时间，不会一直变，格式2019-09-04 20:54:55
    """
    be_list = Palo_client.get_alive_backend_list()
    be_info = be_list[0]
    list = []
    LOG.info(L('be info is', be_info=be_info))
    be_start_time = palo_job.BackendProcInfo(be_info).get_backend_start_time()
    LOG.info(L('palo be start time', start_time=be_start_time))
    list.append(be_start_time)
    broker = Palo_client.get_broker_start_update_time()
    if broker:
        LOG.info(L('broker start_time', start_time=broker))
        list += [broker]
    return list


def unity_datetime_format(time, snapshot_flag=False):
    """统一datetime的格式"，snapshot_flag是否是snapshot，格式不一样"""
    LOG.info(L('unity datetime format', time=time, snapshot=snapshot_flag))
    try:
        if snapshot_flag:
            unity_time = datetime.datetime.strptime('%s' % time, '%Y-%m-%d-%H-%M-%S')
        else:
            unity_time = datetime.datetime.strptime('%s' % time, '%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        if snapshot_flag:
            unity_time = datetime.datetime.strptime('%s' % time, '%Y-%m-%d-%H-%M-%S.%f')
        else:
            unity_time = datetime.datetime.strptime('%s' % time, '%Y-%m-%d %H:%M:%S.%f')
    return unity_time


def recombinate_time(origin_time, offset_hour, offset_minutes):
    """在origin_time时间上叠加上offset_time时间
       -01:10 和+06:10 距离+08:00的分钟计算方式不一样
    """
    seconds = 0
    if offset_hour < 0:
       ##+09:10
       seconds = (offset_hour * 60 - offset_minutes) * 60
    elif offset_hour >= 8:
       ##-01:10
       seconds = (offset_hour * 60 + offset_minutes) * 60
    else:
       ##+06:10
       seconds = (offset_hour * 60 - offset_minutes) * 60
    offset_time = datetime.timedelta(days=0, seconds=seconds, microseconds=0)
    assert isinstance(offset_time, datetime.timedelta)
    offert_time = (origin_time - offset_time).isoformat()
    try:
        offert_time = datetime.datetime.strptime(offert_time, '%Y-%m-%dT%H:%M:%S')
    except ValueError as e:
        offert_time = datetime.datetime.strptime(offert_time, '%Y-%m-%dT%H:%M:%S.%f')
    return offert_time


def wait_for_integer_min():
    """Waiting time to judge in the same minute"""
    sql = "select SECOND(now())"
    ret = Palo_client.execute(sql)   
    if int(ret[0][0]) > 50:
        time.sleep(20) 


def check2_gap_time_zone(palo_res, mysql_res, zone, snapshot_flag=False):
    """比较两个tiemdate时间的差;如：2019-09-19 04:05:59 格式
    参数：palo_res, mysql_res，要对比的两个时间；zone 新设置的时区值"""
    res = get_day_and_second(zone)
    hour = 8 - int(res[0])
    second = int(res[1])
    for palo_data, mysql_data in zip(palo_res, mysql_res):
        if palo_data == 'N/A' or palo_data == '':
            ###没broker时，返回的是N/A
            assert palo_data == mysql_data, "err: excepted %s == %s" % (palo_data, mysql_data)
            continue
        unity_palo = unity_datetime_format(palo_data, snapshot_flag)
        unity_mysql = unity_datetime_format(mysql_data, snapshot_flag)

        ##小于+08:00，如+01:10，-03:10. 偏移量是正数
        palo_date = recombinate_time(unity_palo, hour, second)
        assert palo_date == unity_mysql, "res %s, excepted %s" % (palo_date, unity_mysql)


def get_day_and_second(date_delta):
    """从datetime里获取小时和分钟 eg:-08:10,取出 8和10"""
    if date_delta:
        res_list = date_delta.split(':')
        day = res_list[0]
        second = res_list[1]
        return day, second
    return False


def backup(repo_name, database_name, table_name, snapshot_label):
    """创建仓库，备份"""
    ret = Palo_client.backup(snapshot_label=snapshot_label, backup_list=[table_name],
                             repo_name=repo_name, database_name=database_name,
                             is_wait=True)
    LOG.info(L("backup res", snapshot_label=snapshot_label, repo_name=repo_name, \
         table_name=table_name, res=ret))
    assert ret


def restore(repo_name, database_name, table_name, snapshot_label):
    """恢复到新表里"""
    ##restore
    new_table_name = table_name + "_new"
    restore_list = list()
    restore_list.append('%s AS %s' % (table_name, new_table_name))
    ret = Palo_client.restore(snapshot_name=snapshot_label, repo_name=repo_name,
                              restore_list=restore_list, database_name=database_name,
                              is_wait=True)
    LOG.info(L("restore res", snapshot_label=snapshot_label, repo_name=repo_name, \
               table_name=table_name, res=ret))
    assert ret
    # check
    line1 = 'select * from %s order by k1' % table_name
    line2 = 'select * from %s order by k1' % new_table_name
    LOG.info(L('palo sql 1', palo_sql=line1))
    ret1 = Palo_client.execute(line1)
    LOG.info(L('palo sql 2', palo_sql=line2))
    ret2 = Palo_client.execute(line2)
    util.check(ret1, ret2)


def get_backup_time(backup_db):
    """获取备份相关时间信息"""
    ##backup
    snapshot_name = ''
    res_backup = ''
    time_list = []

    res_backup = Palo_client.show_backup(backup_db)
    LOG.info(L('show backup res', res_backup=res_backup))
    assert res_backup

    backup_info = res_backup[-1]
    time_list = [palo_job.BackupJob(backup_info).get_create_time(), \
         palo_job.BackupJob(backup_info).get_snap_finish_time(),\
         palo_job.BackupJob(backup_info).get_upload_finish_time(), \
         palo_job.BackupJob(backup_info).get_finished_time()]
    snapshot_name = palo_job.BackupJob(backup_info).get_snapshotName()
    return time_list, snapshot_name


def get_restore_time(restore_db):
    """获取备份恢复相关时间信息"""
    ##restore
    snapshot_name = ''
    res_backup = ''
    time_list = []

    res = Palo_client.show_restore(restore_db)
    LOG.info(L('show RESTORE res', res_restore=res))
    assert res
    time_list += [palo_job.RestoreJob(res[-1]).get_create_time(),\
        palo_job.RestoreJob(res[-1]).get_meta_prepare_time(),
        palo_job.RestoreJob(res[-1]).get_snapshot_finished_time(),
        palo_job.RestoreJob(res[-1]).get_down_load_finished(),
        palo_job.RestoreJob(res[-1]).get_finished()]
    return time_list, snapshot_name


def get_repo_time():
    """获取备repo相关时间信息"""
    ##repo
    time_list = []
    res = Palo_client.show_repository()
    assert  res
    LOG.info(L('show REPOSITORIES res', res=res[0]))
    time_list = [palo_job.RepoInfo(res[0]).get_create_time()]
    return time_list, palo_job.RepoInfo(res[0]).get_repo_name()


def get_snapshot_time(repo, snapshot_name=None):
    """获取备snapshot相关时间信息"""
    snap_res = Palo_client.show_snapshot(repo, snapshot_name)
    LOG.info(L('show snapshot repo, snapshot_name, res',\
        repo=repo, snapshot_name=snapshot_name, res=snap_res))
    assert snap_res
    time_list = [palo_job.SnapshotInfo(snap_res[0]).get_timestamp()]
    LOG.info(L('get snapshot time', time_list=time_list))
    return time_list


def create_table(database_name, table_name):
    """建表"""
    Palo_client.clean(database_name)
    Palo_client.create_database(database_name)
    Palo_client.use(database_name)
    sql = "drop table if exists %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = Palo_client.execute(sql)
    # 建表
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['100', '500', '1000', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k3',
                                               partition_name_list, partition_value_list)
    distribution_type_d = 'HASH(k1, k2, k5)'
    distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 31)
    Palo_client.create_table(table_name, DATA.schema_1, partition_info, distribution_info_d)
    assert Palo_client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert Palo_client.get_partition(table_name, partition_name)


def test_set_sys_time_zone():
    """
    {
    "title": "test_sys_time_zone.test_set_sys_time_zone",
    "describe": "设置系统变量system_time_zone。系统不允许，会报错",
    "tag": "function,p1,fuzz"
    }
    """
    """
    设置系统变量system_time_zone。系统不允许，会报错
    """
    zone = '+08:00'
    excepted = "%s variable '%s' is read-only. Use SET %s to assign the value"
    try:
        sql = "set system_time_zone = '%s'" % zone
        res = Palo_client.execute(sql)
        assert 0 == 1, "palo sql %s shoud not ok" % sql
    except Exception as err:
        LOG.info(L('test_set_sys_time_zone err', error=err))
        assert excepted in str(err), "diff: res %s, excepted %s" % (err, excepted)
    try:
        sql = "set global system_time_zone = '%s'" % zone
        Palo_client.execute(sql)
        assert 0 == 1, "palo sql %s shoud not ok" % sql
    except Exception as err:
        LOG.info(L('test_set_sys_time_zone err', error=err))
        assert excepted in str(err), "diff: res %s, excepted %s" % (err, excepted)


def test_set_time_zone():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone",
    "describe": "设置time_zone变量。测试点：多种方式；与system_time_zone不匹配等",
    "tag": "function,p1,fuzz"
    }
    """
    """
    设置time_zone变量。
    测试点：多种方式；与system_time_zone不匹配等
    """
    zone_ok = ['America/Los_Angeles', 'Asia/Shanghai', '-00:00', '+00:00', '-08:00', '+09:00',\
                 '-08:10', '+08:10',  '+12:00', '-12:00']
    zone_err = ['met', 'CMT', 'CTT', 'CAT','UTC','utc', 'CST', 'cst', '-12:01',\
        '+12:01', '-08:90', '+08:90', '09:00', 'SYSTEM']
    for zone in zone_ok:
        Palo_client.set_time_zone(zone)
        res_var = Palo_client.show_variables("time_zone")
        assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)

    excepted = "Unknown or incorrect time zone"
    for zone in zone_err:
        try:
            Palo_client.set_time_zone(zone)
            LOG.error(L('err res', zone=zone))
        except Exception as err:
            assert excepted in str(err), "res %s, excepted %s" % (str(err), excepted)
        try:
            Palo_client.set_time_zone(zone)
            LOG.error(L('err res', zone=zone))
        except Exception as err:
            assert excepted in str(err), "res %s, excepted %s" % (str(err), excepted)


def test_set_time_zone_show_proc():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_show_proc",
    "describe": "验证时区设置对show proc相关的时间显示",
    "tag": "function,p1"
    }
    """
    """
    验证时区设置对show proc相关的时间显示
    """
    zone_list = ['-01:10', '+03:30', '+09:01']
    for zone in zone_list:
        Palo_client.set_time_zone('+08:00')
        res_var = Palo_client.show_variables("time_zone")
        assert res_var[1][1] == "+08:00", "res %s, excepted +08:00" % res_var[1][1]
        time.sleep(5)
        res_before = show_be_broker_time()
        Palo_client.set_time_zone(zone)
        res_var = Palo_client.show_variables("time_zone")
        assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
        #varify_time_zone_correct(zone, Palo_client, False, zone)
        time.sleep(5)
        res_after = show_be_broker_time()
        assert res_after
        LOG.info(L('zone, before res, after res',\
            zone=zone, res_before=res_before, res_after=res_after))
        check2_gap_time_zone(res_before, res_after, zone)


def test_set_time_zone_show_backup():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_show_backup",
    "describe": "创建db，导入数据，备份,setUp里会设置时区为+08:00，有这些数据后，先获取时间信息，然后设置新时区，再次获取时间",
    "tag": "function,p1"
    }
    """
    """show backup_restore
    创建db，导入数据，备份
    setUp里会设置时区为+08:00，有这些数据后，先获取时间信息，然后设置新时区，再次获取时间
    """
    Palo_client.set_time_zone('+08:00')
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == "+08:00", "res %s, excepted +08:00" % res_var[1][1]

    database_name, table_name, index_name = util.gen_name_list()
    create_table(database_name, table_name)
    # 导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert Palo_client.batch_load(util.get_label(), data_desc_list, broker=broker_info)
    ##进行备份恢复
    snapshot_label = util.get_snapshot_label('backup')
    backup(repo_name, database_name, table_name, snapshot_label)
    ######第一次获取时间
    res_before, snapshot = get_backup_time(database_name)
    assert res_before
    ########set time zone
    zone = "+01:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    LOG.info(L('zone, res', zone=zone, res=res_var))
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    time.sleep(5)
    #######再次获取时间
    res_after, snapshot = get_backup_time(database_name)
    assert res_after
    LOG.info(L('zone, before res, after res',\
        zone=zone, res_before=res_before, res_after=res_after))
    check2_gap_time_zone(res_before, res_after, zone)
    Palo_client.clean(database_name)


def test_set_time_zone_show_backup_restore():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_show_backup_restore",
    "describe": "创建db，导入数据，备份，后恢复, setUp里会设置时区为+08:00，有这些数据后，先获取时间信息，然后设置新时区，再次获取时间",
    "tag": "function,p1"
    }
    """
    """show backup_restore
    创建db，导入数据，备份，后恢复
    setUp里会设置时区为+08:00，有这些数据后，先获取时间信息，然后设置新时区，再次获取时间
    """
    Palo_client.set_time_zone("+08:00")
    database_name, table_name, index_name = util.gen_name_list()
    create_table(database_name, table_name)
    # 导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert Palo_client.batch_load(util.get_label(), data_desc_list, broker=broker_info, is_wait=True)
    ##进行备份恢复
    snapshot_label = util.get_snapshot_label('backup_restore')
    backup(repo_name, database_name, table_name, snapshot_label)
    restore(repo_name, database_name, table_name, snapshot_label)
    ######第一次获取时间
    res_before, snapshot = get_restore_time(database_name)
    #有内容才继续
    assert res_before
    zone = "+01:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    LOG.info(L('zone, res', zone=zone, res=res_var))
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    time.sleep(5)
    #######再次获取时间
    res_after, snapshot = get_restore_time(database_name)
    assert res_after
    ###因为标准是+08:00，现在设置+01:00，时间相差7小时,0分钟
    LOG.info(L('zone, before res, after res, excepted',\
        zone=zone, res_before=res_before, res_after=res_after))
    check2_gap_time_zone(res_before, res_after, zone)
    Palo_client.clean(database_name)


def test_set_time_zone_show_repo():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_show_repo",
    "describe": "setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间",
    "tag": "function,p1"
    }
    """
    """show repo
    setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间
    """
    ##set time zone
    zone = "+08:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    res_before = get_repo_time()
    assert res_before
    ##set time zone
    zone = "+01:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    time.sleep(5)
    res_after = get_repo_time()
    assert res_after
    LOG.info(L('zone, before res, after res',\
        zone=zone, res_before=res_before, res_after=res_after))
    check2_gap_time_zone(res_before[0], res_after[0], zone)


def test_set_time_zone_show_snapshot():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_show_snapshot",
    "describe": "setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间",
    "tag": "function,p1,fuzz"
    }
    """
    """show snapshot
    setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间
    """
    ###snapshot, 与时间无关
    res_repo = get_repo_time()
    assert res_repo
    repo = res_repo[1]
    snapshot_name = None
    LOG.info(L('repo,snapshot_name', repo=repo, snapshot_name=snapshot_name))
    if repo:
        zone = "+08:00"
        Palo_client.set_time_zone(zone)
        res_var = Palo_client.show_variables("time_zone")
        assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
        res_before = get_snapshot_time(repo, snapshot_name)
        assert res_before
        ##set time zone
        zone = "+01:00"
        Palo_client.set_time_zone(zone)
        res_var = Palo_client.show_variables("time_zone")
        assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
        time.sleep(5)
        res_after = get_snapshot_time(repo, snapshot_name)
        assert res_after
        LOG.info(L('zone, before res, after res',\
            zone=zone, res_before=res_before, res_after=res_after))
        try:
            check2_gap_time_zone(res_before, res_after, zone, True)
            LOG.error(L('snapshot time zone varify err', res_before=res_before, res_after=res_after))
        except Exception as err:
            assert 'excepted' in str(err), "excepted excepted, err: %s" % str(err)
    else:
        raise pytest.skip('get zone failed')


def test_set_time_zone_alter_table():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_alter_table",
    "describe": "setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证对导schema change的影响。
    setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间
    """
    database_name, table_name, index_name = util.gen_name_list()
    #建表
    create_table(database_name, table_name)
    zone = "+08:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    ##alter table
    column_list = [('k11', 'INT KEY', None, '5')]
    ret = Palo_client.schema_change_add_column(table_name, column_list,\
        after_column_name='k1', is_wait_job=True)
    assert ret
    res_before = []
    res_after = []
    schema_change_job_list = Palo_client.get_table_schema_change_job_list(table_name, database_name)
    LOG.info(L('palo db, table, res', db=database_name, table=table_name, res=schema_change_job_list))
    res_before.append(schema_change_job_list[-1][palo_job.SchemaChangeJob.CreateTime])
    res_before.append(schema_change_job_list[-1][palo_job.SchemaChangeJob.FinishTime])
    ##set time zone
    zone = "+01:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    schema_change_job_list = Palo_client.get_table_schema_change_job_list(table_name, database_name)
    LOG.info(L('palo db, table, res', db=database_name, table=table_name, res=schema_change_job_list))
    res_after.append(schema_change_job_list[-1][palo_job.SchemaChangeJob.CreateTime])
    res_after.append(schema_change_job_list[-1][palo_job.SchemaChangeJob.FinishTime])
    ###check
    LOG.info(L('zone, before res, after res',\
        zone=zone, res_before=res_before, res_after=res_after))
    check2_gap_time_zone(res_before, res_after, zone)
    Palo_client.clean(database_name)


def test_set_time_zone_delete_job():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_delete_job",
    "describe": "设置time_zone变量,验证对删除任务的影响。setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证对删除任务的影响。
    setUp里会设置时区为+08:00，先获取时间信息，然后设置新时区，再次获取时间
    """
    database_name, table_name, index_name = util.gen_name_list()
    create_table(database_name, table_name)
    ##delete table
    zone = "CST"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    ret = Palo_client.delete(table_name, [('k1', '=', '3')], 'partition_a')
    assert ret
    res = Palo_client.show_delete(database_name)
    LOG.info(L('palo delete result', palo_result=res))
    delete_time_before = palo_job.DeleteJob(res[-1]).get_create_time()
    ##set time zone
    zone = "+01:00"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert  res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    res_2 = Palo_client.show_delete(database_name)
    assert res_2
    delete_time_after = palo_job.DeleteJob(res_2[-1]).get_create_time()
    LOG.info(L('zone, before res, after res',\
        zone=zone, res_before=[delete_time_before], res_after=[delete_time_after]))
    print(delete_time_before)
    print(delete_time_after)
    check2_gap_time_zone([delete_time_before], [delete_time_after], zone)
    Palo_client.clean(database_name)


def test_set_time_zone_select_time_function():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_select_time_function",
    "describe": "设置time_zone变量,验证对时间相关函数的影响。测试点：5个函数,now(),curtime(),UNIX_TIMESTAMP() FROM_UNIXTIME(), CONVERT_TZ()",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证对时间相关函数的影响。
    测试点：5个函数,now(),curtime(),UNIX_TIMESTAMP() FROM_UNIXTIME(), CONVERT_TZ()
    """
    zone_list = ["+06:00", "+09:10", "-01:10", "+08:00"]
    for zone in zone_list:
        sql = "set time_zone = '%s'" % zone
        check2(sql)
        res_var = Palo_client.show_variables("time_zone")
        assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
        #UNIX_TIMESTAMP
        sql = "select UNIX_TIMESTAMP('1970-01-01 08:00:00'), UNIX_TIMESTAMP('1970-01-01 18:10:10')"
        check2(sql)
        ##curtime
        sql = "SELECT CURTIME()"
        check2(sql)
        ##FROM_UNIXTIME
        sql = "select FROM_UNIXTIME(123.4, '%Y-%m-%d %H:%i:%s'), FROM_UNIXTIME(-123,\
            '%Y-%m-%d %H:%i:%s'), FROM_UNIXTIME(+123, '%Y-%m-%d %H:%i:%s')"
        check2(sql)
        #CONVERT_TZ
        sql = "SELECT CONVERT_TZ('2004-01-01 12:00:00','+01:00','+10:00')"
        check2(sql)
        ##now
        wait_for_integer_min()
        sql = "select date(now()), HOUR(now()), MINUTE(now()), DAY(now()),\
             MONTH(now()), QUARTER(now()), YEAR(now())"
        check2(sql)


def test_set_time_zone_insert_load_select():
    """
    {
    "title": "test_sys_time_zone.test_set_time_zone_insert_load_select",
    "describe": "设置time_zone变量,验证对导入和查询时间相关函数的影响。测试点：导入数据里含now,curtime()和不受影响的函数utc_timestamp，验证查询结果",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证对导入和查询时间相关函数的影响。
    测试点：导入数据里含now,curtime()和不受影响的函数utc_timestamp，验证查询结果
    """
    database_name, table_name, index_name = util.gen_name_list()
    Palo_client.clean(database_name)
    Palo_client.create_database(database_name)
    Palo_client.use(database_name)
    zone = "CST"
    Palo_client.set_time_zone(zone)
    res_var = Palo_client.show_variables("time_zone")
    assert res_var[1][1] == zone, "res %s, excepted %s" % (res_var[1][1], zone)
    sql = "drop table if exists %s" % table_name
    LOG.info(L('palo&mysql sql', sql=sql))
    res = Palo_client.execute(sql)
    mysql_cursor.execute(sql)
    sql = 'create table %s(k1 tinyint, k2 smallint NULL, k3 tinyint NULL, k4 tinyint NULL,\
        k5 date NULL, k6 datetime NULL, k8 date NULL, k9 datetime NULL, \
        k7 varchar(20) NULL, k10 double sum, k11 float sum) engine=olap \
        distributed by hash(k1) buckets 5 properties("storage_type"="column")' % table_name
    msql = 'create table %s(k1 tinyint, k2 smallint, k3 tinyint NULL, k4 tinyint NULL,\
        k5 date, k6 datetime, k8 date, k9 datetime, k7 varchar(20),\
         k10 double, k11 float)' % table_name
    check2(sql, msql)
    wait_for_integer_min()
    insert_sql = "insert into %s values (1, HOUR(now()), MINUTE(now()),\
        UNIX_TIMESTAMP('1970-01-01 08:00:00'), date(now()),\
        '2019-09-09 09:00:00', date(now())," % table_name
    insert_sql += "FROM_UNIXTIME(+123, '%Y-%m-%d %H:%i:%s'), '7', 10.10, 11.11)"
    check2(insert_sql)
    select_sql = "select * from %s" % table_name
    check2(select_sql)
    Palo_client.clean(database_name)


def teardown_module():
    """清理环境"""
    sql = "set time_zone = '+08:00'"
    check2(sql)


if __name__ == "__main__":
    print("test")
    setup_module()
    test_set_time_zone_select_time_function()
    test_set_time_zone_insert_load_select()

    

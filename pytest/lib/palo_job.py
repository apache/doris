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

############################################################################
#
#   @file palo_job.py 
#   @date 2018-05-08 14:42:27
#   @brief 记录palo元数据中的job信息
#
#############################################################################

"""Palo meta info"""

import socket

class RepoInfo(object):
    """
    show repositories
            RepoId：     唯一的仓库ID
            RepoName：   仓库名称
            CreateTime： 第一次创建该仓库的时间
            IsReadOnly： 是否为只读仓库
            Location：   仓库中用于备份数据的根目录
            Broker：     依赖的 Broker
            ErrMsg：     Palo 会定期检查仓库的连通性，如果出现问题，这里会显示错误信息
    """
    RepoId = 0
    RepoName = 1
    CreateTime = 2
    IsReadOnly = 3
    Location = 4
    Broker = 5
    Type = 6
    ErrMsg = 7

    def __init__(self, repo_info):
        self.repo_info = repo_info

    def get_repo_id(self):
        """get repoid"""
        return self.repo_info[RepoInfo.RepoId]

    def get_repo_name(self):
        """get repo name"""
        return self.repo_info[RepoInfo.RepoName]

    def get_isReadOnly(self):
        """get isreadonly"""
        return self.repo_info[RepoInfo.IsReadOnly]

    def get_location(self):
        """get repo location"""
        return self.repo_info[RepoInfo.Location]

    def get_broker(self):
        """get broker"""
        return self.repo_info[RepoInfo.Broker]

    def get_errmsg(self):
        """get errmsg"""
        return self.repo_info[RepoInfo.ErrMsg]

    def get_create_time(self):
        """get create time"""
        return self.repo_info[RepoInfo.CreateTime]


class BackupJob(object):
    """
    JobId：                  唯一作业id
    SnapshotName：           备份的名称
    DbName：                 所属数据库
    State：                  当前阶段
        PENDING：        提交作业后的初始状态
        SNAPSHOTING：    执行快照中
        UPLOAD_SNAPSHOT：快照完成，准备上传
        UPLOADING：      快照上传中
        SAVE_META：      将作业元信息保存为本地文件
        UPLOAD_INFO：    上传作业元信息
        FINISHED：       作业成功
        CANCELLED：      作业失败
    BackupObjs：             备份的表和分区
    CreateTime：             任务提交时间
    SnapshotFinishedTime：   快照完成时间
    UploadFinishedTime：     快照上传完成时间
    FinishedTime：           作业结束时间
    UnfinishedTasks：        在 SNAPSHOTING 和 UPLOADING 阶段会显示还未完成的子任务id
    Status：                 如果作业失败，显示失败信息
    Timeout：                作业超时时间，单位秒
    """
    JobId = 0
    SnapshotName = 1
    DbName = 2
    State = 3
    BackupObjs = 4
    CreateTime = 5
    SnapshotFinishedTime = 6
    UploadFinishedTime = 7
    FinishedTime = 8
    UnfinishedTasks = 9
    Progress = 10
    TaskErrMsg = 11
    Status = 12
    Timeout = 13

    def __init__(self, backup_job):
        self.backup_job = backup_job

    def get_state(self):
        """get state"""
        return self.backup_job[BackupJob.State]

    def get_dbName(self):
        """get dbname"""
        return self.backup_job[BackupJob.DbName]

    def get_snapshotName(self):
        """get snapshot name"""
        return self.backup_job[BackupJob.SnapshotName]

    def get_create_time(self):
        """get_create_time"""
        return self.backup_job[BackupJob.CreateTime]

    def get_snap_finish_time(self):
        """get_snap_finish_time"""
        return self.backup_job[BackupJob.SnapshotFinishedTime]

    def get_upload_finish_time(self):
        """get_upload_finish_time"""
        return self.backup_job[BackupJob.UploadFinishedTime]

    def get_finished_time(self):
        """get_finished_time"""
        return self.backup_job[BackupJob.FinishedTime]


class RestoreJob(object):
    """
    show restore
            JobId：                  唯一作业id
            Label：                  要恢复的备份的名称
            Timestamp：              要恢复的备份的时间版本
            DbName：                 所属数据库
            State：                  当前阶段
                PENDING：        提交作业后的初始状态
                SNAPSHOTING：    执行快照中
                DOWNLOAD：       快照完成，准备下载仓库中的快照
                DOWNLOADING：    快照下载中
                COMMIT：         快照下载完成，准备生效
                COMMITING：      生效中
                FINISHED：       作业成功
                CANCELLED：      作业失败
            AllowLoad：              恢复时是否允许导入（当前不支持）
            ReplicationNum：         指定恢复的副本数
            ReplicaAllocation
            RestoreJobs：            要恢复的表和分区
            CreateTime：             任务提交时间
            MetaPreparedTime：       元数据准备完成时间
            SnapshotFinishedTime：   快照完成时间
            DownloadFinishedTime：   快照下载完成时间
            FinishedTime：           作业结束时间
            UnfinishedTasks：        在 SNAPSHOTING、DOWNLOADING 和 COMMITING 阶段会显示还未完成的子任务id
            Status：                 如果作业失败，显示失败信息
            Timeout：                作业超时时间，单位秒
    """
    JobId = 0
    Label = 1
    Timestamp = 2
    DbName = 3
    State = 4
    AllowLoad = 5
    ReplicationNum = 6
    ReplicaAllocation = 7
    ReserveReplica = 8
    RestoreObjs = 9
    CreateTime = 10
    MetaPreparedTime = 11
    SnapshotFinishedTime = 12
    DownloadFinishedTime = 13
    FinishedTime = 14
    UnfinishedTaskes = 15
    Progress = 16
    TaskErrMsg = 17
    Status = 18
    Timout = 19
    
    def __init__(self, restore_job):
        self.restore_job = restore_job

    def get_dbName(self):
        """get dbname"""
        return self.restore_job[RestoreJob.DbName]

    def get_state(self):
        """get state"""
        return self.restore_job[RestoreJob.State]

    def get_allowLoad(self):
        """get allowload"""
        return self.restore_job[RestoreJob.AllowLoad]

    def get_create_time(self):
        """get_create_time"""
        return self.restore_job[RestoreJob.CreateTime]

    def get_meta_prepare_time(self):
        """get MetaPreparedTime"""
        return self.restore_job[RestoreJob.MetaPreparedTime]

    def get_snapshot_finished_time(self):
        """get SnapshotFinishedTime"""
        return self.restore_job[RestoreJob.SnapshotFinishedTime]

    def get_down_load_finished(self):
        """get DownloadFinishedTime"""
        return self.restore_job[RestoreJob.DownloadFinishedTime]

    def get_finished(self):
        """get FinishedTime"""
        return self.restore_job[RestoreJob.FinishedTime]


class SnapshotInfo(object):
    """show snapshot on repo"""
    Snapshot = 0
    Timestamp = 1
    Status = 2

    def __init__(self, snapshot):
        self.snapshot = snapshot

    def get_snapshot_name(self):
        """get snapshot name"""
        return self.snapshot[SnapshotInfo.Snapshot]

    def get_timestamp(self):
        """get backup timestamp"""
        return self.snapshot[SnapshotInfo.Timestamp]


class SchemaChangeJob(object):
    """show alter table column same with show proc '/jobs/dbid/schema_change'"""
    JobId = 0
    TableName = 1
    CreateTime = 2
    FinishTime = 3
    IndexName = 4
    IndexId = 5
    OriginIndexId = 6
    SchemaVersion = 7
    TransactionId = 8
    State = 9
    Msg = 10
    Progress = 11
    Timeout = 12

    def __init__(self, schemachange_job):
        self.schema_change_job = schemachange_job

    def get_state(self):
        """get state"""
        return self.schema_change_job[SchemaChangeJob.State]

    def get_table_name(self):
        """get table name"""
        return self.schema_change_job[SchemaChangeJob.TableName]

    def get_msg(self):
        """get msg"""
        return self.schema_change_job[SchemaChangeJob.Msg]

    def get_create_time(self):
        """get CreateTime"""
        return self.schema_change_job[SchemaChangeJob.CreateTime]

    def get_finish_time(self):
        """get FinishTime"""
        return self.schema_change_job[SchemaChangeJob.FinishTime]


class RollupJob(object):
    """show alter table rolllup same with show proc '/jobs/dbid/rollup'"""
    JobId = 0
    TableName = 1
    CreateTime = 2
    FinishedTime = 3
    BaseIndexName = 4
    RollupIndexName = 5
    RollupId = 6
    TransactionId = 7
    State = 8
    Msg = 9
    Progress = 10
    Timeout = 11

    def __init__(self, rollup_job):
        self.rollup_job = rollup_job

    def get_state(self):
        """get state"""
        return self.rollup_job[RollupJob.State]

    def get_table_name(self):
        """get table name"""
        return self.rollup_job[RollupJob.TableName]

    def get_index_name(self):
        """get index name"""
        return self.rollup_job[RollupJob.RollupIndexName]

    def get_msg(self):
        """get msg"""
        return self.rollup_job[RollupJob.Msg]


class DeleteJob(object):
    """show delete different with show proc '/jobs/dbid/delete', this show delete result"""
    TableName = 0
    PartitionName = 1
    CreateTime = 2
    DeleteCondition = 3
    State = 4

    def __init__(self, delete_job):
        self.delete_job = delete_job

    def get_table_name(self):
        """get table name"""
        return self.delete_job[DeleteJob.TableName]

    def get_partition_name(self):
        """get partition name"""
        return self.delete_job[DeleteJob.PartitionName]

    def get_create_time(self):
        """get CreateTime"""
        return self.delete_job[DeleteJob.CreateTime]

    def get_state(self):
        """get state"""
        return self.delete_job[DeleteJob.State]


class LoadJob(object):
    """show load same with show proc '/jobs/dbid/load'"""
    JobId = 0
    Label = 1
    State = 2
    Progress = 3
    Type = 4
    EtlInfo = 5
    TaskInfo = 6
    Errormsg = 7
    CreateTime = 8
    EtlStartTime = 9
    EtlFinishTime = 10
    LoadStartTime = 11
    LoadFinishTime = 12
    URL = 13 
    JobDetails = 14
    
    def __init__(self, load_job):
        self.load_job = load_job

    def get_label(self):
        """get label"""
        return self.load_job[self.Label]

    def get_state(self):
        """get state"""
        return self.load_job[self.State]

    def get_errormsg(self):
        """get errormsg"""
        return self.load_job[self.Errormsg]

    def get_url(self):
        """get url"""
        return self.load_job[self.URL]

    def get_etlinfo(self):
        """get etl info"""
        return self.load_job[self.EtlInfo]

    def get_taskinfo(self):
        """get task info"""
        return self.load_job[self.TaskInfo]


class ExportJob(object):
    """show export same with show proc '/jobs/dbid/export'"""
    JobId = 0
    Label = 1
    State = 2
    Progress = 3
    TaskInfo = 4
    Path = 5
    CreateTime = 6
    StartTime = 7
    FinishTime = 8
    Timeout = 9
    ErrorMsg = 10

    def __init__(self, export_job):
        self.export_job = export_job

    def get_state(self):
        """get state"""
        return self.export_job[self.State]

    def get_label(self):
        """get label"""
        return self.export_job[self.Label]

    def get_timeout(self):
        """get timeout"""
        return self.export_job[self.Timeout]
 
    def get_error_msg(self):
        """get error msg"""
        return self.export_job[self.ErrorMsg]

    def get_task_info(self):
        """get statistic"""
        statistic = self.export_job[self.TaskInfo]
        return eval(statistic)

    def get_exec_mem_limit(self):
        """get exec mem limit"""
        return self.get_task_info()["exec mem limit"]

    def get_column_separator(self):
        """get column separator"""
        return self.get_task_info()["column separator"]

    def get_line_delimiter(self):
        """get line delimiter"""
        return self.get_task_info()["line delimiter"]

    def get_tablet_num(self):
        """get tablet num"""
        return self.get_task_info()["tablet num"]

    def get_coord_num(self):
        """get coord num"""
        return self.get_task_info()["coord num"]


class SelectIntoInfo(object):
    """select into """
    FileNumber = 0
    TotalRows= 1
    FileSize = 2
    URL = 3

    def __init__(self, info):
        self.select_into_info = info

    def get_file_number(self):
        """get file number"""
        return self.select_into_info[self.FileNumber]

    def get_total_rows(self):
        """get file number"""
        return self.select_into_info[self.TotalRows]

    def get_url(self):
        """get file number"""
        return self.select_into_info[self.URL]


class BackendShowInfo(object):
    """show backends different with show proc '/backends';"""
    BackendId = 0
    Host = 1
    HeartbeatPort = 2
    BePort = 3
    HttpPort = 4
    BrpcPort = 5
    LastStartTime = 6
    LastHeartbeat = 7
    Alive = 8
    SystemDecommissioned = 9
    TabletNum = 10
    DataUsedCapacity = 11
    AvailCapacity = 12
    TotalCapacity = 13
    UsedPct = 14
    MaxDiskUsedPct = 15
    RemoteUsedCapacity = 16
    Tag = 17
    ErrMsg = 18
    Version = 19
    Status = 20
    HeartbeatFailureCounter = 21
    NodeRole = 22

    def __init__(self, be_info):
        self.be_info = be_info

    def get_backend_id(self):
        """get backend id"""
        return self.be_info[self.BackendId]

    def get_ip(self):
        """get ip"""
        return socket.gethostbyname(self.be_info[self.Host])

    def get_alive(self):
        """get be state """
        return self.be_info[self.Alive]


class BackendProcInfo(object):
    """show proc '/backends' info different with show backends"""
    BackendId = 0
    Host = 1
    HeartbeatPort = 2
    BePort = 3
    HttpPort = 4
    BrpcPort = 5
    LastStartTime = 6
    LastHeartbeat = 7
    Alive = 8
    SystemDecommissioned = 9
    TabletNum = 10
    DataUsedCapacity = 11
    AvailCapacity = 12
    TotalCapacity = 13
    UsedPct = 14
    MaxDiskUsedPct = 15
    RemoteUsedCapacity = 16
    Tag = 17
    ErrMsg = 18
    Version = 19
    Status = 20
    HeartbeatFailureCounter = 21
    NodeRole = 22
    
    def __init__(self, be_info):
        self.be_info = be_info

    def get_backend_id(self):
        """get backend_id"""
        return self.be_info[self.BackendId]

    def get_ip(self):
        """get ip"""
        return socket.gethostbyname(self.be_info[self.Host])
 
    def get_hostname(self):
        """get hostname"""
        return self.be_info[self.Host]

    def get_alive(self):
        """get alive"""
        return self.be_info[self.Alive]
    
    def get_httpport(self):
        """get httpport"""
        return self.be_info[self.HttpPort]

    def get_heartbeatport(self):
        """get heartbeatport"""
        return self.be_info[self.HeartbeatPort]

    def get_backend_start_time(self):
        """get start time"""
        return self.be_info[self.LastStartTime]

    def get_tag(self):
        """get tag"""
        tag_json = eval(self.be_info[self.Tag])
        tags = tag_json['location']
        return tags


class FrontendInfo(object):
    """show proc '/frontends'"""
    Name = 0
    Host = 1
    EditLogPort = 2
    HttpPort = 3
    QueryPort = 4
    RpcPort = 5
    Role = 6
    IsMaster = 7
    ClusterId = 8
    Join = 9
    Alive = 10
    ReplayedJournalId = 11
    LastHeartbeat = 12
    IsHelper = 13
    ErrMsg = 14
    Version = 15
    CurrentConnected =16

    def __init__(self, frontend):
        self.frontend = frontend

    def get_ismaster(self):
        """get ismaster"""
        return self.frontend[self.IsMaster]   

    def get_host(self):
        """get host"""
        return self.frontend[self.Host]

    def get_httpport(self):
        """get port"""
        return self.frontend[self.HttpPort]

    def get_role(self):
        """get role"""
        return self.frontend[self.Role]

    def get_alive(self):
        """get alive"""
        return self.frontend[self.Alive]

    def get_IP(self):
        """get IP"""
        return socket.gethostbyname(self.frontend[self.Host])

    def get_LastHeartbeat(self):
        """get LastUpdateTime"""
        return self.frontend[self.LastHeartbeat]


class FrontendShowInfo(object):
    """show frontends """
    Name = 0
    Host = 1
    EditLogPort = 2
    HttpPort = 3
    QueryPort = 4
    RpcPort = 5
    Role = 6
    IsMaster = 7
    ClusterId = 8
    Join = 9
    Alive = 10
    ReplayedJournalId = 11
    LastHeartbeat = 12
    IsHelper = 13
    ErrMsg = 14
    Version = 15
    CurrentConnected = 16
    
    def __init__(self, frontend):
        self.frontend = frontend

    def get_ismaster(self):
        """get ismaster"""
        return self.frontend[self.IsMaster]
    
    def get_ip(self):
        """get ip"""
        return socket.gethostbyname(self.frontend[self.Host])


class BrokerInfo(object):
    """
    show proc '/brokers'
     Name: broker 名字
     Host： broker host
     Port：端口
     Alive：是否活着
     LastStartTime：上一次启动时间
     LastUpdateTime：上一次心跳时间，随时变
     ErrMsg： Palo 会定期检查仓库的连通性，如果出现问题，这里会显示错误信息
    """
    Name = 0
    Host = 1
    Port = 2
    Alive = 3
    LastStartTime = 4
    LastUpdateTime = 5
    ErrMsg = 6

    def __init__(self, broker_info):
        self.broker_info = broker_info

    def get_name(self):
        """get Name"""
        return self.broker_info[BrokerInfo.Name]

    def get_ip(self):
        """get IP"""
        return socket.gethostbyname(self.broker_info[self.Host])

    def get_port(self):
        """get Port"""
        return self.broker_info[BrokerInfo.Port]

    def get_alive(self):
        """get Alive"""
        return self.broker_info[BrokerInfo.Alive]

    def get_last_start_time(self):
        """get LastStartTime"""
        return self.broker_info[BrokerInfo.LastStartTime]

    def get_last_update_time(self):
        """get LastUpdateTime"""
        return self.broker_info[BrokerInfo.LastUpdateTime]

    def get_errmsg(self):
        """get ErrMsg"""
        return self.broker_info[BrokerInfo.ErrMsg]


class GrantInfo(object):
    """show grants ; show proc '/auth'"""
    UserIdentity = 0
    Password = 1
    Roles = 2
    GlobalPrivs = 3
    CatalogPrivs = 4
    DatabasePrivs = 5
    TablePrivs = 6
    ResourcePrivs = 7

    def __init__(self, grant):
        self.grant = grant

    def get_user_identity(self):
        """get user identity"""
        return self.grant[self.UserIdentity]

    def get_password(self):
        """get password"""
        return self.grant[self.Password]

    def get_global_privs(self):
        """get global privs"""
        return self.grant[self.GlobalPrivs]

    def get_database_privs(self):
        """get database privs"""
        return self.grant[self.DatabasePrivs]

    def get_table_privs(self):
        """get table privs"""
        return self.grant[self.TablePrivs]


class DescInfo(object):
    """desc table_name"""
    Field = 0
    Type = 1
    Null = 2
    Key = 3
    Default = 4
    Extra = 5

    def __init__(self, column_info):
        self.column_info = column_info

    def get_field(self):
        """get column filed name"""
        return self.column_info[self.Field]

    def get_type(self):
        """get column type"""
        return self.column_info[self.Type]

    def get_null(self):
        """get column is null"""
        return self.column_info[self.Null]

    def get_key(self):
        """get column is key"""
        return self.column_info[self.Key]

    def get_default(self):
        """get column is key"""
        return self.column_info[self.Key]

    def get_extra(self):
        """get extra"""
        return self.column_info[self.Extra]


class DescInfoAll(object):
    """desc all"""
    IndexName = 0
    IndexKeyType = 1
    Field = 2
    Type = 3
    Null = 4
    Key = 5
    Default = 6
    Extra = 7

    def __init__(self, column_info):
        self.column_info = column_info

    def get_index_name(self):
        """get index name"""
        return self.column_info[self.IndexName]

    def get_field(self):
        """get column filed name"""
        return self.column_info[self.Field]

    def get_type(self):
        """get column type"""
        return self.column_info[self.Type]

    def get_null(self):
        """get column is null"""
        return self.column_info[self.Null]

    def get_key(self):
        """get column is key"""
        return self.column_info[self.Key]

    def get_default(self):
        """get column is key"""
        return self.column_info[self.Key]

    def get_extra(self):
        """get extra"""
        return self.column_info[self.Extra]


class PartitionInfo(object):
    """show partitions"""
    PartitionId = 0
    PartitionName = 1
    VisibleVersion = 2
    VisibleVersionTime = 3
    State = 4
    PartitionKey = 5
    Range = 6
    DistributionKey = 7
    Buckets = 8
    ReplicationNum = 9
    StorageMedium = 10
    CooldownTime = 11
    RemoteStoragePolicy = 12
    LastConsistencyCheckTime = 13
    DataSize = 14
    IsInMemory = 15
    ReplicaAllocation = 16
    IsMutable = 17

    def __init__(self, partition):
        self.partition = partition

    def get_partition_name(self):
        """get partition name"""
        return self.partition[self.PartitionName]

    def get_replication_num(self):
        """get replication num"""
        return self.partition[self.ReplicationNum]

    def get_buckets(self):
        """get buckets"""
        return self.partition[self.Buckets]

    def get_is_in_memory(self):
        """get is in memory"""
        return self.partition[self.IsInMemory]

    def get_replica_allocation(self):
        """get replica allocation"""
        return self.partition[self.ReplicaAllocation]


class RoutineLoadJob(object):
    """show routine load """
    Id = 0
    Name = 1
    CreateTime = 2
    PauseTime = 3
    EndTime = 4
    DbName = 5
    TableName = 6
    IsMultiTable = 7
    State = 8
    DataSourceType = 9
    CurrentTaskNum = 10
    JobProperties = 11
    DataSourceProperties = 12
    CustomProperties = 13
    Statistic = 14
    Progress = 15
    Lag = 16
    ReasonOfStateChanged = 17
    ErrorLogUrls = 18
    OtherMsg = 19
    User = 20
    Comment = 21

    def __init__(self, routine_job):
        self.routine_job = routine_job

    def get_state(self):
        """get routine job state"""
        return self.routine_job[self.State]

    def get_name(self):
        """get routine job name"""
        return self.routine_job[self.Name]

    def get_progress(self):
        """get progress"""
        return self.routine_job[self.Progress]

    def get_statistic(self):
        """get statistic"""
        statistic = self.routine_job[self.Statistic]
        return eval(statistic)

    def get_error_log_urls(self):
        """get errorlogurls"""
        return self.routine_job[self.ErrorLogUrls]

    def get_job_properties(self):
        """get job properties"""
        property = self.routine_job[self.JobProperties]
        return eval(property)

    def get_current_task_num(self):
        """get current task num"""
        return self.routine_job[self.CurrentTaskNum]

    def get_reason_of_state_changed(self):
        """get reason of state changed"""
        return self.routine_job[self.ReasonOfStateChanged]

    def get_received_bytes_rate(self):
        """get received bytes rate"""
        return self.get_statistic()["receivedBytesRate"]

    def get_loaded_rows(self):
        """get loaded rows"""
        return self.get_statistic()["loadedRows"]

    def get_error_rows(self):
        """get error rows"""
        return self.get_statistic()["errorRows"]

    def get_total_rows(self):
        """get total rows"""
        return self.get_statistic()["totalRows"]

    def get_unselected_rows(self):
        """get unselected rows"""
        return self.get_statistic()["unselectedRows"]

    def get_task_execute_time_ms(self):
        """get task execute time ms"""
        return self.get_statistic()["taskExecuteTimeMs"]

    def get_task_committed_task_num(self):
        """get task committed task num"""
        return self.get_statistic()["committedTaskNum"]
    
    def get_task_aborted_task_num(self):
        """get task aborted task num"""
        return self.get_statistic()["abortedTaskNum"]

    def get_table_name(self):
        """get table name"""
        return self.routine_job[self.TableName]
    
    def get_db_name(self):
        """get database name"""
        return self.routine_job[self.DbName]

    def get_merge_type(self):
        """get merge type"""
        return self.get_job_properties()["mergeType"]


class RoutineLoadTask(object):
    """show routine load task"""
    TaskId = 0
    TxnId = 1
    JobId = 2
    CreateTime = 3
    ExecuteStartTime = 4
    BeId = 5
    DataSourceProperties = 6

    def __init__(self, routine_task):
        self.routine_task = routine_task

    def get_taskid(self):
        """get taskid"""
        return self.routine_task[self.TaskId]

    def get_jobid(self):
        """get jobid"""
        return self.routine_task[self.JobId]

    def get_beid(self):
        """get beid"""
        return self.routine_task[self.BeId]


class TabletsInfo(object):
    """
    show tablets from table_name
    SHOW PROC '/dbs/db_id/table_id/partitions/partition_id/index_id/tablet_id'
    """
    TabletId = 0
    ReplicaId = 1
    BackendId = 2
    SchemaHash = 3
    Version = 4
    LstSuccessVersion = 5
    LstFailedVersion = 6
    LstFailedTime = 7
    LocalDataSize = 8
    RemoteDataSize = 9
    RowCount = 10
    State = 11
    LstConsistencyCheckTime = 12
    CheckVersion = 13
    VersionCount = 14
    QueryHits = 15
    PathHash = 16
    MetaUrl = 17
    CompactionStatus = 18
    CooldownReplicaId = 19
    CooldownMetaId = 20

    def __init__(self, tablet_info):
        self.tablet_info = tablet_info

    def get_tablet_id(self):
        """get tablet id"""
        return self.tablet_info[self.TabletId]

    def get_replica_id(self):
        """get replica id"""
        return self.tablet_info[self.ReplicaId]

    def get_backend_id(self):
        """get backend id"""
        return self.tablet_info[self.BackendId]

    def get_state(self):
        """get state"""
        return self.tablet_info[self.State]
   
    def get_compaction_status(self):
        """get compaction status"""
        return self.tablet_info[self.CompactionStatus]


class TabletIdInfo(object):
    """show tablet tablet_id"""
    DbName = 0
    TableName = 1
    PartitionName = 2
    IndexName = 3
    DbId = 4
    TableId = 5
    PartitionId = 6
    IndexId = 7
    IsSync = 8
    Order = 9
    QueryHits = 10
    DetailCmd = 11
    
    def __init__(self, tablet_id_info):
        self.tablet_id_info = tablet_id_info
        
    def get_db_name(self):
        """get db name"""
        return self.tablet_id_info[self.DbName]
    
    def get_table_name(self):
        """get table name"""
        return self.tablet_id_info[self.TableName]
    
    def get_partition_name(self):
        """get partition name"""
        return self.tablet_id_info[self.PartitionName]
    
    def get_index_name(self):
        """get index name"""
        return self.tablet_id_info[self.IndexName]
    
    def get_db_id(self):
        """get db id"""
        return self.tablet_id_info[self.DbId]
    
    def get_table_id(self):
        """get table id"""
        return self.tablet_id_info[self.TableId]
    
    def get_partition_id(self):
        """get partition id"""
        return self.tablet_id_info[self.PartitionId]
    
    def get_index_id(self):
        """get index id"""
        return self.tablet_id_info[self.IndexId]
    
    def get_is_sync(self):
        """get is sync"""
        return self.tablet_id_info[self.IsSync]
    
    def get_detail_cmd(self):
        """get detail cmd"""
        return self.tablet_id_info[self.DetailCmd]
  

class TransactionInfo(object):
    """show transcation"""
    TransactionId = 0
    Label = 1
    Coordinator = 2
    TransactionStatus = 3
    LoadJobSourceType = 4
    PrepareTime = 5
    PreCommitTime = 6
    CommitTime = 7
    PublishTime = 8
    FinishTime = 9
    Reason = 10
    ErrorReplicasCount = 11
    ListenerId = 12
    TimeoutMs = 13
    ErrMsg = 14
   
    def __init__(self, txn):
        self.txn = txn

    def get_transaction_id(self):
        """get transaction id"""
        return self.txn[self.TransactionId]

    def get_label(self):
        """get label"""
        return self.txn[self.Label]

    def get_transaction_status(self):
        """get txn status"""
        return self.txn[self.TransactionStatus]

    def get_timeout_ms(self):
        """get txn timeout ms"""
        return self.txn[self.TimeoutMs]


class TableIndexInfo(object):
    """show index from database.table"""
    TableName = 0
    NonUnique = 1
    KeyName = 2
    SeqInIndex = 3
    ColumnName = 4
    Collation = 5
    Cardinality = 6
    SubPart = 7
    Packed = 8
    IsNull = 9
    IndexType = 10
    Comment = 11
    
    def __init__(self, table_index_info):
        self.table_index_info = table_index_info
    
    def get_table_name(self):
        """get table name"""
        return self.table_index_info[self.TableName]
    
    def get_key_name(self):
        """get index key name"""
        return self.table_index_info[self.KeyName]
    
    def get_column_name(self):
        """get column name"""
        return self.table_index_info[self.ColumnName]
    
    def get_index_type(self):
        """get column name"""
        return self.table_index_info[self.IndexType]


class AdminShowConfig(object):
    """ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"]"""
    Key = 0
    Value = 1
    Type = 2
    IsMutable = 3
    MasterOnly = 4
    Comment = 5

    def __init__(self, key_config):
        self.key_config = key_config

    def get_key(self):
        """get key"""
        return self.key_config[self.Key]

    def get_value(self):
        """get value"""
        return self.key_config[self.Value]


class DynamicPartitionInfo(object):
    """show dynamic partition tables"""
    TableName = 0
    Enable = 1
    TimeUnit = 2
    Start = 3
    End = 4
    Prefix = 5
    Buckets = 6
    ReplicationNum = 7
    ReplicaAllocation = 8
    StartOf = 9
    LastUpdatetime = 10
    LastSchedulerTime = 11
    State = 12
    LastCreatePartitionMsg = 13
    LastDropPartitionMsg = 14

    def __init__(self, dynamic_partition_info):
        self.dynamic_partition_info = dynamic_partition_info

    def get_enable(self):
        """get enable"""
        return self.dynamic_partition_info[self.Enable]

    def get_time_unit(self):
        """get time unit"""
        return self.dynamic_partition_info[self.TimeUnit]

    def get_start(self):
        """get start"""
        return self.dynamic_partition_info[self.Start]

    def get_end(self):
        """get end"""
        return self.dynamic_partition_info[self.End]

    def get_prefix(self):
        """get prefix"""
        return self.dynamic_partition_info[self.Prefix]

    def get_buckets(self):
        """get buckets"""
        return self.dynamic_partition_info[self.Buckets]

    def get_replication_num(self):
        """get replication num"""
        return self.dynamic_partition_info[self.ReplicationNum]

    def get_start_of(self):
        """get start of"""
        return self.dynamic_partition_info[self.StartOf]


class SyncJobInfo(object):
    """show sync job"""
    JobId = 0
    JobName = 1
    Type = 2
    State = 3
    Channel = 4
    Status = 5
    JobConfig = 6
    CreateTime = 7
    LastStartTime = 8
    LastStopTime = 9
    FinishTime = 10
    Msg = 11

    def __init__(self, sync_job_info):
        self.sync_job_info = sync_job_info

    def get_job_name(self):
        """get job name"""
        return self.sync_job_info[self.JobName]

    def get_state(self):
        """get state"""
        return self.sync_job_info[self.State]

    def get_channel(self):
        """get channel"""
        return self.sync_job_info[self.Channel]

    def get_job_config(self):
        """get job config"""
        return self.sync_job_info[self.JobConfig]


class ReplicaStatus(object):
    """admin show replica status"""
    TabletId = 0
    ReplicaId = 1
    BackendId = 2
    Version = 3
    LastFailedVersion = 4
    LastSuccessVersion = 5
    CommittedVersion = 6
    SchemaHash = 7
    VersionNum = 8
    IsBad = 9
    State = 10
    Status = 11

    def __init__(self, replica_status):
        self.replica_status = replica_status

    def get_backend_id(self):
        """get backend id"""
        return self.replica_status[self.BackendId]


if __name__ == '__main__':
    print(ReplicaStatus.__name__)


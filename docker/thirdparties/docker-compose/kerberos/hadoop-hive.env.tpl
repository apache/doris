#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

HIVE_SITE_CONF_javax_jdo_option_ConnectionURL=jdbc:mysql://127.0.0.1:${MYSQL_PORT}/metastore
HIVE_SITE_CONF_javax_jdo_option_ConnectionDriverName=com.mysql.jdbc.Driver
HIVE_SITE_CONF_javax_jdo_option_ConnectionUserName=root
HIVE_SITE_CONF_javax_jdo_option_ConnectionPassword=root
HIVE_SITE_CONF_datanucleus_autoCreateSchema=false
HIVE_SITE_CONF_hive_metastore_port=${HMS_PORT}
HIVE_SITE_CONF_hive_metastore_uris=thrift://${IP_HOST}:${HMS_PORT}
HIVE_SITE_CONF_hive_server2_thrift_bind_host=0.0.0.0
HIVE_SITE_CONF_hive_server2_thrift_port=${HS_PORT}
HIVE_SITE_CONF_hive_server2_webui_port=0
HIVE_SITE_CONF_hive_compactor_initiator_on=true
HIVE_SITE_CONF_hive_compactor_worker_threads=2
HIVE_SITE_CONF_metastore_storage_schema_reader_impl=org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader
BEELINE_SITE_CONF_beeline_hs2_jdbc_url_tcpUrl=jdbc:hive2://${HOST}:${HS_PORT}/default;user=hdfs;password=hive
BEELINE_SITE_CONF_beeline_hs2_jdbc_url_httpUrl=jdbc:hive2://${HOST}:${HS_PORT}/default;user=hdfs;password=hive


CORE_CONF_fs_defaultFS=hdfs://${HOST}:${FS_PORT}
CORE_CONF_hadoop_http_staticuser_user=root
CORE_CONF_hadoop_proxyuser_hue_hosts=*
CORE_CONF_hadoop_proxyuser_hue_groups=*

HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false
HDFS_CONF_dfs_datanode_address=${HOST}:${DFS_DN_PORT}
HDFS_CONF_dfs_datanode_http_address=${HOST}:${DFS_DN_HTTP_PORT}
HDFS_CONF_dfs_datanode_ipc_address=${HOST}:${DFS_DN_IPC_PORT}
HDFS_CONF_dfs_namenode_http___address=${HOST}:${DFS_NN_HTTP_PORT}
YARN_CONF_yarn_log___aggregation___enable=true
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_fs_state___store_uri=/rmstate
YARN_CONF_yarn_nodemanager_remote___app___log___dir=/var/log/hadoop-yarn/apps
YARN_CONF_yarn_log_server_url=http://${HOST}:${YARM_LOG_SERVER_PORT}/jobhistory/logs
YARN_CONF_yarn_timeline___service_enabled=false
YARN_CONF_yarn_timeline___service_generic___application___history_enabled=true
YARN_CONF_yarn_resourcemanager_system___metrics___publisher_enabled=true
YARN_CONF_yarn_resourcemanager_hostname=${HOST}
MAPRED_CONF_mapreduce_shuffle_port=${MAPREDUCE_SHUFFLE_PORT}
YARN_CONF_yarn_timeline___service_hostname=${HOST}
YARN_CONF_yarn_resourcemanager_address=${HOST}:${YARN_RM_PORT}
YARN_CONF_yarn_resourcemanager_scheduler_address=${HOST}:${YARN_RM_SCHEDULER_PORT}
YARN_CONF_yarn_resourcemanager_resource___tracker_address=${HOST}:${YARN_RM_TRACKER_PORT}
YARN_CONF_yarn_resourcemanager_admin_address=${HOST}:${YARN_RM_ADMIN_PORT}
YARN_CONF_yarn_resourcemanager_webapp_address=${HOST}:${YARN_RM_WEBAPP_PORT}
YARN_CONF_yarn_nodemanager_localizer_address=${HOST}:${YARN_NM_LOCAL_PORT}
YARN_CONF_yarn_nodemanager_webapp_address=${HOST}:${YARN_NM_WEBAPP_PORT}

HIVE_SITE_CONF_fs_s3a_impl=org.apache.hadoop.fs.s3a.S3AFileSystem
HIVE_SITE_CONF_fs_s3a_access_key=${AWSAk}
HIVE_SITE_CONF_fs_s3a_secret_key=${AWSSk}
HIVE_SITE_CONF_fs_s3a_endpoint=${AWSEndpoint}
HIVE_SITE_CONF_fs_AbstractFileSystem_obs_impl=org.apache.hadoop.fs.obs.OBS
HIVE_SITE_CONF_fs_obs_access_key=${OBSAk}
HIVE_SITE_CONF_fs_obs_secret_key=${OBSSk}
HIVE_SITE_CONF_fs_obs_endpoint=${OBSEndpoint}
HIVE_SITE_CONF_fs_cosn_credentials_provider=org.apache.hadoop.fs.auth.SimpleCredentialProvider
HIVE_SITE_CONF_fs_cosn_userinfo_secretId=${COSAk}
HIVE_SITE_CONF_fs_cosn_userinfo_secretKey=${COSSk}
HIVE_SITE_CONF_fs_cosn_bucket_region=${COSRegion}
HIVE_SITE_CONF_fs_cosn_impl=org.apache.hadoop.fs.CosFileSystem
HIVE_SITE_CONF_fs_AbstractFileSystem_cosn_impl=org.apache.hadoop.fs.CosN
HIVE_SITE_CONF_fs_oss_impl=org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem
HIVE_SITE_CONF_fs_oss_accessKeyId=${OSSAk}
HIVE_SITE_CONF_fs_oss_accessKeySecret=${OSSSk}
HIVE_SITE_CONF_fs_oss_endpoint=${OSSEndpoint}
enablePaimonHms=${enablePaimonHms}
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


HIVE_SITE_CONF_hive_metastore_event_db_notification_api_auth=false
HIVE_SITE_CONF_hive_metastore_dml_events=true
HIVE_SITE_CONF_hive_metastore_transactional_event_listeners=org.apache.hive.hcatalog.listener.DbNotificationListener
HIVE_SITE_CONF_hive_stats_column_autogather=false
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
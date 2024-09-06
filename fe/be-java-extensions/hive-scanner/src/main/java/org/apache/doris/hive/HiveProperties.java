// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.hive;

public class HiveProperties {

    protected static final String COLUMNS_TYPE_DELIMITER = "#";
    protected static final String FIELDS_DELIMITER = ",";

    protected static final String IS_GET_TABLE_SCHEMA = "is_get_table_schema";  // 确定是否需要获取表数据
    protected static final String COLUMNS_TYPES = "columns_types";  // 列类型(rc-binary文件本身不存储)
    protected static final String COLUMNS_NAMES  = "columns_names"; // 列名称(rc-binary文件本身不存储)
    protected static final String REQUIRED_FIELDS = "required_fields";  // 需要获取的列名
    protected static final String FILE_TYPE = "file_type";  // 文件类型：FILE_LOCAL / FILE_S3 / FILE_HDFS
    protected static final String URI = "uri";  // 文件路径
    protected static final String S3_ACCESS_KEY = "s3.access_key";
    protected static final String S3_SECRET_KEY = "s3.secret_key";
    protected static final String S3_ENDPOINT = "s3.endpoint";
    protected static final String S3_REGION = "s3.region";
    protected static final String HIVE_SERDE = "hive.serde";    // 序列化方法
    protected static final String COLUMNS = "columns";
    protected static final String COLUMNS2TYPES = "columns.types";
    protected static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    protected static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    protected static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    protected static final String FS_S3A_REGION = "fs.s3a.region";
    protected static final String SPLIT_START_OFFSET = "split_start_offset";
    protected static final String SPLIT_SIZE = "split_size";
    protected static final String SPLIT_FILE_SIZE = "split_file_size";

}

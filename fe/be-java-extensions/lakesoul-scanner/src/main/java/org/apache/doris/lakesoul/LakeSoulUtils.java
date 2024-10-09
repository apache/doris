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

package org.apache.doris.lakesoul;

import java.util.Arrays;
import java.util.List;

public class LakeSoulUtils {
    public static final String FILE_NAMES = "file_paths";
    public static final String PRIMARY_KEYS = "primary_keys";
    public static final String SCHEMA_JSON = "table_schema";
    public static final String PARTITION_DESC = "partition_descs";
    public static final String REQUIRED_FIELDS = "required_fields";
    public static final String OPTIONS = "options";
    public static final String SUBSTRAIT_PREDICATE = "substrait_predicate";
    public static final String LIST_DELIM = ";";
    public static final String PARTITIONS_KV_DELIM = "=";

    public static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    public static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    public static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    public static final String FS_S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";

    public static final List<String> OBJECT_STORE_OPTIONS = Arrays.asList(
            FS_S3A_ACCESS_KEY,
            FS_S3A_SECRET_KEY,
            FS_S3A_ENDPOINT,
            FS_S3A_PATH_STYLE_ACCESS
    );
}

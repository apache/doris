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

package org.apache.doris.common;

import org.apache.doris.persist.meta.FeMetaFormat;

public class FeConstants {
    // The default value of bucket setting && auto bucket without estimate_partition_size
    public static int default_bucket_num = 10;

    /*
     * Those two fields is responsible for determining the default key columns in duplicate table.
     * If user does not specify key of duplicate table in create table stmt,
     * the default key columns will be supplemented by Doris.
     * The default key columns are first 36 bytes(DEFAULT_DUP_KEYS_BYTES) of the columns in define order.
     * If the number of key columns in the first 36 is less than 3(DEFAULT_DUP_KEYS_COUNT),
     * the first 3 columns will be used.
     */
    public static int shortkey_max_column_count = 3;
    public static int shortkey_maxsize_bytes = 36;

    public static int checkpoint_interval_second = 60; // 1 minutes

    // dpp version
    public static String dpp_version = "3_2_0";

    // bloom filter false positive probability
    public static double default_bloom_filter_fpp = 0.05;

    // set to true to skip some step when running FE unit test
    public static boolean runningUnitTest = false;
    // use to set some mocked values for FE unit test
    public static Object unitTestConstant = null;

    // set to false to disable internal schema db
    public static boolean enableInternalSchemaDb = true;

    // default scheduler interval is 10 seconds
    public static int default_scheduler_interval_millisecond = 10000;

    // general model
    // Current meta data version. Use this version to write journals and image
    public static int meta_version = FeMetaVersion.VERSION_CURRENT;

    // Current meta format. Use this format to read and write image.
    public static FeMetaFormat meta_format = FeMetaFormat.COR1;

    // use \N to indicate NULL
    public static String null_string = "\\N";

    // use for copy into test
    public static boolean disablePreHeat = false;

    public static final String FS_PREFIX_S3 = "s3";
    public static final String FS_PREFIX_S3A = "s3a";
    public static final String FS_PREFIX_S3N = "s3n";
    public static final String FS_PREFIX_OSS = "oss";
    public static final String FS_PREFIX_GCS = "gs";
    public static final String FS_PREFIX_BOS = "bos";
    public static final String FS_PREFIX_COS = "cos";
    public static final String FS_PREFIX_COSN = "cosn";
    public static final String FS_PREFIX_LAKEFS = "lakefs";
    public static final String FS_PREFIX_OBS = "obs";
    public static final String FS_PREFIX_OFS = "ofs";
    public static final String FS_PREFIX_GFS = "gfs";
    public static final String FS_PREFIX_JFS = "jfs";
    public static final String FS_PREFIX_HDFS = "hdfs";
    public static final String FS_PREFIX_VIEWFS = "viewfs";
    public static final String FS_PREFIX_FILE = "file";

    public static final String INTERNAL_DB_NAME = "__internal_schema";
    public static final String INTERNAL_FILE_CACHE_HOTSPOT_TABLE_NAME = "cloud_cache_hotspot";
    public static String TEMP_MATERIZLIZE_DVIEW_PREFIX = "internal_tmp_materialized_view_";

    public static String METADATA_FAILURE_RECOVERY_KEY = "metadata_failure_recovery";

    public static String CLOUD_RETRY_E230 = "E-230";

    public static String BUILT_IN_STORAGE_VAULT_NAME = "built_in_storage_vault";
}

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

public class FeConstants {
    // Database and table's default configurations, we will never change them
    public static short default_replication_num = 3;
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

    public static int heartbeat_interval_second = 5;
    public static int checkpoint_interval_second = 60; // 1 minutes

    // dpp version
    public static String dpp_version = "3_2_0";

    // bloom filter false positive probability
    public static double default_bloom_filter_fpp = 0.05;

    // set to true to skip some step when running FE unit test
    public static boolean runningUnitTest = false;

    // default scheduler interval is 10 seconds
    public static int default_scheduler_interval_millisecond = 10000;

    // general model
    // Current meta data version. Use this version to write journals and image
    public static int meta_version = FeMetaVersion.VERSION_CURRENT;

    // Current meta format. Use this format to read and write image.
    public static FeMetaFormat meta_format = FeMetaFormat.COR1;

    // use \N to indicate NULL
    public static String null_string = "\\N";

    public static long tablet_checker_interval_ms = 20 * 1000L;

    public static String csv = "csv";
    public static String csv_with_names = "csv_with_names";
    public static String csv_with_names_and_types = "csv_with_names_and_types";

    public static String text = "text";
}

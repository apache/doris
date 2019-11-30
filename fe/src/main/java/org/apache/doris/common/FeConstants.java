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
    public static int shortkey_max_column_count = 5;
    public static int shortkey_maxsize_bytes = 36;
    public static long default_db_data_quota_bytes = 1024 * 1024 * 1024 * 1024L; // 1TB

    public static int heartbeat_interval_second = 5;
    public static int checkpoint_interval_second = 60; // 1 minutes

    // dpp version
    public static String dpp_version = "3_2_0";

    // bloom filter false positive probability
    public static double default_bloom_filter_fpp = 0.05;

    // set to true to skip some step when running FE unit test
    public static boolean runningUnitTest = false;

    // general model
    // Current meta data version. Use this version to write journals and image
    public static int meta_version = FeMetaVersion.VERSION_67;
}

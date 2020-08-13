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

package org.apache.doris.broker.hdfs;

import org.apache.doris.common.ConfigBase;


public class BrokerConfig extends ConfigBase {
    
    @ConfField
    public static int hdfs_read_buffer_size_kb = 1024;
    
    @ConfField
    public static int hdfs_write_buffer_size_kb = 1024;
    
    @ConfField
    public static int client_expire_seconds = 300;
    
    @ConfField
    public static int broker_ipc_port = 8000;
    
    @ConfField
    public static String sys_log_dir = System.getenv("BROKER_HOME") + "/log";
    @ConfField
    public static String sys_log_level = "INFO"; // INFO, WARNING, ERROR, FATAL
    @ConfField
    public static String sys_log_roll_mode = "SIZE-MB-1024"; // TIME-DAY
                                                             // TIME-HOUR
                                                             // SIZE-MB-nnn
    @ConfField
    public static int sys_log_roll_num = 30; // the config doesn't work if
                                             // rollmode is TIME-*
    @ConfField
    public static String audit_log_dir = System.getenv("BROKER_HOME") + "/log";
    @ConfField
    public static String[] audit_log_modules = {};
    @ConfField
    public static String audit_log_roll_mode = "TIME-DAY"; // TIME-DAY TIME-HOUR
                                                           // SIZE-MB-nnn
    @ConfField
    public static int audit_log_roll_num = 10; // the config doesn't work if
                                               // rollmode is TIME-*
    // verbose modules. VERBOSE level is implemented by log4j DEBUG level.
    @ConfField
    public static String[] sys_log_verbose_modules = { "org.apache.doris" };
}

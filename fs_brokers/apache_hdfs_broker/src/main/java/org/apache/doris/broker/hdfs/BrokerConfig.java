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
    public static int client_expire_seconds = 3600;

    @ConfField
    public static boolean enable_input_stream_expire_check = false;

    @ConfField
    public static int input_stream_expire_seconds = 300;

    @ConfField
    public static int broker_ipc_port = 8000;
}

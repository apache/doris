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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;

public class DbUtil {

    public static long getCreateReplicasTimeoutMs(int tabletNum) {
        long timeout = Config.tablet_create_timeout_second * 1000L * tabletNum;
        timeout = Math.max(timeout, Config.min_create_table_timeout_second * 1000);
        timeout = Math.min(timeout, Config.max_create_table_timeout_second * 1000);
        return timeout;
    }

}

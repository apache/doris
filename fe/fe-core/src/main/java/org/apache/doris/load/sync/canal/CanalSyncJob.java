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

package org.apache.doris.load.sync.canal;

import org.apache.doris.load.sync.SyncJob;

import com.google.gson.annotations.SerializedName;

@Deprecated
public class CanalSyncJob extends SyncJob {
    @SerializedName(value = "remote")
    private CanalDestination remote;
    @SerializedName(value = "username")
    private String username;
    @SerializedName(value = "password")
    private String password;
    @SerializedName(value = "batchSize")
    private int batchSize = 8192;
    @SerializedName(value = "debug")
    private boolean debug = false;
}

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

package org.apache.doris.catalog;

import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;


public class EnvFactory {

    public static Env createEnv(boolean isCheckpointCatalog) {
        if (Config.isCloudMode()) {
            return new CloudEnv(isCheckpointCatalog);
        } else {
            return new Env(isCheckpointCatalog);
        }
    }

    public static InternalCatalog createInternalCatalog() {
        if (Config.isCloudMode()) {
            return new CloudInternalCatalog();
        } else {
            return new InternalCatalog();
        }
    }

    public static Partition createPartition() {
        if (Config.isCloudMode()) {
            return new CloudPartition();
        } else {
            return new Partition();
        }
    }

    public static Replica createReplica() {
        if (Config.isCloudMode()) {
            return new CloudReplica();
        } else {
            return new Replica();
        }
    }
}

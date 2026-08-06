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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;

final class ProcReplicaVersionDisplay {
    private ProcReplicaVersionDisplay() {
    }

    static long getDisplayReplicaVersion(Replica replica, long partitionVisibleVersion) {
        if (!Config.isCloudMode()) {
            return replica.getVersion();
        }
        return getCloudDisplayVersion(partitionVisibleVersion);
    }

    static long getDisplayReplicaLastSuccessVersion(Replica replica, long partitionVisibleVersion) {
        if (!Config.isCloudMode()) {
            return replica.getLastSuccessVersion();
        }
        return getCloudDisplayVersion(partitionVisibleVersion);
    }

    private static long getCloudDisplayVersion(long partitionVisibleVersion) {
        return partitionVisibleVersion >= 0 ? partitionVisibleVersion : -1L;
    }
}

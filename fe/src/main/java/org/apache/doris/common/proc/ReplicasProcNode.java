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
import org.apache.doris.common.util.TimeUtils;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId/tabletId
 * show replicas' detail info within a tablet
 */
public class ReplicasProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ReplicaId").add("BackendId").add("Version").add("VersionHash")
            .add("LstSuccessVersion").add("LstSuccessVersionHash")
            .add("LstFailedVersion").add("LstFailedVersionHash")
            .add("LstFailedTime").add("SchemaHash").add("DataSize").add("RowCount").add("State")
            .add("VersionCount").add("PathHash")
            .build();
    
    private List<Replica> replicas;

    public ReplicasProcNode(List<Replica> replicas) {
        this.replicas = replicas;
    }

    @Override
    public ProcResult fetchResult() {

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (Replica replica : replicas) {
            result.addRow(Arrays.asList(String.valueOf(replica.getId()),
                                        String.valueOf(replica.getBackendId()),
                                        String.valueOf(replica.getVersion()),
                                        String.valueOf(replica.getVersionHash()),
                                        String.valueOf(replica.getLastSuccessVersion()),
                                        String.valueOf(replica.getLastSuccessVersionHash()),
                                        String.valueOf(replica.getLastFailedVersion()),
                                        String.valueOf(replica.getLastFailedVersionHash()),
                                        TimeUtils.longToTimeString(replica.getLastFailedTimestamp()),
                                        String.valueOf(replica.getSchemaHash()),
                                        String.valueOf(replica.getDataSize()),
                                        String.valueOf(replica.getRowCount()),
                                        String.valueOf(replica.getState()),
                                        String.valueOf(replica.getVersionCount()),
                                        String.valueOf(replica.getPathHash())));
        }
        return result;
    }
}


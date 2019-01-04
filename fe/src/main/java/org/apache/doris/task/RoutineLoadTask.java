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

package org.apache.doris.task;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.common.LoadException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

public class RoutineLoadTask extends AgentTask {

    private String id;
    private long txnId;
    private RoutineLoadDesc routineLoadDesc;
    private LoadDataSourceType dataSourceType;


    public RoutineLoadTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, String id,
                           LoadDataSourceType dataSourceType, long txnId) {
        super(resourceInfo, backendId, TTaskType.STREAM_LOAD, dbId, tableId, 0L, 0L, 0L,
              Catalog.getCurrentCatalog().getNextId());
        this.id = id;
        this.txnId = txnId;
        this.dataSourceType = dataSourceType;
    }

    public void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) throws LoadException {
        if (this.routineLoadDesc != null) {
            throw new LoadException("Column separator has been initialized");
        }
        this.routineLoadDesc = new RoutineLoadDesc(routineLoadDesc.getColumnSeparator(),
                                                   routineLoadDesc.getColumnsInfo(),
                                                   routineLoadDesc.getWherePredicate(),
                                                   null);
    }
}

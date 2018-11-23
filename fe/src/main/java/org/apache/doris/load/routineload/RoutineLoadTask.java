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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.CatalogIdGenerator;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

public class RoutineLoadTask extends AgentTask {

    private String id;
    private String columns;
    private String where;
    private String columnSeparator;
    private RoutineLoadJob.DataSourceType dataSourceType;


    public RoutineLoadTask(TResourceInfo resourceInfo, long backendId, TTaskType taskType,
                           long dbId, long tableId, long partitionId, long indexId, long tabletId, String id,
                           String columns, String where, String columnSeparator,
                           RoutineLoadJob.DataSourceType dataSourceType) {
        super(resourceInfo, backendId, taskType, dbId, tableId, partitionId, indexId, tabletId,
                Catalog.getCurrentCatalog().getNextId());
        this.id = id;
        this.columns = columns;
        this.where = where;
        this.columnSeparator = columnSeparator;
        this.dataSourceType = dataSourceType;
    }
}

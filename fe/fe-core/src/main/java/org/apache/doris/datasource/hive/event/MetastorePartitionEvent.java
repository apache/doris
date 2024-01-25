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


package org.apache.doris.datasource.hive.event;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.Set;

/**
 * Base class for all the partition events
 */
public abstract class MetastorePartitionEvent extends MetastoreTableEvent {

    // for test
    protected MetastorePartitionEvent(long eventId, String catalogName, String dbName,
                                      String tblName, MetastoreEventType eventType) {
        super(eventId, catalogName, dbName, tblName, eventType);
    }

    protected MetastorePartitionEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
    }

    protected boolean willCreateOrDropTable() {
        return false;
    }

    protected boolean willChangeTableName() {
        return false;
    }

    /**
     * Returns if the process of this event will rename this partition.
     */
    protected abstract boolean willChangePartitionName();

    public abstract Set<String> getAllPartitionNames();
}

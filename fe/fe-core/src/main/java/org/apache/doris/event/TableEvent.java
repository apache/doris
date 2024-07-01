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

package org.apache.doris.event;

public abstract class TableEvent extends Event {
    protected final long ctlId;
    protected final long dbId;
    protected final long tableId;

    public TableEvent(EventType eventType, long ctlId, long dbId, long tableId) {
        super(eventType);
        this.ctlId = ctlId;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public long getCtlId() {
        return ctlId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "TableEvent{"
                + "ctlId=" + ctlId
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + "} " + super.toString();
    }
}

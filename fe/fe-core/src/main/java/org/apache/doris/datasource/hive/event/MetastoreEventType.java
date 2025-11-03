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

/**
 * Currently we only support handling some events.
 */
public enum MetastoreEventType {
    CREATE_TABLE("CREATE_TABLE"),
    DROP_TABLE("DROP_TABLE"),
    ALTER_TABLE("ALTER_TABLE"),
    CREATE_DATABASE("CREATE_DATABASE"),
    DROP_DATABASE("DROP_DATABASE"),
    ALTER_DATABASE("ALTER_DATABASE"),
    ADD_PARTITION("ADD_PARTITION"),
    ALTER_PARTITION("ALTER_PARTITION"),
    ALTER_PARTITIONS("ALTER_PARTITIONS"),
    DROP_PARTITION("DROP_PARTITION"),
    INSERT("INSERT"),
    INSERT_PARTITIONS("INSERT_PARTITIONS"),
    ALLOC_WRITE_ID_EVENT("ALLOC_WRITE_ID_EVENT"),
    COMMIT_TXN("COMMIT_TXN"),
    ABORT_TXN("ABORT_TXN"),
    OTHER("OTHER");

    private final String eventType;

    MetastoreEventType(String msEventType) {
        this.eventType = msEventType;
    }

    @Override
    public String toString() {
        return eventType;
    }

    /**
     * Returns the MetastoreEventType from a given string value of event from Metastore's
     * NotificationEvent.eventType. If none of the supported MetastoreEventTypes match,
     * return OTHER
     *
     * @param eventType EventType value from the {@link org.apache.hadoop.hive.metastore.api.NotificationEvent}
     */
    public static MetastoreEventType from(String eventType) {
        for (MetastoreEventType metastoreEventType : values()) {
            if (metastoreEventType.eventType.equalsIgnoreCase(eventType)) {
                return metastoreEventType;
            }
        }
        return OTHER;
    }
}

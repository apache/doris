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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;

/**
 * MetastoreEvent for Alter_DATABASE event type
 */
public class AlterDatabaseEvent extends MetastoreEvent {

    private AlterDatabaseEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.ALTER_DATABASE));
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new AlterDatabaseEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        // only can change properties,we do nothing
        infoLog("catalogName:[{}],dbName:[{}]", catalogName, dbName);
    }
}

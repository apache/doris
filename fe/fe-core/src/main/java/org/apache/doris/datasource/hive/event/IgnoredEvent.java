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

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;

/**
 * An event type which is ignored. Useful for unsupported metastore event types
 */
public class IgnoredEvent extends MetastoreEvent {
    private IgnoredEvent(NotificationEvent event, String catalogName) {
        super(event, catalogName);
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new IgnoredEvent(event, catalogName));
    }

    @Override
    public void process() {
        infoLog("Ignoring unknown event type " + metastoreNotificationEvent.getEventType());
    }
}

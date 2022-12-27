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
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;

import java.util.List;

/**
 * MetastoreEvent for CREATE_TABLE event type
 */
public class CreateTableEvent extends MetastoreTableEvent {
    public static final String CREATE_TABLE_EVENT_TYPE = "CREATE_TABLE";

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new CreateTableEvent(event, catalogName));
    }

    private CreateTableEvent(NotificationEvent event,
            String catalogName)
            throws MetastoreNotificationException {
        super(event, catalogName);
        Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(getEventType()));
        Preconditions.checkNotNull(MetastoreEventType.CREATE_TABLE, debugString("Event message is null"));
        CreateTableMessage createTableMessage =
                MetastoreEventsProcessor.getMessageDeserializer().getCreateTableMessage(event.getMessage());

        try {
            hmsTbl = createTableMessage.getTableObj();
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to deserialize the event message"), e);
        }
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        throw new UnsupportedOperationException("Unsupported event type: " + getEventType());
    }
}

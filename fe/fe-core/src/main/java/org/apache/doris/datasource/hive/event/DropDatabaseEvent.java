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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.MetaIdMappingsLog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;

/**
 * MetastoreEvent for DROP_DATABASE event type
 */
public class DropDatabaseEvent extends MetastoreEvent {

    // for test
    public DropDatabaseEvent(long eventId, String catalogName, String dbName) {
        super(eventId, catalogName, dbName, null, MetastoreEventType.DROP_DATABASE);
    }

    private DropDatabaseEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkArgument(getEventType().equals(MetastoreEventType.DROP_DATABASE));
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(new DropDatabaseEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            infoLog("catalogName:[{}],dbName:[{}]", catalogName, dbName);
            Env.getCurrentEnv().getCatalogMgr()
                    .unregisterExternalDatabase(dbName, catalogName, true);
        } catch (DdlException e) {
            throw new MetastoreNotificationException(
                    debugString("Failed to process event"), e);
        }
    }

    @Override
    protected List<MetaIdMappingsLog.MetaIdMapping> transferToMetaIdMappings() {
        MetaIdMappingsLog.MetaIdMapping metaIdMapping = new MetaIdMappingsLog.MetaIdMapping(
                    MetaIdMappingsLog.OPERATION_TYPE_DELETE,
                    MetaIdMappingsLog.META_OBJECT_TYPE_DATABASE,
                    dbName);
        return ImmutableList.of(metaIdMapping);
    }
}

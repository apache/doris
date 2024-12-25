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

import org.apache.doris.datasource.MetaIdMappingsLog;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The wrapper parent class of the NotificationEvent class
 */
public abstract class MetastoreEvent {
    private static final Logger LOG = LogManager.getLogger(MetastoreEvent.class);

    protected final String catalogName;

    protected final String dbName;

    protected final String tblName;

    protected final long eventId;

    protected final long eventTime;

    protected final MetastoreEventType eventType;

    protected final NotificationEvent event;
    protected final NotificationEvent metastoreNotificationEvent;

    // for test
    protected MetastoreEvent(long eventId, String catalogName, String dbName,
            String tblName, MetastoreEventType eventType) {
        this.eventId = eventId;
        this.eventTime = -1L;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.eventType = eventType;
        this.metastoreNotificationEvent = null;
        this.event = null;
    }

    // for IgnoredEvent
    protected MetastoreEvent(NotificationEvent event) {
        this.event = event;
        this.metastoreNotificationEvent = event;
        this.eventId = -1;
        this.eventTime = -1L;
        this.catalogName = null;
        this.dbName = null;
        this.tblName = null;
        this.eventType = null;
    }

    protected MetastoreEvent(NotificationEvent event, String catalogName) {
        this.event = event;
        // Some events that we don't care about, dbName may be empty
        String eventDbName = event.getDbName();
        this.dbName = StringUtils.isEmpty(eventDbName) ? eventDbName : eventDbName.toLowerCase(Locale.ROOT);
        this.tblName = event.getTableName();
        this.eventId = event.getEventId();
        this.eventTime = event.getEventTime() * 1000L;
        this.eventType = MetastoreEventType.from(event.getEventType());
        this.metastoreNotificationEvent = event;
        this.catalogName = catalogName;
    }

    /**
     * Can batch processing be performed to improve processing performance
     *
     * @param event
     * @return
     */
    protected boolean canBeBatched(MetastoreEvent event) {
        return false;
    }

    protected abstract void process() throws MetastoreNotificationException;

    protected String getMsgWithEventInfo(String formatSuffix, Object... args) {
        String format = "EventId: %d EventType: %s " + formatSuffix;
        Object[] argsWithEventInfo = getArgsWithEventInfo(args);
        return String.format(format, argsWithEventInfo);
    }

    protected void logInfo(String formatSuffix, Object... args) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        String format = "EventId: {} EventType: {} " + formatSuffix;
        Object[] argsWithEventInfo = getArgsWithEventInfo(args);
        LOG.info(format, argsWithEventInfo);
    }

    /**
     * Add event information to the parameters
     */
    private Object[] getArgsWithEventInfo(Object[] args) {
        Object[] res = new Object[args.length + 2];
        res[0] = eventId;
        res[1] = eventType;
        int i = 2;
        for (Object arg : args) {
            res[i] = arg;
            i++;
        }
        return res;
    }

    protected String getPartitionName(Map<String, String> part, List<String> partitionColNames) {
        if (part.size() == 0) {
            return "";
        }
        if (partitionColNames.size() != part.size()) {
            return "";
        }
        StringBuilder name = new StringBuilder();
        int i = 0;
        for (String colName : partitionColNames) {
            if (i++ > 0) {
                name.append("/");
            }
            name.append(colName);
            name.append("=");
            name.append(part.get(colName));
        }
        return name.toString();
    }

    /**
     * Create a MetaIdMapping list from the event if the event is a create/add/drop event
     */
    protected List<MetaIdMappingsLog.MetaIdMapping> transferToMetaIdMappings() {
        return ImmutableList.of();
    }

    public String getDbName() {
        return dbName;
    }

    public String getTblName() {
        return tblName;
    }

    public long getEventId() {
        return eventId;
    }

    public MetastoreEventType getEventType() {
        return eventType;
    }

    @Override
    public String toString() {
        return "MetastoreEvent{"
                + "eventId=" + eventId
                + ", eventType=" + eventType
                + '}';
    }
}

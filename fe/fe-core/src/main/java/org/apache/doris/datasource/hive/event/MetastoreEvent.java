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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract base class for all MetastoreEvents. A MetastoreEvent is an object used to
 * process a NotificationEvent received from metastore.
 */
public abstract class MetastoreEvent {
    private static final Logger LOG = LogManager.getLogger(MetastoreEvent.class);
    // String.format compatible string to prepend event id and type
    private static final String STR_FORMAT_EVENT_ID_TYPE = "EventId: %d EventType: %s ";

    // logger format compatible string to prepend to a log formatted message
    private static final String LOG_FORMAT_EVENT_ID_TYPE = "EventId: {} EventType: {} ";

    // the notification received from metastore which is processed by this
    protected final NotificationEvent event;

    // dbName from the event
    protected final String dbName;

    // tblName from the event
    protected final String tblName;

    // eventId of the event. Used instead of calling getter on event everytime
    private final long eventId;

    // eventType from the NotificationEvent
    private final MetastoreEventType eventType;

    // Actual notificationEvent object received from Metastore
    protected final NotificationEvent metastoreNotificationEvent;

    protected final String catalogName;

    protected MetastoreEvent(NotificationEvent event, String catalogName) {
        this.event = event;
        this.dbName = event.getDbName();
        this.tblName = event.getTableName();
        this.eventId = event.getEventId();
        this.eventType = MetastoreEventType.from(event.getEventType());
        this.metastoreNotificationEvent = event;
        this.catalogName = catalogName;
    }

    public long getEventId() {
        return eventId;
    }

    public MetastoreEventType getEventType() {
        return eventType;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTblName() {
        return tblName;
    }

    /**
     * Checks if the given event can be batched into this event. Default behavior is
     * to return false which can be overridden by a sub-class.
     * The current version is relatively simple to process batch events, so all that need to be processed are true.
     *
     * @param event The event under consideration to be batched into this event.
     * @return false if event cannot be batched into this event; otherwise true.
     */
    protected boolean canBeBatched(MetastoreEvent event) {
        return false;
    }

    /**
     * Adds the given event into the batch of events represented by this event. Default
     * implementation is to return null. Sub-classes must override this method to
     * implement batching.
     *
     * @param event The event which needs to be added to the batch.
     * @return The batch event which represents all the events batched into this event
     * until now including the given event.
     */
    protected MetastoreEvent addToBatchEvents(MetastoreEvent event) {
        return null;
    }


    protected boolean existInCache() throws MetastoreNotificationException {
        return false;
    }

    /**
     * Returns the number of events represented by this event. For most events this is 1.
     * In case of batch events this could be more than 1.
     */
    protected int getNumberOfEvents() {
        return 1;
    }

    /**
     * Certain events like ALTER_TABLE or ALTER_PARTITION implement logic to ignore
     * some events because they do not affect query results.
     *
     * @return true if this event can be skipped.
     */
    protected boolean canBeSkipped() {
        return false;
    }

    /**
     * Whether the current version of FE supports processing of some events, some events are reserved,
     * and may be processed later version.
     */
    protected boolean isSupported() {
        return false;
    }

    /**
     * Process the information available in the NotificationEvent.
     */
    protected abstract void process() throws MetastoreNotificationException;

    /**
     * Helper method to get debug string with helpful event information prepended to the
     * message. This can be used to generate helpful exception messages
     *
     * @param msgFormatString String value to be used in String.format() for the given message
     * @param args args to the <code>String.format()</code> for the given msgFormatString
     */
    protected String debugString(String msgFormatString, Object... args) {
        String formatString = STR_FORMAT_EVENT_ID_TYPE + msgFormatString;
        Object[] formatArgs = getLogFormatArgs(args);
        return String.format(formatString, formatArgs);
    }

    /**
     * Helper method to generate the format args after prepending the event id and type
     */
    private Object[] getLogFormatArgs(Object[] args) {
        Object[] formatArgs = new Object[args.length + 2];
        formatArgs[0] = getEventId();
        formatArgs[1] = getEventType();
        int i = 2;
        for (Object arg : args) {
            formatArgs[i] = arg;
            i++;
        }
        return formatArgs;
    }

    /**
     * Logs at info level the given log formatted string and its args. The log formatted
     * string should have {} pair at the appropriate location in the string for each arg
     * value provided. This method prepends the event id and event type before logging the
     * message. No-op if the log level is not at INFO
     */
    protected void infoLog(String logFormattedStr, Object... args) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        String formatString = LOG_FORMAT_EVENT_ID_TYPE + logFormattedStr;
        Object[] formatArgs = getLogFormatArgs(args);
        LOG.info(formatString, formatArgs);
    }

    /**
     * Similar to infoLog excepts logs at debug level
     */
    protected void debugLog(String logFormattedStr, Object... args) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        String formatString = LOG_FORMAT_EVENT_ID_TYPE + logFormattedStr;
        Object[] formatArgs = getLogFormatArgs(args);
        LOG.debug(formatString, formatArgs);
    }

    @Override
    public String toString() {
        return String.format(STR_FORMAT_EVENT_ID_TYPE, eventId, eventType);
    }
}

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

package org.apache.doris.connector.hms;

/**
 * One HMS notification event as a connector-SPI DTO — the SPI-clean projection of Hive's
 * {@code NotificationEvent} that {@link HmsClient#getNextNotification} returns, so no Hive thrift type
 * crosses the client interface. It carries exactly the fields the plugin's event parser needs to build
 * a neutral change descriptor: the event id + type + affected db/table, and the raw message payload +
 * its format so the JSON/GZIP message deserializers can extract the details (partition names, renamed
 * objects, etc.).
 */
public final class HmsNotificationEvent {

    private final long eventId;
    private final String eventType;
    private final String dbName;
    private final String tableName;
    private final String message;
    private final String messageFormat;
    private final long eventTime;

    public HmsNotificationEvent(long eventId, String eventType, String dbName, String tableName,
            String message, String messageFormat, long eventTime) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.dbName = dbName;
        this.tableName = tableName;
        this.message = message;
        this.messageFormat = messageFormat;
        this.eventTime = eventTime;
    }

    /** The event's unique incremental id. */
    public long getEventId() {
        return eventId;
    }

    /** The event type name (e.g. {@code CREATE_TABLE}, {@code ADD_PARTITION}). */
    public String getEventType() {
        return eventType;
    }

    /** The affected database name (may be {@code null} for some event types). */
    public String getDbName() {
        return dbName;
    }

    /** The affected table name (may be {@code null} for database-level events). */
    public String getTableName() {
        return tableName;
    }

    /** The raw message payload, to be parsed by the message deserializer for this event's format. */
    public String getMessage() {
        return message;
    }

    /** The message format (e.g. {@code json-0.2}, {@code gzip(json-0.2)}); selects the deserializer. */
    public String getMessageFormat() {
        return messageFormat;
    }

    /** The event time in epoch seconds (as recorded by the metastore). */
    public long getEventTime() {
        return eventTime;
    }

    @Override
    public String toString() {
        return "HmsNotificationEvent{eventId=" + eventId + ", eventType=" + eventType
                + ", db=" + dbName + ", table=" + tableName + ", messageFormat=" + messageFormat
                + ", eventTime=" + eventTime + '}';
    }
}

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

package org.apache.doris.plugin;

import org.apache.doris.qe.ConnectContext;

public class AuditEvent {
    /**
     * Audit plugin event type
     */
    public final static short AUDIT_CONNECTION = 1;
    public final static short AUDIT_QUERY = 2;

    /**
     * Connection sub-event masks
     */
    public final static short AUDIT_CONNECTION_CONNECT = 1;
    public final static short AUDIT_CONNECTION_DISCONNECT = 1 << 1;

    /**
     * Query sub-event masks
     */
    public final static short AUDIT_QUERY_START = 1;
    public final static short AUDIT_QUERY_END = 1 << 1;

    protected String query;

    protected short type;

    protected short masks;

    protected ConnectContext context;

    protected AuditEvent(ConnectContext context) {
        this.context = context;
    }

    protected AuditEvent(ConnectContext context, String query) {
        this.context = context;
        this.query = query;
    }

    public void setEventTypeAndMasks(short type, short masks) {
        this.type = type;
        this.masks = masks;
    }

    public short getEventType() {
        return type;
    }

    public short getEventMasks() {
        return masks;
    }

    public int getConnectId() {
        return context.getConnectionId();
    }

    public String getIp() {
        return context.getRemoteIP();
    }

    public String getUser() {
        return context.getQualifiedUser();
    }

    public long getStmtId() {
        return context.getStmtId();
    }

    public String getQuery() {
        return query;
    }

    public static AuditEvent createConnectEvent(ConnectContext ctx) {
        return new AuditEvent(ctx);
    }

    public static AuditEvent createQueryEvent(ConnectContext ctx, String query) {
        return new AuditEvent(ctx, query);
    }

}

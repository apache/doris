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

package org.apache.doris.protocol.mysql;

/**
 * MySQL server status flags.
 * 
 * <p>These flags are sent by the server in the status_flags field of OK and EOF packets.
 * They indicate the current state of the server regarding transactions, queries, etc.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html
 * 
 * @since 2.0.0
 */
public final class MysqlServerStatusFlag {
    
    private MysqlServerStatusFlag() {
        // Constants class
    }
    
    /** A transaction is currently active */
    public static final int SERVER_STATUS_IN_TRANS = 0x0001;
    
    /** Auto-commit mode is set */
    public static final int SERVER_STATUS_AUTOCOMMIT = 0x0002;
    
    /** More results exist (multi-statement) */
    public static final int SERVER_MORE_RESULTS_EXISTS = 0x0008;
    
    /** Query used no good index */
    public static final int SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
    
    /** Query used no index at all */
    public static final int SERVER_STATUS_NO_INDEX_USED = 0x0020;
    
    /** A cursor exists for this connection */
    public static final int SERVER_STATUS_CURSOR_EXISTS = 0x0040;
    
    /** The last row of the result has been sent */
    public static final int SERVER_STATUS_LAST_ROW_SENT = 0x0080;
    
    /** A database was dropped */
    public static final int SERVER_STATUS_DB_DROPPED = 0x0100;
    
    /** No backslash escapes */
    public static final int SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    
    /** Metadata has changed */
    public static final int SERVER_STATUS_METADATA_CHANGED = 0x0400;
    
    /** Query was slow */
    public static final int SERVER_QUERY_WAS_SLOW = 0x0800;
    
    /** PS out params */
    public static final int SERVER_PS_OUT_PARAMS = 0x1000;
    
    /** In a read-only transaction */
    public static final int SERVER_STATUS_IN_TRANS_READONLY = 0x2000;
    
    /** Session state has changed */
    public static final int SERVER_SESSION_STATE_CHANGED = 0x4000;
}

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

package org.apache.doris.mysql;

import java.util.EnumSet;

// MySQL protocol capability
public class MysqlCapability {
    public static enum Flag {
        CLIENT_LONG_PASSWORD(0x00000001, "CLIENT_LONG_PASSWORD"),
        CLIENT_FOUND_ROWS(0x00000002, "CLIENT_FOUND_ROWS"),
        CLIENT_LONG_FLAG(0x00000004, "CLIENT_LONG_FLAG"),
        CLIENT_CONNECT_WITH_DB(0x00000008, "CLIENT_CONNECT_WITH_DB"),
        CLIENT_NO_SCHEMA(0x00000010, "CLIENT_NO_SCHEMA"),
        CLIENT_COMPRESS(0x00000020, "CLIENT_COMPRESS"),
        CLIENT_ODBC(0x00000040, "CLIENT_ODBC"),
        CLIENT_LOCAL_FILES(0x00000080, "CLIENT_LOCAL_FILES"),
        CLIENT_IGNORE_SPACE(0x00000100, "CLIENT_IGNORE_SPACE"),
        CLIENT_PROTOCOL_41(0x00000200, "CLIENT_PROTOCOL_41"),
        CLIENT_INTERACTIVE(0x00000400, "CLIENT_INTERACTIVE"),
        CLIENT_SSL(0x00000800, "CLIENT_SSL"),
        CLIENT_IGNORE_SIGPIPE(0x00001000, "CLIENT_IGNORE_SIGPIPE"),
        CLIENT_TRANSACTIONS(0x00002000, "CLIENT_TRANSACTIONS"),
        CLIENT_RESERVED(0x00004000, "CLIENT_RESERVED"),
        CLIENT_SECURE_CONNECTION(0x00008000, "CLIENT_SECURE_CONNECTION"),
        CLIENT_MULTI_STATEMENTS(0x00010000, "CLIENT_MULTI_STATEMENTS"),
        CLIENT_MULTI_RESULTS(0x00020000, "CLIENT_MULTI_RESULTS"),
        CLIENT_PS_MULTI_RESULTS(0x00040000, "CLIENT_PS_MULTI_RESULTS"),
        CLIENT_PLUGIN_AUTH(0x00080000, "CLIENT_PLUGIN_AUTH"),
        CLIENT_CONNECT_ATTRS(0x00100000, "CLIENT_CONNECT_ATTRS"),
        CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000, "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA"),
        CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000, "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS"),
        CLIENT_SESSION_TRACK(0x00800000, "CLIENT_SESSION_TRACK"),
        CLIENT_DEPRECATE_EOF(0x01000000, "CLIENT_DEPRECATE_EOF");

        private Flag(int flagBit, String description) {
            this.flagBit = flagBit;
            this.description = description;
        }

        private int flagBit;
        private String description;

        public int getFlagBit() {
            return flagBit;
        }

        public String getComment() {
            return description;
        }

        @Override
        public String toString() {
            return getComment();
        }
    }

    private static final EnumSet<Flag> FLAG_SET = EnumSet.allOf(Flag.class);

    private static final int DEFAULT_FLAGS = Flag.CLIENT_PROTOCOL_41.getFlagBit()
            | Flag.CLIENT_CONNECT_WITH_DB.getFlagBit() | Flag.CLIENT_SECURE_CONNECTION.getFlagBit()
            | Flag.CLIENT_PLUGIN_AUTH.getFlagBit() | Flag.CLIENT_LOCAL_FILES.getFlagBit() | Flag.CLIENT_LONG_FLAG
            .getFlagBit();

    private static final int SSL_FLAGS = Flag.CLIENT_PROTOCOL_41.getFlagBit()
            | Flag.CLIENT_CONNECT_WITH_DB.getFlagBit() | Flag.CLIENT_SECURE_CONNECTION.getFlagBit()
            | Flag.CLIENT_PLUGIN_AUTH.getFlagBit() | Flag.CLIENT_LOCAL_FILES.getFlagBit()
            | Flag.CLIENT_LONG_FLAG.getFlagBit() | Flag.CLIENT_SSL.getFlagBit();

    public static final MysqlCapability DEFAULT_CAPABILITY = new MysqlCapability(DEFAULT_FLAGS);
    public static final MysqlCapability SSL_CAPABILITY = new MysqlCapability(SSL_FLAGS);

    private int flags;

    public MysqlCapability(int flags) {
        this.flags = flags;
    }

    public static boolean isCompatible(MysqlCapability server, MysqlCapability client) {
        return true;
    }

    public int getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (Flag flag : FLAG_SET) {
            if ((flags & flag.getFlagBit()) != 0) {
                if (idx != 0) {
                    sb.append(" | ");
                }
                sb.append(flag.getComment());
                idx++;
            }
        }

        return sb.toString();
    }

    public boolean isProtocol41() {
        return (flags & Flag.CLIENT_PROTOCOL_41.getFlagBit()) != 0;
    }

    public boolean isClientUseSsl() {
        return (flags & Flag.CLIENT_SSL.getFlagBit()) != 0;
    }

    public boolean isTransactions() {

        return (flags & Flag.CLIENT_TRANSACTIONS.getFlagBit()) != 0;
    }

    public boolean isConnectedWithDb() {
        return (flags & Flag.CLIENT_CONNECT_WITH_DB.getFlagBit()) != 0;
    }

    public boolean isPluginAuthDataLengthEncoded() {
        return (flags & Flag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.getFlagBit()) != 0;
    }

    public boolean isConnectAttrs() {
        return (flags & Flag.CLIENT_CONNECT_ATTRS.getFlagBit()) != 0;
    }

    public boolean isPluginAuth() {
        return (flags & Flag.CLIENT_PLUGIN_AUTH.getFlagBit()) != 0;
    }

    public boolean isSecureConnection() {
        return (flags & Flag.CLIENT_SECURE_CONNECTION.getFlagBit()) != 0;
    }

    public boolean isSessionTrack() {
        return (flags & Flag.CLIENT_SESSION_TRACK.getFlagBit()) != 0;
    }

    public boolean supportClientLocalFile() {
        return (flags & Flag.CLIENT_LOCAL_FILES.getFlagBit()) != 0;
    }

    public boolean isDeprecatedEOF() {
        return (flags & Flag.CLIENT_DEPRECATE_EOF.getFlagBit()) != 0;
    }

    public boolean isClientMultiStatements() {
        return (flags & Flag.CLIENT_MULTI_STATEMENTS.getFlagBit()) != 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MysqlCapability)) {
            return false;
        }
        if (flags != ((MysqlCapability) obj).flags) {
            return false;
        }
        return true;
    }
}

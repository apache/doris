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

import com.google.common.collect.Maps;

import java.util.EnumSet;
import java.util.Map;

// MySQL protocol text command
// Reused by arrow flight protocol
public enum MysqlCommand {
    COM_SLEEP("Sleep", 0),
    COM_QUIT("Quit", 1),
    COM_INIT_DB("Init DB", 2),
    COM_QUERY("Query", 3),
    COM_FIELD_LIST("Field List", 4),
    COM_CREATE_DB("Create DB", 5),
    COM_DROP_DB("Drop Db", 6),
    COM_REFRESH("Refresh", 7),
    COM_SHUTDOWN("Shutdown", 8),
    COM_STATISTICS("Statistics", 9),
    COM_PROCESS_INFO("Processlist", 10),
    COM_CONNECT("Connect", 11),
    COM_PROCESS_KILL("Kill", 12),
    COM_DEBUG("Debug", 13),
    COM_PING("Ping", 14),
    COM_TIME("Time", 15),
    COM_DELETE_INSERT("DELETE_INSERT", 16),
    COM_CHANGE_USER("Change user", 17),
    COM_STMT_PREPARE("COM_STMT_PREPARE", 22),
    COM_STMT_EXECUTE("COM_STMT_EXECUTE", 23),
    COM_STMT_SEND_LONG_DATA("COM_STMT_SEND_LONG_DATA", 24),
    COM_STMT_CLOSE("COM_STMT_CLOSE", 25),
    COM_STMT_RESET("COM_STMT_RESET", 26),
    COM_DAEMON("COM_DAEMON", 29),
    COM_RESET_CONNECTION("COM_RESET_CONNECTION", 31);

    private static Map<Integer, MysqlCommand> codeMap = Maps.newHashMap();

    static {
        EnumSet<MysqlCommand> enumSet = EnumSet.allOf(MysqlCommand.class);
        for (MysqlCommand command : enumSet) {
            codeMap.put(command.commandCode, command);
        }
    }

    private final String description;
    private final int commandCode;

    private MysqlCommand(String description, int commandCode) {
        this.description = description;
        this.commandCode = commandCode;
    }

    public static MysqlCommand fromCode(int code) {
        return codeMap.get(code);
    }

    @Override
    public String toString() {
        return description;
    }
}

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

package org.apache.doris.common.util;

import com.google.common.collect.Lists;
import org.apache.doris.thrift.TUniqueId;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class LogBuilder {

    private final StringBuffer stringBuffer;
    private final List<LogEntry> entries;

    public LogBuilder(String identifier) {
        stringBuffer = new StringBuffer(identifier).append("-");
        entries = Lists.newLinkedList();
    }

    public LogBuilder(LogKey key, Long identifier) {
        stringBuffer = new StringBuffer().append(key.name()).append("=").append(identifier).append(", ");
        entries = Lists.newLinkedList();
    }

    public LogBuilder(LogKey key, UUID identifier) {
        TUniqueId tUniqueId = new TUniqueId(identifier.getMostSignificantBits(), identifier.getLeastSignificantBits());
        stringBuffer = new StringBuffer().append(key.name()).append("=").append(DebugUtil.printId(tUniqueId)).append(", ");
        entries = Lists.newLinkedList();
    }

    public LogBuilder(LogKey key, String identifier) {
        stringBuffer = new StringBuffer().append(key.name()).append("=").append(identifier).append(", ");
        entries = Lists.newLinkedList();
    }


    public LogBuilder add(String key, long value) {
        entries.add(new LogEntry(key, String.valueOf(value)));
        return this;
    }

    public LogBuilder add(String key, int value) {
        entries.add(new LogEntry(key, String.valueOf(value)));
        return this;
    }

    public LogBuilder add(String key, float value) {
        entries.add(new LogEntry(key, String.valueOf(value)));
        return this;
    }

    public LogBuilder add(String key, boolean value) {
        entries.add(new LogEntry(key, String.valueOf(value)));
        return this;
    }

    public LogBuilder add(String key, String value) {
        entries.add(new LogEntry(key, String.valueOf(value)));
        return this;
    }

    public LogBuilder add(String key, Object value) {
        if (value == null) {
            entries.add(new LogEntry(key, "null"));
        } else {
            entries.add(new LogEntry(key, value.toString()));
        }
        return this;
    }

    public String build() {
        Iterator<LogEntry> it = entries.iterator();
        while (it.hasNext()) {
            LogEntry logEntry = it.next();
            stringBuffer.append(logEntry.key).append("={").append(logEntry.value).append("}");
            if (it.hasNext()) {
                stringBuffer.append(", ");
            }
        }
        return stringBuffer.toString();
    }

    private class LogEntry {
        String key;
        String value;

        public LogEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    @Override
    public String toString() {
        return build();
    }
}

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

package org.apache.doris.common.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

public class DefaultDorisLogger extends ExtendedLoggerWrapper implements TaggableLogger {

    private final TaggedLogFormat format;
    private final ThreadLocal<Tags> tags;

    public DefaultDorisLogger(ExtendedLogger logger, TaggedLogFormat format) {
        super(logger, logger.getName(), logger.getMessageFactory());
        this.format = format;
        this.tags = ThreadLocal.withInitial(Tags::new);
    }

    @Override
    public TaggableLogger tag(String key, Object value) {
        Tags head = this.tags.get();
        Tags tag = new Tags();
        tag.key = key;
        tag.value = value;
        tag.next = head.next;
        head.next = tag;
        return this;
    }

    @Override
    public void logMessage(String fqcn, Level level, Marker marker, Message message, Throwable t) {
        Tags tags = this.tags.get();
        Message m = tags.next == null ? message : format.getTaggedMessage(message, tags.next);
        super.logMessage(fqcn, level, marker, m, t);
        tags.next = null;
    }
}
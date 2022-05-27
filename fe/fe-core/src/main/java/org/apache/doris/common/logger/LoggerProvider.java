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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.spi.ExtendedLogger;

public class LoggerProvider {

    public static TaggableLogger getLogger(Class<?> clazz) {
        return wrap(LogManager.getLogger(clazz));
    }

    public static TaggableLogger getLogger(String name) {
        return wrap(LogManager.getLogger(name));
    }

    public static TaggableLogger wrap(Logger logger) {
        return new DefaultDorisLogger((ExtendedLogger) logger, getTaggedLogFormat());
    }

    // custom your log format here
    public static TaggedLogFormat getTaggedLogFormat() {
        return (message, tags) -> {
            StringBuilder builder = new StringBuilder(message.getFormattedMessage());
            while (tags != null) {
                builder.append('|').append(tags.key).append('=').append(tags.value);
                tags = tags.next;
            }
            return new SimpleMessage(builder);
        };
    }

}

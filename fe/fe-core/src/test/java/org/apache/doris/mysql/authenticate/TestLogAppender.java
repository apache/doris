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

package org.apache.doris.mysql.authenticate;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TestLogAppender extends AbstractAppender implements AutoCloseable {
    private final Logger logger;
    private final Level originalLevel;
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    private TestLogAppender(String name, Logger logger) {
        super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
        this.logger = logger;
        this.originalLevel = logger.getLevel();
    }

    public static TestLogAppender attach(Class<?> loggerClass) {
        return attach(loggerClass, Level.DEBUG);
    }

    public static TestLogAppender attach(Class<?> loggerClass, Level level) {
        Logger logger = (Logger) LogManager.getLogger(loggerClass);
        TestLogAppender appender = new TestLogAppender(
                "TestLogAppender-" + loggerClass.getSimpleName() + "-" + System.nanoTime(), logger);
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(level);
        return appender;
    }

    @Override
    public void append(LogEvent event) {
        events.add(event.toImmutable());
    }

    public boolean contains(Level level, String messageFragment) {
        for (LogEvent event : events) {
            if (event.getLevel().equals(level)
                    && event.getMessage().getFormattedMessage().contains(messageFragment)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        logger.removeAppender(this);
        logger.setLevel(originalLevel);
        stop();
    }
}

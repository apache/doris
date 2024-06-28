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

package org.apache.doris.common;

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.StringBuilderWriter;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogUtils {

    public static final String STDOUT_LOG_MARKER = "StdoutLogger ";
    public static final String STDERR_LOG_MARKER = "StderrLogger ";

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    private static String formattedTime() {
        LocalDateTime dateTime = LocalDateTime.now();
        return dateTime.format(TIME_FORMATTER);
    }

    // Developer should use `LogUtils.stdout` or `LogUtils.stderr`
    // instead of `System.out` and `System.err`.
    public static void stdout(String message) {
        System.out.println(STDOUT_LOG_MARKER + formattedTime() + " " + message);
    }

    public static void stderr(String message) {
        System.err.println(STDERR_LOG_MARKER + formattedTime() + " " + message);
    }

    // TODO: this custom layout is not used in the codebase, but it is a good example of how to create a custom layout
    // 1. Add log4j2.component.properties in fe/conf with content:
    //      log4j.layoutFactory=org.apache.doris.common.LogUtils$SingleLineExceptionLayout
    // 2. Change PatternLayout in Log4jConfig.java to SingleLineExceptionLayout
    @Plugin(name = "SingleLineExceptionLayout", category = Node.CATEGORY,
            elementType = Layout.ELEMENT_TYPE, printObject = true)
    public static class SingleLineExceptionLayout extends AbstractStringLayout {

        private final PatternLayout patternLayout;

        protected SingleLineExceptionLayout(PatternLayout patternLayout, Charset charset) {
            super(charset);
            this.patternLayout = patternLayout;
        }

        @Override
        public String toSerializable(LogEvent event) {
            StringBuilder result = new StringBuilder(patternLayout.toSerializable(event));

            if (event.getThrown() != null) {
                StringBuilderWriter sw = new StringBuilderWriter();
                event.getThrown().printStackTrace(new PrintWriter(sw));
                String stackTrace = sw.toString().replace("\n", " ").replace("\r", " ");
                result.append(stackTrace);
            }

            return result.toString();
        }

        @PluginFactory
        public static Layout<String> createLayout(
                @PluginAttribute(value = "pattern") String pattern,
                @PluginAttribute(value = "charset", defaultString = "UTF-8") Charset charset) {
            PatternLayout patternLayout = PatternLayout.newBuilder()
                    .withPattern(pattern)
                    .withCharset(charset)
                    .build();
            return new SingleLineExceptionLayout(patternLayout, charset);
        }
    }
}

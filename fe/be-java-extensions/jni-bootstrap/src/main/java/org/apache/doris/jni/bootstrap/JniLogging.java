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

package org.apache.doris.jni.bootstrap;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Sends everything the JVM logs into {@code log/jni.log}.
 *
 * <h2>Why java.util.logging</h2>
 *
 * <p>Not a preference - it is the only backend that can be shared here. Each plugin brings its own
 * logging facade and its own log4j2, and those are different classes in each plugin's classloader;
 * they cannot write to one appender. JUL lives in {@code java.logging}, a JDK module every
 * classloader resolves to the same classes, so a plugin adding {@code log4j-to-jul} or
 * {@code slf4j-jdk14} gets its output into this file with no shared jar anywhere. Trino funnels
 * plugin logging the same way, for the same reason.
 *
 * <p>Rotation is size based, matching what the log4j2 configuration it replaces did: 10 MB, five
 * files. One visible difference: JUL numbers its files, so the live file is {@code jni.log.0} and
 * older ones are {@code jni.log.1}..{@code jni.log.4}. During the migration window that keeps the
 * new runtime's output from interleaving with the old loader's {@code jni.log}.
 */
final class JniLogging {

    private static final int ROTATION_BYTES = 10 * 1024 * 1024;
    private static final int ROTATION_FILES = 5;

    private static boolean configured;

    private JniLogging() {
    }

    /** Idempotent, and never fatal: BE must start even when its log directory is not writable. */
    static synchronized void configure() {
        if (configured) {
            return;
        }
        configured = true;
        try {
            File logFile = resolveLogFile();
            File parent = logFile.getParentFile();
            if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
                throw new IllegalStateException("cannot create log directory " + parent);
            }
            FileHandler handler = new FileHandler(logFile.getPath() + ".%g",
                    ROTATION_BYTES, ROTATION_FILES, true);
            handler.setFormatter(new JniLogFormatter());
            handler.setLevel(Level.INFO);

            Logger root = Logger.getLogger("");
            for (Handler existing : root.getHandlers()) {
                // The default console handler would send plugin logging to be.out, which is where
                // the JVM's own crash output goes; keeping the two apart is the point of jni.log.
                root.removeHandler(existing);
            }
            root.addHandler(handler);
            root.setLevel(Level.INFO);
            Logger.getLogger(JniLogging.class.getName())
                    .info("BE Java plugin logging initialized: " + logFile.getPath() + ".0");
        } catch (Exception | LinkageError e) {
            // Leave JUL at its defaults. Losing the log file must not cost the query engine its
            // Java capability, so this is reported and swallowed.
            System.err.println("Failed to initialize BE Java plugin logging: " + e);
        }
    }

    /**
     * {@code logPath} is set by start_be.sh and already points at the jni log file; the environment
     * fallback keeps unit tests and ad-hoc JVMs working.
     */
    private static File resolveLogFile() {
        String configured = System.getProperty("logPath");
        if (configured != null && !configured.trim().isEmpty()) {
            return new File(configured.trim().replaceAll("//+", "/"));
        }
        String dorisHome = System.getenv("DORIS_HOME");
        if (dorisHome != null && !dorisHome.trim().isEmpty()) {
            return new File(new File(dorisHome, "log"), "jni.log");
        }
        return new File(System.getProperty("java.io.tmpdir"), "doris-jni.log");
    }

    /** Mirrors the pattern the previous log4j2 configuration used, so greps keep working. */
    private static final class JniLogFormatter extends Formatter {
        private static final DateTimeFormatter TIMESTAMP =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

        @Override
        public String format(LogRecord record) {
            StringBuilder text = new StringBuilder(160);
            text.append(TIMESTAMP.format(Instant.ofEpochMilli(record.getMillis())))
                    .append(' ').append(record.getLevel().getName())
                    .append(" [").append(Thread.currentThread().getName()).append("] ")
                    .append(record.getLoggerName())
                    .append(" - ").append(formatMessage(record))
                    .append(System.lineSeparator());
            if (record.getThrown() != null) {
                StringWriter trace = new StringWriter();
                record.getThrown().printStackTrace(new PrintWriter(trace));
                text.append(trace);
            }
            return text.toString();
        }
    }
}

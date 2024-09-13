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

import org.apache.doris.httpv2.config.SpringLog4j2Config;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.apache.logging.log4j.io.IoBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

//
// don't use trace. use INFO, WARN, ERROR, FATAL
//
public class Log4jConfig extends XmlConfiguration {
    private static final long serialVersionUID = 1L;

    private static StringBuilder xmlConfTemplateBuilder = new StringBuilder();

    private static void getXmlConfByStrategy(final String size, final String age) {
        if (Config.log_rollover_strategy.equalsIgnoreCase("size")) {
            xmlConfTemplateBuilder
                    .append("          <IfAny>\n")
                    .append("             <IfAccumulatedFileSize exceeds=\"${").append(size).append("}GB\"/>\n")
                    .append("           </IfAny>\n");
        } else {
            // default age
            xmlConfTemplateBuilder
                    .append("          <IfLastModified age=\"${").append(age).append("}\" />\n");
        }
    }

    // Placeholders
    private static final String RUNTIME_LOG_FORMAT_PLACEHOLDER = "<!--REPLACED BY LOG FORMAT-->";
    private static final String VERBOSE_MODULE_PLACEHOLDER = "<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->";
    private static final String CONSOLE_APPENDER_PLACEHOLDER = "<!--REPLACED BY CONSOLE APPENDER-->";
    private static final String RUNTIME_LOG_FILE_APPENDER_PLACEHOLDER = "<!--REPLACED BY LOG APPENDER-->";
    private static final String RUNTIME_LOG_WARN_FILE_APPENDER_PLACEHOLDER = "<!--REPLACED BY WARN_LOG_APPENDER-->";
    private static final String AUDIT_CONSOLE_LOGGER_PLACEHOLDER = "<!--REPLACED BY AUDIT CONSOLE LOGGER-->";
    private static final String AUDIT_FILE_LOGGER_PLACEHOLDER = "<!--REPLACED BY AUDIT FILE LOGGER-->";
    private static final String RUNTIME_LOG_MARKER_PLACEHOLDER = "<!--REPLACED BY RUNTIME LOG MARKER-->";
    private static final String AUDIT_LOG_MARKER_PLACEHOLDER = "<!--REPLACED BY AUDIT LOG MARKER-->";

    // Appender names
    private static final String RUNTIME_LOG_CONSOLE_APPENDER = "Console";
    private static final String RUNTIME_LOG_FILE_APPENDER = "Sys";
    private static final String RUNTIME_LOG_WARN_FILE_APPENDER = "SysWF";
    private static final String AUDIT_LOG_CONSOLE_APPENDER = "AuditConsole";
    private static final String AUDIT_LOG_FILE_APPENDER = "AuditFile";

    // Log patterns
    private static final String RUNTIME_LOG_PATTERN
            = RUNTIME_LOG_MARKER_PLACEHOLDER + "%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid)"
            + RUNTIME_LOG_FORMAT_PLACEHOLDER + "%m%n";
    private static final String AUDIT_LOG_PATTERN
            = AUDIT_LOG_MARKER_PLACEHOLDER + "%d{yyyy-MM-dd HH:mm:ss,SSS} [%c{1}] %m%n";

    // Log markers
    private static final String RUNTIME_LOG_MARKER = "RuntimeLogger ";
    private static final String AUDIT_LOG_MARKER = "AuditLogger ";

    // @formatter:off
    static {
        // CHECKSTYLE OFF
        xmlConfTemplateBuilder.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
            .append("\n<!-- Auto Generated. DO NOT MODIFY IT! -->\n")
            .append("<Configuration status=\"info\" packages=\"org.apache.doris.common\">\n")
            .append("  <Appenders>\n")
            .append("    <Console name=\"" + RUNTIME_LOG_CONSOLE_APPENDER + "\" target=\"SYSTEM_OUT\">\n")
            .append("      <PatternLayout charset=\"UTF-8\">\n")
            .append("        <Pattern>" + RUNTIME_LOG_PATTERN + "</Pattern>\n")
            .append("      </PatternLayout>\n")
            .append("    </Console>\n")
            .append("    <Console name=\"" + AUDIT_LOG_CONSOLE_APPENDER + "\" target=\"SYSTEM_OUT\">\n")
            .append("      <PatternLayout charset=\"UTF-8\">\n")
            .append("        <Pattern>" + AUDIT_LOG_PATTERN + "</Pattern>\n")
            .append("      </PatternLayout>\n")
            .append("    </Console>\n")
            .append("    <RollingFile name=\"" + RUNTIME_LOG_FILE_APPENDER + "\" fileName=\"${sys_log_dir}/fe.log\" filePattern=\"${sys_log_dir}/fe.log.${sys_file_pattern}-%i${sys_file_postfix}\" immediateFlush=\"${immediate_flush_flag}\">\n")
            .append("      <PatternLayout charset=\"UTF-8\">\n")
            .append("        <Pattern>" + RUNTIME_LOG_PATTERN + "</Pattern>\n")
            .append("      </PatternLayout>\n")
            .append("      <Policies>\n")
            .append("        <TimeBasedTriggeringPolicy/>\n")
            .append("        <SizeBasedTriggeringPolicy size=\"${sys_roll_maxsize}MB\"/>\n")
            .append("      </Policies>\n")
            .append("      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"max\">\n")
            .append("        <Delete basePath=\"${sys_log_dir}/\" maxDepth=\"1\">\n")
            .append("          <IfFileName glob=\"fe.log.*\" />\n");

        getXmlConfByStrategy("info_sys_accumulated_file_size", "sys_log_delete_age");

        xmlConfTemplateBuilder
            .append("        </Delete>\n")
            .append("      </DefaultRolloverStrategy>\n")
            .append("    </RollingFile>\n")
            .append("    <RollingFile name=\"" + RUNTIME_LOG_WARN_FILE_APPENDER + "\" fileName=\"${sys_log_dir}/fe.warn.log\" filePattern=\"${sys_log_dir}/fe.warn.log.${sys_file_pattern}-%i${sys_file_postfix}\" immediateFlush=\"${immediate_flush_flag}\">\n")
            .append("      <PatternLayout charset=\"UTF-8\">\n")
            .append("        <Pattern>" + RUNTIME_LOG_PATTERN + "</Pattern>\n")
            .append("      </PatternLayout>\n")
            .append("      <Policies>\n")
            .append("        <TimeBasedTriggeringPolicy/>\n")
            .append("        <SizeBasedTriggeringPolicy size=\"${sys_roll_maxsize}MB\"/>\n")
            .append("      </Policies>\n")
            .append("      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"max\">\n")
            .append("        <Delete basePath=\"${sys_log_dir}/\" maxDepth=\"1\">\n")
            .append("          <IfFileName glob=\"fe.warn.log.*\" />\n");

        getXmlConfByStrategy("warn_sys_accumulated_file_size", "sys_log_delete_age");

        xmlConfTemplateBuilder
            .append("        </Delete>\n")
            .append("      </DefaultRolloverStrategy>\n")
            .append("    </RollingFile>\n")
            .append("    <RollingFile name=\"" + AUDIT_LOG_FILE_APPENDER + "\" fileName=\"${audit_log_dir}/fe.audit.log\" filePattern=\"${audit_log_dir}/fe.audit.log.${audit_file_pattern}-%i${audit_file_postfix}\">\n")
            .append("      <PatternLayout charset=\"UTF-8\">\n")
            .append("        <Pattern>" + AUDIT_LOG_PATTERN + "</Pattern>\n")
            .append("      </PatternLayout>\n")
            .append("      <Policies>\n")
            .append("        <TimeBasedTriggeringPolicy/>\n")
            .append("        <SizeBasedTriggeringPolicy size=\"${audit_roll_maxsize}MB\"/>\n")
            .append("      </Policies>\n")
            .append("      <DefaultRolloverStrategy max=\"${audit_roll_num}\" fileIndex=\"max\">\n")
            .append("        <Delete basePath=\"${audit_log_dir}/\" maxDepth=\"1\">\n")
            .append("          <IfFileName glob=\"fe.audit.log.*\" />\n");

        getXmlConfByStrategy("audit_sys_accumulated_file_size", "audit_log_delete_age");

        xmlConfTemplateBuilder
            .append("        </Delete>\n")
            .append("      </DefaultRolloverStrategy>\n")
            .append("    </RollingFile>\n")
            .append("  </Appenders>\n")
            .append("  <Loggers>\n")
            .append("    <Root level=\"${sys_log_level}\" includeLocation=\"${include_location_flag}\">\n")
            .append("      " + RUNTIME_LOG_FILE_APPENDER_PLACEHOLDER + "\n")
            .append("      " + RUNTIME_LOG_WARN_FILE_APPENDER_PLACEHOLDER + "\n")
            .append("      " + CONSOLE_APPENDER_PLACEHOLDER + "\n")
            .append("    </Root>\n")
            .append("    <Logger name=\"audit\" level=\"ERROR\" additivity=\"false\">\n")
            .append("      " + AUDIT_FILE_LOGGER_PLACEHOLDER + "\n")
            .append("      " + AUDIT_CONSOLE_LOGGER_PLACEHOLDER + "\n")
            .append("    </Logger>\n")
            .append("    " + VERBOSE_MODULE_PLACEHOLDER + "\n")
            .append("  </Loggers>\n")
            .append("</Configuration>");
        // CHECKSTYLE ON
    }
    // @formatter:on

    private static StrSubstitutor strSub;
    private static String sysLogLevel;
    private static String sysLogMode;
    private static String[] verboseModules;
    private static String[] auditModules;
    // save the generated xml conf template
    private static String logXmlConfTemplate;
    // dir of fe.conf
    public static String confDir;
    // custom conf dir
    public static String customConfDir;
    // Doris uses both system.out and log4j to print log messages.
    // This variable is used to check whether to add console appender to loggers.
    //     If doris is running under daemon mode, then this variable == false, and console logger will not be added.
    //     If doris is not running under daemon mode, then this variable == true, and console logger will be added to
    //     loggers, all logs will be printed to console.
    public static boolean foreground = false;

    private static void reconfig() throws IOException {
        String newXmlConfTemplate = xmlConfTemplateBuilder.toString();

        // sys log config
        // ATTN, sys_log_dir is deprecated, use LOG_DIR instead
        String sysLogDir = Strings.isNullOrEmpty(Config.sys_log_dir) ? System.getenv("LOG_DIR") :
                Config.sys_log_dir;
        String sysRollNum = String.valueOf(Config.sys_log_roll_num);
        String sysDeleteAge = String.valueOf(Config.sys_log_delete_age);
        boolean compressSysLog = Config.sys_log_enable_compress;

        if (!(sysLogLevel.equalsIgnoreCase("INFO")
                || sysLogLevel.equalsIgnoreCase("WARN")
                || sysLogLevel.equalsIgnoreCase("ERROR")
                || sysLogLevel.equalsIgnoreCase("FATAL"))) {
            throw new IOException("sys_log_level config error");
        }

        if (!(sysLogMode.equalsIgnoreCase("NORMAL")
                || sysLogMode.equalsIgnoreCase("BRIEF")
                || sysLogMode.equalsIgnoreCase("ASYNC"))) {
            throw new IOException("sys_log_mode config error");
        }

        String sysLogRollPattern = "%d{yyyyMMdd}";
        String sysRollMaxSize = String.valueOf(Config.log_roll_size_mb);
        if (Config.sys_log_roll_interval.equals("HOUR")) {
            sysLogRollPattern = "%d{yyyyMMddHH}";
        } else if (Config.sys_log_roll_interval.equals("DAY")) {
            sysLogRollPattern = "%d{yyyyMMdd}";
        } else {
            throw new IOException("sys_log_roll_interval config error: " + Config.sys_log_roll_interval);
        }

        // audit log config
        String auditLogDir = Config.audit_log_dir;
        String auditLogRollPattern = "%d{yyyyMMdd}";
        String auditRollNum = String.valueOf(Config.audit_log_roll_num);
        String auditRollMaxSize = String.valueOf(Config.log_roll_size_mb);
        String auditDeleteAge = String.valueOf(Config.audit_log_delete_age);
        boolean compressAuditLog = Config.audit_log_enable_compress;
        if (Config.audit_log_roll_interval.equals("HOUR")) {
            auditLogRollPattern = "%d{yyyyMMddHH}";
        } else if (Config.audit_log_roll_interval.equals("DAY")) {
            auditLogRollPattern = "%d{yyyyMMdd}";
        } else {
            throw new IOException("audit_log_roll_interval config error: " + Config.audit_log_roll_interval);
        }

        // verbose modules and audit log modules
        StringBuilder sb = new StringBuilder();
        for (String s : verboseModules) {
            sb.append("<Logger name='" + s + "' level='DEBUG'/>");
        }
        for (String s : auditModules) {
            sb.append("<Logger name='audit." + s + "' level='INFO'/>");
        }
        newXmlConfTemplate = newXmlConfTemplate.replaceAll(VERBOSE_MODULE_PLACEHOLDER, sb.toString());

        // BRIEF: async, no location
        // ASYNC: async, with location
        // NORMAL: sync, with location
        boolean includeLocation = !sysLogMode.equalsIgnoreCase("BRIEF");
        boolean immediateFlush = sysLogMode.equalsIgnoreCase("NORMAL");
        if (includeLocation) {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(RUNTIME_LOG_FORMAT_PLACEHOLDER, " [%C{1}.%M():%L] ");
        } else {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(RUNTIME_LOG_FORMAT_PLACEHOLDER, " ");
        }
        if (!immediateFlush) {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll("Root", "AsyncRoot");
        }

        if (Config.enable_file_logger) {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(RUNTIME_LOG_FILE_APPENDER_PLACEHOLDER,
                    "<AppenderRef ref=\"" + RUNTIME_LOG_FILE_APPENDER + "\"/>\n");
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(RUNTIME_LOG_WARN_FILE_APPENDER_PLACEHOLDER,
                    "<AppenderRef ref=\"" + RUNTIME_LOG_WARN_FILE_APPENDER + "\" level=\"WARN\"/>\n");
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(AUDIT_FILE_LOGGER_PLACEHOLDER,
                    "<AppenderRef ref=\"" + AUDIT_LOG_FILE_APPENDER + "\"/>\n");
        }

        if (foreground) {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(RUNTIME_LOG_MARKER_PLACEHOLDER, RUNTIME_LOG_MARKER);
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(AUDIT_LOG_MARKER_PLACEHOLDER, AUDIT_LOG_MARKER);
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(CONSOLE_APPENDER_PLACEHOLDER,
                    "<AppenderRef ref=\"" + RUNTIME_LOG_CONSOLE_APPENDER + "\"/>\n");
            newXmlConfTemplate = newXmlConfTemplate.replaceAll(AUDIT_CONSOLE_LOGGER_PLACEHOLDER,
                    "<AppenderRef ref=\"" + AUDIT_LOG_CONSOLE_APPENDER + "\"/>\n");
        }

        Map<String, String> properties = Maps.newHashMap();
        properties.put("sys_log_dir", sysLogDir);
        properties.put("sys_file_pattern", sysLogRollPattern);
        properties.put("sys_roll_maxsize", sysRollMaxSize);
        properties.put("sys_roll_num", sysRollNum);
        properties.put("sys_log_delete_age", sysDeleteAge);
        properties.put("sys_log_level", sysLogLevel);
        properties.put("sys_file_postfix", compressSysLog ? ".gz" : "");

        properties.put("audit_log_dir", auditLogDir);
        properties.put("audit_file_pattern", auditLogRollPattern);
        properties.put("audit_roll_maxsize", auditRollMaxSize);
        properties.put("audit_roll_num", auditRollNum);
        properties.put("audit_log_delete_age", auditDeleteAge);

        properties.put("info_sys_accumulated_file_size", String.valueOf(Config.info_sys_accumulated_file_size));
        properties.put("warn_sys_accumulated_file_size", String.valueOf(Config.warn_sys_accumulated_file_size));
        properties.put("audit_sys_accumulated_file_size", String.valueOf(Config.audit_sys_accumulated_file_size));

        properties.put("include_location_flag", Boolean.toString(includeLocation));
        properties.put("immediate_flush_flag", Boolean.toString(immediateFlush));
        properties.put("audit_file_postfix", compressAuditLog ? ".gz" : "");

        strSub = new StrSubstitutor(new Interpolator(properties));
        newXmlConfTemplate = strSub.replace(newXmlConfTemplate);

        LogUtils.stdout("=====\n" + newXmlConfTemplate + "\n=====");
        logXmlConfTemplate = newXmlConfTemplate;
        SpringLog4j2Config.writeSpringLogConf(customConfDir);

        // new SimpleLog4jConfiguration with xmlConfTemplate
        if (newXmlConfTemplate == null || newXmlConfTemplate.isEmpty()) {
            throw new IOException("The configuration template is empty!");
        }

        Log4jConfig config;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(newXmlConfTemplate.getBytes(StandardCharsets.UTF_8))) {
            ConfigurationSource source = new ConfigurationSource(bis);
            config = new Log4jConfig(source);

            LoggerContext context = (LoggerContext) LogManager.getContext(LogManager.class.getClassLoader(), false);
            context.start(config);
        } catch (Exception e) {
            throw new IOException("Error occurred while configuring Log4j", e);
        }

        redirectStd();
    }

    private static void redirectStd() {
        PrintStream logPrintStream = IoBuilder.forLogger(LogManager.getLogger("system.out")).setLevel(Level.INFO)
                .buildPrintStream();
        System.setOut(logPrintStream);
        PrintStream errorPrintStream = IoBuilder.forLogger(LogManager.getLogger("system.err")).setLevel(Level.ERROR)
                .buildPrintStream();
        System.setErr(errorPrintStream);
    }

    public static String getLogXmlConfTemplate() {
        return logXmlConfTemplate;
    }

    public static class Tuple<X, Y, Z, U> {
        public final X x;
        public final Y y;
        public final Z z;
        public final U u;

        public Tuple(X x, Y y, Z z, U u) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.u = u;
        }
    }

    @Override
    public StrSubstitutor getStrSubstitutor() {
        return strSub;
    }

    public Log4jConfig(final ConfigurationSource configSource) {
        super(LoggerContext.getContext(), configSource);
    }

    public static synchronized void initLogging(String dorisConfDir) throws IOException {
        sysLogLevel = Config.sys_log_level;
        sysLogMode = Config.sys_log_mode;
        verboseModules = Config.sys_log_verbose_modules;
        auditModules = Config.audit_log_modules;
        confDir = dorisConfDir;
        customConfDir = Config.custom_config_dir;
        reconfig();
    }

    public static synchronized Tuple<String, String, String[], String[]> updateLogging(
            String level, String mode, String[] verboseNames, String[] auditNames) throws IOException {
        boolean toReconfig = false;
        if (level != null) {
            sysLogLevel = level;
            toReconfig = true;
        }
        if (mode != null) {
            sysLogMode = mode;
            toReconfig = true;
        }
        if (verboseNames != null) {
            verboseModules = verboseNames;
            toReconfig = true;
        }
        if (auditNames != null) {
            auditModules = auditNames;
            toReconfig = true;
        }
        if (toReconfig) {
            reconfig();
        }
        return new Tuple<>(sysLogLevel, sysLogMode, verboseModules, auditModules);
    }
}

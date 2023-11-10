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

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

//
// don't use trace. use INFO, WARN, ERROR, FATAL
//
public class Log4jConfig extends XmlConfiguration {
    private static final long serialVersionUID = 1L;

    // CHECKSTYLE OFF
    private static String xmlConfTemplate = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
            + "\n<!-- Auto Generated. DO NOT MODIFY IT! -->\n"
            + "<Configuration status=\"info\" packages=\"org.apache.doris.common\">\n"
            + "  <Appenders>\n"
            + "    <Console name=\"Console\" target=\"SYSTEM_OUT\">"
            + "      <PatternLayout charset=\"UTF-8\">\n"
            + "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid)<!--REPLACED BY LOG FORMAT-->%m%n</Pattern>\n"
            + "      </PatternLayout>\n"
            + "    </Console>"
            + "    <RollingFile name=\"Sys\" fileName=\"${sys_log_dir}/fe.log\" filePattern=\"${sys_log_dir}/fe.log.${sys_file_pattern}-%i${sys_file_postfix}\" immediateFlush=\"${immediate_flush_flag}\">\n"
            + "      <PatternLayout charset=\"UTF-8\">\n"
            + "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid)<!--REPLACED BY LOG FORMAT-->%m%n</Pattern>\n"
            + "      </PatternLayout>\n"
            + "      <Policies>\n"
            + "        <TimeBasedTriggeringPolicy/>\n"
            + "        <SizeBasedTriggeringPolicy size=\"${sys_roll_maxsize}MB\"/>\n"
            + "      </Policies>\n"
            + "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n"
            + "        <Delete basePath=\"${sys_log_dir}/\" maxDepth=\"1\">\n"
            + "          <IfFileName glob=\"fe.log.*\" />\n"
            + "          <IfLastModified age=\"${sys_log_delete_age}\" />\n"
            + "        </Delete>\n"
            + "      </DefaultRolloverStrategy>\n"
            + "    </RollingFile>\n"
            + "    <RollingFile name=\"SysWF\" fileName=\"${sys_log_dir}/fe.warn.log\" filePattern=\"${sys_log_dir}/fe.warn.log.${sys_file_pattern}-%i${sys_file_postfix}\" immediateFlush=\"${immediate_flush_flag}\">\n"
            + "      <PatternLayout charset=\"UTF-8\">\n"
            + "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid)<!--REPLACED BY LOG FORMAT-->%m%n</Pattern>\n"
            + "      </PatternLayout>\n"
            + "      <Policies>\n"
            + "        <TimeBasedTriggeringPolicy/>\n"
            + "        <SizeBasedTriggeringPolicy size=\"${sys_roll_maxsize}MB\"/>\n"
            + "      </Policies>\n"
            + "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n"
            + "        <Delete basePath=\"${sys_log_dir}/\" maxDepth=\"1\">\n"
            + "          <IfFileName glob=\"fe.warn.log.*\" />\n"
            + "          <IfLastModified age=\"${sys_log_delete_age}\" />\n"
            + "        </Delete>\n"
            + "      </DefaultRolloverStrategy>\n"
            + "    </RollingFile>\n"
            + "    <RollingFile name=\"Auditfile\" fileName=\"${audit_log_dir}/fe.audit.log\" filePattern=\"${audit_log_dir}/fe.audit.log.${audit_file_pattern}-%i${audit_file_postfix}\">\n"
            + "      <PatternLayout charset=\"UTF-8\">\n"
            + "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} [%c{1}] %m%n</Pattern>\n"
            + "      </PatternLayout>\n"
            + "      <Policies>\n"
            + "        <TimeBasedTriggeringPolicy/>\n"
            + "        <SizeBasedTriggeringPolicy size=\"${audit_roll_maxsize}MB\"/>\n"
            + "      </Policies>\n"
            + "      <DefaultRolloverStrategy max=\"${audit_roll_num}\" fileIndex=\"min\">\n"
            + "        <Delete basePath=\"${audit_log_dir}/\" maxDepth=\"1\">\n"
            + "          <IfFileName glob=\"fe.audit.log.*\" />\n"
            + "          <IfLastModified age=\"${audit_log_delete_age}\" />\n"
            + "        </Delete>\n"
            + "      </DefaultRolloverStrategy>\n"
            + "    </RollingFile>\n"
            + "  </Appenders>\n"
            + "  <Loggers>\n"
            + "    <Root level=\"${sys_log_level}\" includeLocation=\"${include_location_flag}\">\n"
            + "      <AppenderRef ref=\"Sys\"/>\n"
            + "      <AppenderRef ref=\"SysWF\" level=\"WARN\"/>\n"
            + "      <!--REPLACED BY Console Logger-->\n"
            + "    </Root>\n"
            + "    <Logger name=\"audit\" level=\"ERROR\" additivity=\"false\">\n"
            + "      <AppenderRef ref=\"Auditfile\"/>\n"
            + "    </Logger>\n"
            + "    <!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->\n"
            + "  </Loggers>\n"
            + "</Configuration>";
    // CHECKSTYLE ON

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
        String newXmlConfTemplate = xmlConfTemplate;

        // sys log config
        String sysLogDir = Config.sys_log_dir;
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
        newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->",
                sb.toString());

        if (sysLogMode.equalsIgnoreCase("NORMAL")) {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY LOG FORMAT-->",
                    " [%C{1}.%M():%L] ");
        } else {
            newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY LOG FORMAT-->", " ");
            if (sysLogMode.equalsIgnoreCase("ASYNC")) {
                newXmlConfTemplate = newXmlConfTemplate.replaceAll("Root", "AsyncRoot");
            }
        }

        if (foreground) {
            StringBuilder consoleLogger = new StringBuilder();
            consoleLogger.append("<AppenderRef ref=\"Console\"/>\n");
            newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY Console Logger-->",
                    consoleLogger.toString());
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
        properties.put("include_location_flag", sysLogMode.equalsIgnoreCase("NORMAL") ? "true" : "false");
        properties.put("immediate_flush_flag", sysLogMode.equalsIgnoreCase("ASYNC") ? "false" : "true");
        properties.put("audit_file_postfix", compressAuditLog ? ".gz" : "");

        strSub = new StrSubstitutor(new Interpolator(properties));
        newXmlConfTemplate = strSub.replace(newXmlConfTemplate);

        System.out.println("=====");
        System.out.println(newXmlConfTemplate);
        System.out.println("=====");
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

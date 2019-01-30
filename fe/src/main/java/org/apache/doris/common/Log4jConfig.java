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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// 
// don't use trace. use INFO, WARN, ERROR, FATAL
//
public class Log4jConfig extends XmlConfiguration {
    private static final long serialVersionUID = 1L; 

    private static String xmlConfTemplate = "" + 
        "<?xml version='1.0' encoding='UTF-8'?>" +
        "<Configuration status='debug' packages='org.apache.doris.common'>" +
        "<Appenders>" +
            "<RollingFile name='Sys' fileName='${sys_log_dir}/fe.log' " +
            "filePattern='${sys_log_dir}/fe.log.${sys_file_pattern}'>" +
            "<PatternLayout charset='UTF-8'>" +
            "<Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p %tid [%C{1}.%M():%L] %m%n</Pattern>" +
                "</PatternLayout>" +
                "<Policies>" + 
                    "<TimeBasedTriggeringPolicy /><!--SYSLOG-->" +
                    "<SizeBasedTriggeringPolicy size='${sys_roll_maxsize}MB'/>" + 
                "</Policies>" + 
                "<DefaultRolloverStrategy max='${sys_roll_num}' fileIndex='min'/>" +
            "</RollingFile>" + 
            "<RollingFile name='SysWF' fileName='${sys_log_dir}/fe.warn.log' " + 
                                      "filePattern='${sys_log_dir}/fe.warn.log.${sys_file_pattern}'>" +
                "<PatternLayout charset='UTF-8'>" +
            "<Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p %tid [%C{1}.%M():%L] %m%n</Pattern>" +
                "</PatternLayout>" +
                "<Policies>" + 
                    "<TimeBasedTriggeringPolicy /><!--SYSLOG-->" +
                    "<SizeBasedTriggeringPolicy size='${sys_roll_maxsize}MB'/>" + 
                "</Policies>" + 
                "<DefaultRolloverStrategy max='${sys_roll_num}' fileIndex='min'/>" +
            "</RollingFile>" +
            "<RollingFile name='Auditfile' fileName='${audit_log_dir}/fe.audit.log' " + 
                                    "filePattern='${audit_log_dir}/fe.audit.log.${audit_file_pattern}'>" +
                "<PatternLayout charset='UTF-8'>" +
                "<Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} [%c{1}] %m%n</Pattern>" +
                "</PatternLayout>" +
                "<Policies>" + 
                "<TimeBasedTriggeringPolicy /><!--AUDITLOG-->" +
                "<SizeBasedTriggeringPolicy size='${audit_roll_maxsize}MB'/>" + 
                "</Policies>" + 
                "<DefaultRolloverStrategy max='${audit_roll_num}' fileIndex='min'/>" +
                "</RollingFile>" + 
        "</Appenders>" + 
        "<Loggers>" + 
            "<Root level='${sys_log_level}'>" + 
                "<AppenderRef ref='Sys'/>" + 
                "<AppenderRef ref='SysWF' level='WARN'/>" + 
            "</Root>" + 
            "<Logger name='audit' level='ERROR' additivity='false'>" + 
                "<AppenderRef ref='Auditfile'/>" + 
            "</Logger>" + 
            "<Logger name='org.apache.thrift.transport' level='DEBUG'>\n" +
            "    <AppenderRef ref='Sys'/>\n" +
            "</Logger>" +
            "<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->" + 
        "</Loggers>" + 
        "</Configuration>";
    
    private static StrSubstitutor strSub;
    private static String sysLogLevel;
    private static String[] verboseModules;
    private static String[] auditModules;
    
    private static void reconfig() throws IOException {
        String newXmlConfTemplate = xmlConfTemplate;
        // get sys log config properties
        String sysLogDir = Config.sys_log_dir;
        String sysRollNum = String.valueOf(Config.sys_log_roll_num);
        
        if (!(sysLogLevel.equalsIgnoreCase("INFO") || 
                sysLogLevel.equalsIgnoreCase("WARN") ||
                sysLogLevel.equalsIgnoreCase("ERROR") || 
                sysLogLevel.equalsIgnoreCase("FATAL"))) {
            throw new IOException("sys_log_level config error");
        }
        
        String sysFilePattern = "";
        String sysRollMaxSize = "1000000000"; // default ~= 1PB
        String sysrollMode = Config.sys_log_roll_mode;
        if (sysrollMode.equals("TIME-HOUR")) {
            sysFilePattern = "%d{yyyyMMddHH}";      
        } else if (sysrollMode.equals("TIME-DAY")) {
            sysFilePattern = "%d{yyyyMMdd}";        
        } else if (sysrollMode.startsWith("SIZE-MB-")) {
            sysRollMaxSize = String.valueOf(Integer.parseInt(sysrollMode.replaceAll("SIZE-MB-", "")));
            sysFilePattern = "%i";
            newXmlConfTemplate = newXmlConfTemplate.replaceAll("<TimeBasedTriggeringPolicy /><!--SYSLOG-->", "");
        } else {
            throw new IOException("sys_log_roll_mode config error");
        }
        
        // get audit log config
        String auditLogDir = Config.audit_log_dir;
        String auditRollNum = String.valueOf(Config.audit_log_roll_num);
            
        String auditFilePattern = "";
        String auditRollMaxSize = "1000000000"; // default ~= 1PB
        String auditRollMode = Config.audit_log_roll_mode;
        if (auditRollMode.equals("TIME-HOUR")) {
            auditFilePattern = "%d{yyyyMMddHH}";        
        } else if (auditRollMode.equals("TIME-DAY")) {
            auditFilePattern = "%d{yyyyMMdd}";          
        } else if (auditRollMode.startsWith("SIZE-MB-")) {
            auditRollMaxSize = String.valueOf(Integer.parseInt(auditRollMode.replaceAll("SIZE-MB-", "")));
            auditFilePattern = "%i";
            newXmlConfTemplate = newXmlConfTemplate.replaceAll("<TimeBasedTriggeringPolicy /><!--AUDITLOG-->", "");
        } else {
            throw new IOException("audit_log_roll_mode config error");
        }
        
        StringBuilder sb = new StringBuilder();
        for (String s : verboseModules) {
            sb.append("<Logger name='" + s + "' level='DEBUG'/>");
        }
        for (String s : auditModules) {
            sb.append("<Logger name='audit." + s + "' level='INFO'/>");
        }
        newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->",
                                                           sb.toString());
            
        ConcurrentMap<String, String> properties = new ConcurrentHashMap<String, String>();
        properties.put("audit_log_dir", auditLogDir);
        properties.put("audit_file_pattern", auditFilePattern);
        properties.put("audit_roll_maxsize", auditRollMaxSize);
        properties.put("audit_roll_num", auditRollNum);
        
        properties.put("sys_log_dir", sysLogDir);
        properties.put("sys_file_pattern", sysFilePattern);
        properties.put("sys_roll_maxsize", sysRollMaxSize);
        properties.put("sys_roll_num", sysRollNum);
        properties.put("sys_log_level", sysLogLevel);
        
        strSub = new StrSubstitutor(new Interpolator(properties));
        
        // new SimpleLog4jConfiguration with xmlConfTemplate
        ByteArrayInputStream bis = new ByteArrayInputStream(newXmlConfTemplate.getBytes("UTF-8"));
        ConfigurationSource source = new ConfigurationSource(bis);
        Log4jConfig config = new Log4jConfig(source);
        
        // LoggerContext.start(new Configuration)
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.start(config);  
    }
    
    public static class Tuple<X, Y, Z> {
        public final X x; 
        public final Y y; 
        public final Z z; 
        public Tuple(X x, Y y, Z z) { 
            this.x = x; 
            this.y = y; 
            this.z = z;
        } 
    } 
    
    @Override
    public StrSubstitutor getStrSubstitutor() {
        return strSub;
    }

    public Log4jConfig(final ConfigurationSource configSource) {
        super(configSource);
    }
 
    public synchronized static void initLogging() throws IOException {
        sysLogLevel = Config.sys_log_level;
        verboseModules = Config.sys_log_verbose_modules;
        auditModules = Config.audit_log_modules;
        reconfig();
    }

    public synchronized static Tuple<String, String[], String[]> updateLogging(
            String level, String[] verboseNames, String[] auditNames) throws IOException {
        boolean toReconfig = false;
        if (level != null) {
            sysLogLevel = level;
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
        return new Tuple<String, String[], String[]>(sysLogLevel, verboseModules, auditModules);
    }
}

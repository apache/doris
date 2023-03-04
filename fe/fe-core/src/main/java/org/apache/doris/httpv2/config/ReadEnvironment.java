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

package org.apache.doris.httpv2.config;

import org.apache.doris.common.Log4jConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.boot.logging.LogFile;
import org.springframework.boot.logging.LoggingInitializationContext;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.File;


@Component
public class ReadEnvironment implements ApplicationContextAware {
    private static final Logger LOG = LogManager.getLogger(ReadEnvironment.class);
    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    public void reinitializeLoggingSystem() {
        ConfigurableEnvironment environment = (ConfigurableEnvironment) this.applicationContext.getEnvironment();
        File file = new File(Log4jConfig.confDir + SpringLog4j2Config.SPRING_LOG_XML_FILE);
        String logConfig = file.getAbsolutePath();
        LogFile logFile = LogFile.get(environment);
        LoggingSystem system = LoggingSystem.get(LoggingSystem.class.getClassLoader());
        try {
            ResourceUtils.getURL(logConfig).openStream().close();
            // Three step initialization that accounts for the clean up of the logging
            // context before initialization. Spring Boot doesn't initialize a logging
            // system that hasn't had this sequence applied (since 1.4.1).
            system.cleanUp();
            system.beforeInitialize();
            system.initialize(new LoggingInitializationContext(environment),
                    logConfig, logFile);
        } catch (Exception ex) {
            LOG.warn("", ex);
        }

    }
}

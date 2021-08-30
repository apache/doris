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

package org.apache.doris.manager.agent.service;

import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.common.domain.ServiceRole;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public abstract class Service {
    protected ServiceRole serviceRole = null;
    protected String installDir = null;
    protected Integer httpPort = null;
    protected String configFile = null;

    public Service(ServiceRole serviceRole, String installDir, String configFilePath) {
        this.serviceRole = serviceRole;
        this.installDir = installDir;
        this.configFile = configFilePath;
        validService();
    }

    public void validService() {
        File file = new File(this.installDir);
        if (!file.exists() || !file.isDirectory()) {
            throw new AgentException("service install dir not exists,dir:" + this.installDir);
        }

        File configFilePath = new File(this.configFile);
        if (!configFilePath.exists() || !configFilePath.isFile()) {
            throw new AgentException("service config file not exists,path:" + this.configFile);
        }
    }

    public ServiceRole getServiceRole() {
        return serviceRole;
    }

    public String getInstallDir() {
        return installDir;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public abstract boolean isHealth();

    public void load() {
        doLoad();
    }

    public abstract void doLoad();

    public Properties getConfig() {
        String configPath = configFile;
        Properties props;
        try {
            props = new Properties();
            props.load(new FileReader(configPath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new AgentException("load conf file fail:" + configPath);
        }
        return props;
    }

    public String getConfigFilePath() {
        return configFile;
    }

    @Override
    public String toString() {
        return "Service{"
                + "serviceRole=" + serviceRole
                + ", installDir='" + installDir + '\''
                + ", httpPort=" + httpPort
                + '}';
    }
}

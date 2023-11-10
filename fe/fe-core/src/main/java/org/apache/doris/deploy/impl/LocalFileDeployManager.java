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

package org.apache.doris.deploy.impl;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.deploy.DeployManager;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

/*
 * This for Boxer2 Baidu BCC agent
 * It will watch the change of file cluster.info, which contains:
 *  FE=ip:port,ip:port,...
 *  BE=ip:port,ip:port,...
 *  BROKER=ip:port,ip:port,...
 */
public class LocalFileDeployManager extends DeployManager {
    private static final Logger LOG = LogManager.getLogger(LocalFileDeployManager.class);

    public static final String ENV_APP_NAMESPACE = "APP_NAMESPACE";
    public static final String ENV_FE_SERVICE = "FE_SERVICE";
    public static final String ENV_FE_OBSERVER_SERVICE = "FE_OBSERVER_SERVICE";
    public static final String ENV_BE_SERVICE = "BE_SERVICE";
    public static final String ENV_BROKER_SERVICE = "BROKER_SERVICE";
    public static final String ENV_CN_SERVICE = "CN_SERVICE";

    private String clusterInfoFile;

    public LocalFileDeployManager(Env env, long intervalMs) {
        super(env, intervalMs);
        initEnvVariables(ENV_FE_SERVICE, ENV_FE_OBSERVER_SERVICE, ENV_BE_SERVICE, ENV_BROKER_SERVICE, ENV_CN_SERVICE);
    }

    @Override
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup, String envCnServiceGroup) {
        super.initEnvVariables(envElectableFeServiceGroup, envObserverFeServiceGroup, envBackendServiceGroup,
                envBrokerServiceGroup, envCnServiceGroup);

        // namespace
        clusterInfoFile = Strings.nullToEmpty(System.getenv(ENV_APP_NAMESPACE));

        if (Strings.isNullOrEmpty(clusterInfoFile)) {
            LOG.error("failed get cluster info file name: " + ENV_APP_NAMESPACE);
            System.exit(-1);
        }

        LOG.info("get cluster info file name: {}", clusterInfoFile);
    }

    @Override
    public List<SystemInfoService.HostInfo> getGroupHostInfos(NodeType nodeType) {
        String groupName = nodeTypeAttrMap.get(nodeType).getServiceName();
        List<SystemInfoService.HostInfo> result = Lists.newArrayList();
        LOG.info("begin to get group: {} from file: {}", groupName, clusterInfoFile);

        FileChannel channel = null;
        FileLock lock = null;
        BufferedReader bufferedReader = null;
        try (FileInputStream stream = new FileInputStream(clusterInfoFile)) {
            channel = stream.getChannel();
            lock = channel.lock(0, Long.MAX_VALUE, true);

            bufferedReader = new BufferedReader(new InputStreamReader(stream));
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                if (!str.startsWith(groupName)) {
                    continue;
                }
                LOG.debug("read line: {}", str);
                String[] parts = str.split("=");
                if (parts.length != 2 || Strings.isNullOrEmpty(parts[1])) {
                    return result;
                }
                String endpointList = parts[1];
                String[] endpoints = endpointList.split(",");

                for (String endpoint : endpoints) {
                    Pair<String, Integer> hostPorts = SystemInfoService.validateHostAndPort(endpoint);
                    result.add(new SystemInfoService.HostInfo(hostPorts.first, hostPorts.second));
                }

                // only need one line
                break;
            }
        } catch (FileNotFoundException e) {
            LOG.warn("file not found", e);
            return null;
        } catch (IOException e) {
            LOG.warn("failed to read file", e);
            return null;
        } catch (AnalysisException e) {
            LOG.warn("failed to parse endpoint", e);
            return null;
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.warn("failed to close buffered reader after reading file: {}", clusterInfoFile, e);
                }
            }
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException e) {
                    LOG.warn("failed to release lock after reading file: {}", clusterInfoFile, e);
                }
            }
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    LOG.warn("failed to close channel after reading file: {}", clusterInfoFile, e);
                }
            }
        }

        LOG.info("get hosts from {}: {}", groupName, result);
        return result;
    }
}

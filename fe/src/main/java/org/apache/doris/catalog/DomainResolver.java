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

package org.apache.doris.catalog;

import org.apache.doris.common.util.Daemon;
import org.apache.doris.mysql.privilege.PaloAuth;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class DomainResolver extends Daemon {
    private static final Logger LOG = LogManager.getLogger(DomainResolver.class);
    private static final String BNS_RESOLVER_TOOLS_PATH = "/usr/bin/get_instance_by_service";

    private PaloAuth auth;

    private AtomicBoolean isStart = new AtomicBoolean(false);

    public DomainResolver(PaloAuth auth) {
        super("domain resolver", 10 * 1000);
        this.auth = auth;
    }

    @Override
    public synchronized void start() {
        if (isStart.compareAndSet(false, true)) {
            super.start();
        }
    }

    @Override
    public void runOneCycle() {
        // qualified user name -> domain name
        Map<String, Set<String>> userMap = Maps.newHashMap();
        auth.getCopiedWhiteList(userMap);
        LOG.info("begin to resolve domain: {}", userMap);

        // get unique domain names
        Set<String> domainSet = Sets.newHashSet();
        for (Map.Entry<String, Set<String>> entry : userMap.entrySet()) {
            domainSet.addAll(entry.getValue());
        }
        
        // resolve domain name
        for (String domain : domainSet) {
            Set<String> resolvedIPs = Sets.newHashSet();
            if (!resolveWithBNS(domain, resolvedIPs) && !resolveWithBNS(domain, resolvedIPs)) {
                continue;
            }
            LOG.debug("get resolved ip of domain {}: {}", domain, resolvedIPs);

            for (Map.Entry<String, Set<String>> userEntry : userMap.entrySet()) {
                if (!userEntry.getValue().contains(domain)) {
                    continue;
                }

                auth.updateResolovedIps(userEntry.getKey(), domain, resolvedIPs);
            }
        }
    }

    /**
     * Check if domain name is valid
     * 
     * @param host:
     *            currently is the user's whitelist bns or dns name
     * @return true of false
     */
    public boolean isValidDomain(String domainName) {
        if (Strings.isNullOrEmpty(domainName)) {
            LOG.warn("Domain name is null or empty");
            return false;
        }
        Set<String> ipSet = Sets.newHashSet();
        if (!resolveWithDNS(domainName, ipSet) && !resolveWithBNS(domainName, ipSet)) {
            return false;
        }
        return true;
    }

    /**
     * resolve domain name with dns
     */
    public boolean resolveWithDNS(String domainName, Set<String> resolvedIPs) {
        InetAddress[] address;
        try {
            address = InetAddress.getAllByName(domainName);
        } catch (UnknownHostException e) {
            LOG.warn("unknown domain name " + domainName + " with dns: " + e.getMessage());
            return false;
        }

        for (InetAddress addr : address) {
            resolvedIPs.add(addr.getHostAddress());
        }
        return true;
    }

    public boolean resolveWithBNS(String domainName, Set<String> resolvedIPs) {
        File binaryFile = new File(BNS_RESOLVER_TOOLS_PATH);
        if (!binaryFile.exists()) {
            LOG.info("{} does not exist", BNS_RESOLVER_TOOLS_PATH);
            return false;
        }

        final StringBuilder cmdBuilder = new StringBuilder();
        cmdBuilder.append(BNS_RESOLVER_TOOLS_PATH).append(" -a ").append(domainName);
        Process process = null;
        BufferedReader bufferedReader = null;
        String str = null;
        String ip = null;
        try {
            process = Runtime.getRuntime().exec(cmdBuilder.toString());
            bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((str = bufferedReader.readLine()) != null) {
                ip = str.split(" ")[1];
                resolvedIPs.add(ip);
            }
            final int exitCode = process.waitFor();
            // mean something error
            if (exitCode != 0) {
                LOG.warn("failed to execute cmd: {}, exit code: {}", cmdBuilder.toString(), exitCode);
                resolvedIPs.clear();
                return false;
            }
            return true;
        } catch (IOException e) {
            LOG.warn("failed to revole domain with BNS", e);
            resolvedIPs.clear();
            return false;
        } catch (InterruptedException e) {
            LOG.warn("failed to revole domain with BNS", e);
            resolvedIPs.clear();
            return false;
        } finally {
            if (process != null) {
                process.destroy();
            }
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                LOG.error("Close bufferedReader error! " + e);
            }
        }
    }

}

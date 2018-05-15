// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is responseble for resolving domain name, to resolve domain name
 * , first register your domain with username, and so get domain name's ips with
 * call getUserDomainToIps，There may be delays, because domain name resolution 
 * is an asynchronous process.
 * <p/>
 * 
 * @author chenhao
 *
 */
public final class DomainResolverServer {
    private static final Logger LOG = LogManager.getLogger(DomainResolverServer.class);
    private static final int RESOLVING_INTERVAL = 10000;
    private static final String BNS_RESOLVER_TOOLS_PATH = "/usr/bin/get_instance_by_service";
    private static final int RESOLVING_RETRY_COUNT = 2;

    private static DomainResolverServer instance;
    // User to domain name, domains to be resolved
    private Map<String, Set<String>> userToDomainName = Maps.newHashMap();
    // User to domain name, domains have been resolved successfully
    private Map<String, Map<String, Set<String>>> userToDomainNameToIpSet = Maps.newHashMap();
    // Lock for userToDomainName and userToDomainNameToIpSet
    private Lock cloneLock = new ReentrantLock();
    private Thread server;

    private DomainResolverServer() {
        server = new Thread(new ResolverServer());
        server.start();
    }

    public static DomainResolverServer getInstance() {
        if (instance == null) {
            synchronized (DomainResolverServer.class) {
                if (instance == null) {
                    instance = new DomainResolverServer();
                }
            }
        }
        return instance;
    }
    
    //for test
    public Collection<String> getRegisteredUserDomain(String user) {
        return userToDomainName.get(user);
    }
    
    /**
     * @param domainNameCollection
     * @return
     */
    private boolean isNullOrEmptyCollection(Collection<String> domainNameCollection) {
        return domainNameCollection == null || domainNameCollection.isEmpty();
    }
    
    /**
     * Register domain name with username, 
     * 
     * @param user: usually a user account in palo
     * @param domainNameList: currently is the user's whitelist domain name 
     * @return true or false
     */
    public boolean register(String user, Collection<String> domainNameCollection) {
        if (Strings.isNullOrEmpty(user) || isNullOrEmptyCollection(domainNameCollection)) {
            LOG.warn("Register param error user[{}]", user);
            return false;
        }
        
        if (LOG.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            final Iterator<String> iterator = domainNameCollection.iterator();
            while (iterator.hasNext()) {
                sb.append(iterator.next());
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            LOG.debug("Register user[{}], domain[{}] ...", user, sb.toString());            
        }
                
        cloneLock.lock();
        try {
            Set<String> domainNameSet = userToDomainName.get(user);
            if (domainNameSet == null) {
                domainNameSet = Sets.newHashSet();
                userToDomainName.put(user, domainNameSet);
            }
            
            boolean needUpdate = false;
            for (String domainName : domainNameCollection) {
                if (Strings.isNullOrEmpty(domainName)) {
                    LOG.warn("Register param error user[{}] domain[null]", user);
                    continue;
                }
                if (!domainNameSet.contains(domainName)) {
                    domainNameSet.add(domainName);
                    needUpdate = true;
                }
            }
            
            if (needUpdate) {
                server.interrupt();
            }
        } finally {
            cloneLock.unlock();
        }

        return true;
    }

    /**
     * Unregister domain name with username
     * 
     * @param user: usually a user account in palo
     * @param domainNameList: currently is the user's whitelist domain name 
     */
    public void unregister(String user, Collection<String> domainNameCollection) {
        if (Strings.isNullOrEmpty(user) || isNullOrEmptyCollection(domainNameCollection)) {
            LOG.warn("Unregister param error");
            return;
        }
        if (LOG.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            final Iterator<String> iterator = domainNameCollection.iterator();
            while (iterator.hasNext()) {
                sb.append(iterator.next());
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            LOG.debug("Unregister user[{}], domain[{}] ...", user, sb.toString());            
        }

        cloneLock.lock();
        try {
            final Set<String> domainNameSet = userToDomainName.get(user);
            if (domainNameSet == null) {
                return;
            }
            final Map<String, Set<String>> resolvedDomainNameMap = 
                    userToDomainNameToIpSet.get(user);
            for (String domainName : domainNameCollection) {
                domainNameSet.remove(domainName);
                if (resolvedDomainNameMap != null) {
                    resolvedDomainNameMap.remove(domainName);
                }
            }
            
            if (domainNameSet.isEmpty()) {
                userToDomainName.remove(user);
            }
            
            if (resolvedDomainNameMap != null && resolvedDomainNameMap.isEmpty()) {
                userToDomainNameToIpSet.remove(user);
            }
        } finally {
            cloneLock.unlock();
        }
    }
    
    /**
     * Utils for clone 
     * @param srcMap
     * @return cloneMap
     */
    private Map<String, Set<String>> cloneMap(Map<String, Set<String>> srcMap) {
        final Map<String, Set<String>> copyOfMap = Maps.newHashMap();
        for (String key : srcMap.keySet()) {
            final Set<String> sets = Sets.newHashSet();
            for (String value : srcMap.get(key)) {
                sets.add(value);
            }
            copyOfMap.put(key, sets);
        }
        return copyOfMap;
    }

    /**
     * Get user's ips
     * 
     * @param user: usually a user account in palo
     * @return map domain name to ips
     */
    public Map<String, Set<String>> getUserDomainToIps(String user) {
        Map<String, Set<String>> copyOfDomainToIpSet = null;
        cloneLock.lock();
        try {
            final Map<String, Set<String>> domainNameToIpSet = userToDomainNameToIpSet.get(user);
            if (domainNameToIpSet == null || domainNameToIpSet.isEmpty()) {
                LOG.debug("GetUserDomainToIps error, user[{}]", user);
                return null;
            }
            copyOfDomainToIpSet = cloneMap(domainNameToIpSet);
        } finally {
            cloneLock.unlock();
        }
        return copyOfDomainToIpSet;
    }

    /**
     * 
     * @param domainName: currently is the user's whitelist domain name 
     * @return ips
     * @throws UnknownHostException
     */
    private Set<String> getDNSIps(String domainName) throws UnknownHostException {
        final Set<String> hostIpSet = Sets.newHashSet();
        final InetAddress[] address = InetAddress.getAllByName(domainName);
        for (InetAddress addr : address) {
            hostIpSet.add(addr.getHostAddress());
        }
        return hostIpSet;
    }
    
    /**
     * Synchronous resolve domain name with dns
     * 
     * @param domainName: currently is the user's whitelist domain name 
     * @return ips 
     */
    private Set<String> resolveWithDNS(String domainName) {
        try {
            for (int i = 0; i < RESOLVING_RETRY_COUNT; i++) {
                final Set<String> resolvedIpSet = getDNSIps(domainName);
                if (resolvedIpSet.size() > 0) {
                    return resolvedIpSet;
                }
                // avoid last unused wait
                if (i < (RESOLVING_RETRY_COUNT - 1)) {
                    // sleep 5ms for retry
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e2) {
                        LOG.warn("Sleep encounter InterruptedException");
                    }
                }
            }
        } catch (UnknownHostException e) {
            LOG.warn("Resolve domain name[{}] with dns error: {}", domainName, e.getMessage());
            return null;
        }
        
        LOG.warn("Resolve domain name[{}] with dns unknown error.", domainName);
        return null;
    }

    /**
     * 
     * @param domainName: currently is the user's whitelist domain name 
     * @return
     * @throws Exception
     */
    private Set<String> getBNSIps(String domainName) throws Exception {
        final Set<String> resolvedIpSet = Sets.newHashSet();
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
                resolvedIpSet.add(ip);
            }
            final int exitCode = process.waitFor();
            // mean something error
            if (exitCode != 0) {
                LOG.warn("GetBNSIps error code:{}", exitCode);
                resolvedIpSet.clear();
            }
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
        return resolvedIpSet;
    }
    
    /**
     * synchronous resolve domain name with bns
     * 
     * @param domainName: currently is the user's whitelist domain name 
     * @return ips 
     */
    private Set<String> resolveWithBNS(String domainName) {
        try {
            for (int i = 0; i < RESOLVING_RETRY_COUNT; i++) {
                final Set<String> resolvedIpSet = getBNSIps(domainName);
                if (resolvedIpSet.size() > 0) {
                    return resolvedIpSet;
                }
                // avoid last unused wait
                if (i < (RESOLVING_RETRY_COUNT - 1)) {
                    // sleep 5ms for retry
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e2) {
                        LOG.warn("Sleep encounter InterruptedException");
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Resolve domain name[{}] with bns error: {}", domainName, e.getMessage());
            return null;
        }

        LOG.warn("Resolve domain name[{}] with bns unknown error", domainName);
        return null;
    }

    /**
     * Check if domain name is valid
     * 
     * @param host: currently is the user's whitelist bns or dns name 
     * @return true of false
     */
    public boolean isAvaliableDomain(String domainName) {
        if (Strings.isNullOrEmpty(domainName)) {
            LOG.warn("Domain name is null or empty");
            return false;
        }
        Set<String> ips = resolveWithDNS(domainName);
        if (isNullOrEmptyCollection(ips)) {
            ips = resolveWithBNS(domainName);
            if (isNullOrEmptyCollection(ips)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Clone userToDomainName
     * 
     * @return userToHost copy
     */
    private Map<String, Set<String>> cloneUserToDomainName() {
        cloneLock.lock();
        final Map<String, Set<String>> copyMaps = cloneMap(userToDomainName);
        cloneLock.unlock();
        return copyMaps;
    }
 
    // Resolve domain name at intervals, when new domain name are registered 
    // calling register() , server will immediately start a new asynchronous
    // resolvation.
    private class ResolverServer implements Runnable {

        public ResolverServer() {
        }

        @Override
        public void run() {
            LOG.info("DomainResolverServer start");
            while (true) {
                // avoid lock userToDomainName in resolvation
                final Map<String, Set<String>> userToDomainNameCopy = cloneUserToDomainName();
                LOG.debug("Start a new resolvation");
                final Map<String, Map<String, Set<String>>> newUserToDomainNameToIpSet = Maps.newHashMap();
                for (String user : userToDomainNameCopy.keySet()) {
                    LOG.debug("Start resolve user[{}]", user);
                    final Set<String> domainNameWithDNSSet = userToDomainNameCopy.get(user);
                    final Map<String, Set<String>> domainNameToIpSet = Maps.newHashMap();
                    final Set<String> domainNameWithBNSSet = Sets.newHashSet();

                    // 1. check ipWhiteList if contains domain name with dns
                    for (String domainName : domainNameWithDNSSet) {
                        Set<String> ipSet = resolveWithDNS(domainName);
                        if (ipSet == null || ipSet.isEmpty()) {
                            domainNameWithBNSSet.add(domainName);
                            continue;
                        }
                        LOG.debug("DNS: domain[{}] ip[{}]", domainName, ipSet);
                        domainNameToIpSet.put(domainName, ipSet);
                    }

                    // 2. check ipWhiteList if contains domain name with bns
                    for (String domainName : domainNameWithBNSSet) {
                        final Set<String> ipSet = resolveWithBNS(domainName);
                        if (ipSet == null || ipSet.isEmpty()) {
                            continue;
                        }
                        LOG.debug("BNS: domain[{}] ip[{}]", domainName, ipSet);
                        domainNameToIpSet.put(domainName, ipSet);
                    }
                    newUserToDomainNameToIpSet.put(user, domainNameToIpSet);
                }
                cloneLock.lock();
                userToDomainNameToIpSet.clear();
                userToDomainNameToIpSet.putAll(newUserToDomainNameToIpSet);
                cloneLock.unlock();
                try {
                    Thread.sleep(RESOLVING_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.info("Sleep interrupted");
                }
            }
        }

    }
}

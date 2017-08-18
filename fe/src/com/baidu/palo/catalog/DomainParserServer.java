// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public final class DomainParserServer {
    private static final Logger LOG = LogManager.getLogger(DomainParserServer.class);

    private static DomainParserServer instance;
    // user to host to parse
    private Map<String, Set<String>> userToHost = Maps.newHashMap();
    // user to map which successd parsed
    private Map<String, Map<String, Set<String>>> userTohostToIpSet = Maps.newHashMap();
    private Lock cloneLock = new ReentrantLock();
    private Thread server = new Thread();

    private DomainParserServer() {
        server =  new Thread(new ParserServer());
        server.start();
    }

    public static DomainParserServer getInstance() {
        synchronized (DomainParserServer.class) {
            if (instance == null) {
                instance = new DomainParserServer();
            }
        }
        return instance;
    }

    public void register(String user, List<String> hostList) {
        if (Strings.isNullOrEmpty(user) || hostList == null || hostList.size() == 0) {
            LOG.warn("ParserServer register param error user[{}] hostList[{}]", user, hostList);
            return;
        }
        LOG.debug("ParserServer register user[{}], host[{}]", user, hostList.get(0));
        cloneLock.lock();
        for (String host : hostList) {
            if (!userToHost.containsKey(user)) {
                final Set<String> sets = Sets.newHashSet();
                userToHost.put(user, sets);
            }
            userToHost.get(user).add(host);
        }
        server.interrupt();
        cloneLock.unlock();
    }

    public void register(String user, Set<String> hostList) {
        final List<String> list = Lists.newArrayList();
        list.addAll(hostList);
        register(user, list);
    }

    public void unregister(String user, List<String> hostList) {
        if (Strings.isNullOrEmpty(user) || hostList == null || hostList.size() == 0) {
            LOG.warn("ParserServer unregister param error");
            return;
        }
        LOG.debug("ParserServer unregister user[{}], host[{}]", user, hostList.get(0));
        cloneLock.lock();
        for (String host : hostList) {
            if (!userToHost.containsKey(user)) {
                return;
            }
            userToHost.get(user).remove(host);
            userTohostToIpSet.get(user).remove(host);
        }
        cloneLock.unlock();
    }

    /**
     * get ips which belong to user
     * 
     * @param user
     * @return
     */
    public Map<String, Set<String>> getUserHostIp(String user) {
        if (!userTohostToIpSet.containsKey(user)) {
            LOG.warn("ParserServer getUserHostIp error , user[{}]", user);
            return null;
        }
        cloneLock.lock();
        final Map<String, Set<String>> tmp = userTohostToIpSet.get(user);
        final Map<String, Set<String>> ret = Maps.newHashMap();
        for (String key : tmp.keySet()) {
            final Set<String> sets = Sets.newHashSet();
            for (String ip : tmp.get(key)) {
                sets.add(ip);
            }
            ret.put(key, sets);
        }
        cloneLock.unlock();
        return ret;
    }

    /**
     * parse host with dns
     * 
     * @param hostName
     * @return
     */
    private Set<String> getIpFromHost(String hostName) {
        Set<String> hostIpSet = Sets.newHashSet();

        try {
            InetAddress[] address = InetAddress.getAllByName(hostName);
            for (InetAddress addr : address) {
                hostIpSet.add(addr.getHostAddress());
            }
        } catch (UnknownHostException e) {
            LOG.warn("first Unknown host or BNS name: " + hostName);

            hostIpSet.clear();
            InetAddress[] address;

            // sleep 5ms for retry
            try {
                Thread.sleep(5);
            } catch (InterruptedException e2) {
                LOG.warn("sleep encounter InterruptedException");
            }

            try {
                address = InetAddress.getAllByName(hostName);
                for (InetAddress addr : address) {
                    hostIpSet.add(addr.getHostAddress());
                }
            } catch (UnknownHostException e1) {
                LOG.warn("BNS name: " + hostName);
                return null;
            }

        }

        return hostIpSet;
    }

    /**
     * parse host with bns
     * 
     * @param host
     * @return
     */
    private Set<String> getIpsWithBnsHost(String host) {
        Set<String> hostIpSet = new HashSet<>();
        List<String> cmd = new ArrayList<>();
        cmd.add("/usr/bin/get_instance_by_service");
        cmd.add("-a");
        cmd.add(host);

        Process process = null;
        BufferedReader bufferedReader = null;
        try {
            String str = null;
            String hostIp = null;
            process = Runtime.getRuntime().exec(cmd.toArray(new String[cmd.size()]));
            bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((str = bufferedReader.readLine()) != null) {
                hostIp = str.split(" ")[1];
                hostIpSet.add(hostIp);
            }
            int exitCode = process.waitFor();
            // mean something error
            if (exitCode != 0) {
                hostIpSet.clear();
            }
        } catch (Exception e) {
            hostIpSet.clear();
            LOG.warn("Parse host name with cmd error! " + e);
        } finally {
            if (process != null) {
                process.destroy();
            }
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                LOG.error("parseHostNameWithCmd: Close bufferedReader error! " + e);
            }
        }
        return hostIpSet;
    }

    /**
     * check if host is valid
     * 
     * @param host
     * @return
     */
    public boolean isAvaliableDomain(String host) {
        Set<String> tmp = getIpFromHost(host);
        if (tmp == null || tmp.size() == 0) {
            tmp = getIpsWithBnsHost(host);
            if (tmp == null || tmp.size() == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @return
     */
    private Map cloneUserToHostToIpSets() {
        cloneLock.lock();
        final Map<String, Map<String, Set<String>>> retMaps = Maps.newConcurrentMap();
        for (String user : userTohostToIpSet.keySet()) {
            final Map<String, Set<String>> tmp = userTohostToIpSet.get(user);
            final Map<String, Set<String>> ret = Maps.newHashMap();
            for (String key : tmp.keySet()) {
                final Set<String> sets = Sets.newHashSet();
                for (String ip : tmp.get(key)) {
                    sets.add(ip);
                }
                ret.put(key, sets);
            }
            retMaps.put(user, ret);
        }
        cloneLock.unlock();
        return retMaps;
    }

    private Map<String, Set<String>> cloneUserToHost() {
        cloneLock.lock();
        final Map<String, Set<String>> retMaps = Maps.newConcurrentMap();
        for (String user : userToHost.keySet()) {
            final Set<String> tmp = userToHost.get(user);
            final Set<String> ret = Sets.newHashSet();
            for (String key : tmp) {
                ret.add(key);
            }
            retMaps.put(user, ret);
        }
        cloneLock.unlock();
        return retMaps;
    }

    // server
    class ParserServer implements Runnable {

        public ParserServer() {

        }

        @Override
        public void run() {
            LOG.info("ParserServer start");
            while (true) {
                final Map<String, Set<String>> retMaps = cloneUserToHost();
                LOG.debug("ParserServer start new parse");
                final Map<String, Map<String, Set<String>>> tmp = Maps.newHashMap();
                for (String user : retMaps.keySet()) {
                    LOG.debug("start parse user[{}]", user);
                    final Set<String> userHosts = retMaps.get(user);
                    ConcurrentMap<String, Set<String>> hostToIp = Maps.newConcurrentMap();
                    Set<String> dnsHost = Sets.newHashSet();
                    // 1. check ipWhiteList if contains hostName with dns
                    for (String entryIp : userHosts) {
                        Set<String> ipSet = getIpFromHost(entryIp);
                        if (ipSet == null || ipSet.size() == 0) {
                            LOG.warn("parse dns fail , host[{}] may bns", entryIp);
                            dnsHost.add(entryIp);
                            continue;
                        }
                        LOG.debug("dns: host[{}] ip[{}]", entryIp, ipSet);
                        hostToIp.put(entryIp, ipSet);
                    }

                    // 2. check ipWhiteList if contains hostName with bns
                    for (String bnsHost : dnsHost) {
                        final Set<String> ipSet = getIpsWithBnsHost(bnsHost);
                        if (ipSet == null || ipSet.size() == 0) {
                            LOG.warn("parse bns fail , host[{}] ", bnsHost);
                            continue;
                        }
                        LOG.debug("bns: host[{}] ip[{}]", bnsHost, ipSet);
                        hostToIp.put(bnsHost, ipSet);
                    }
                    tmp.put(user, hostToIp);
                }
                cloneLock.lock();
                userTohostToIpSet.clear();
                userTohostToIpSet.putAll(tmp);
                cloneLock.unlock();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    LOG.warn("sleep interrupted");
                }
            }
        }

    }
}

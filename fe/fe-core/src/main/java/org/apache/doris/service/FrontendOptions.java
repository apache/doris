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

package org.apache.doris.service;

import org.apache.doris.common.CIDR;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.NetUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class FrontendOptions {
    private static final Logger LOG = LogManager.getLogger(FrontendOptions.class);

    private static String PRIORITY_CIDR_SEPARATOR = ";";

    private static List<CIDR> priorityCidrs = Lists.newArrayList();
    private static InetAddress localAddr = InetAddress.getLoopbackAddress();
    private static boolean useFqdn = false;

    public static void init() throws UnknownHostException {
        localAddr = null;
        List<InetAddress> hosts = new ArrayList<>();
        NetUtils.getHosts(hosts);
        if (hosts.isEmpty()) {
            LOG.error("fail to get localhost");
            System.exit(-1);
        }
        if (Config.enable_fqdn_mode) {
            initAddrUsingFqdn(hosts);
        } else {
            initAddrUseIp(hosts);
        }
    }

    // 1. If priority_networks is configured . Obtain the IP that complies with the rules,
    // and stop the process if it is not obtained
    // 2. If the priority_networks is not configured, priority should be given to obtaining non loopback IPv4 addresses.
    // If not, use loopback
    static void initAddrUseIp(List<InetAddress> hosts) {
        useFqdn = false;
        analyzePriorityCidrs();
        boolean hasMatchedIp = false;
        if (!priorityCidrs.isEmpty()) {
            for (InetAddress addr : hosts) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("check ip address: {}", addr);
                }
                if (isInPriorNetwork(addr.getHostAddress())) {
                    localAddr = addr;
                    hasMatchedIp = true;
                    break;
                }
            }
            //if all ips not match the priority_networks then print the err log and exit
            if (!hasMatchedIp) {
                LOG.error("ip address range configured for priority_networks does not include the current IP address");
                System.exit(-1);
            }
        } else {
            // if not set frontend_address, get a non-loopback ip
            InetAddress loopBack = null;
            for (InetAddress addr : hosts) {
                if (addr.isLoopbackAddress()) {
                    loopBack = addr;
                } else if (addr instanceof Inet4Address) {
                    localAddr = addr;
                    break;
                }
            }
            // nothing found, use loopback addr
            if (localAddr == null) {
                localAddr = loopBack;
            }
        }
        LOG.info("local address: {}.", localAddr);
    }

    static void initAddrUsingFqdn(List<InetAddress> hosts) throws UnknownHostException {
        useFqdn = true;

        // Try to get FQDN from host
        String fqdnString = null;
        try {
            fqdnString = InetAddress.getLocalHost().getCanonicalHostName();
            String ip = InetAddress.getLocalHost().getHostAddress();
            if (LOG.isDebugEnabled()) {
                LOG.debug("ip is {}", ip);
            }
        } catch (UnknownHostException e) {
            LOG.error("Got a UnknownHostException when try to get FQDN");
            System.exit(-1);
        }

        if (null == fqdnString) {
            LOG.error("Got a null when try to read FQDN");
            System.exit(-1);
        }

        // Try to parse FQDN to get InetAddress
        InetAddress uncheckedInetAddress = null;
        try {
            uncheckedInetAddress = InetAddress.getByName(fqdnString);
        } catch (UnknownHostException e) {
            LOG.error("Got a UnknownHostException when try to parse FQDN, "
                    + "FQDN: {}, message: {}", fqdnString, e.getMessage());
            System.exit(-1);
        }

        if (null == uncheckedInetAddress) {
            LOG.error("uncheckedInetAddress is null");
            System.exit(-1);
        }

        if (!uncheckedInetAddress.getCanonicalHostName().equals(fqdnString)) {
            LOG.error("The FQDN of the parsed address [{}] is not the same as the FQDN obtained from the host [{}]",
                    uncheckedInetAddress.getCanonicalHostName(), fqdnString);
            System.exit(-1);
        }

        // Check the InetAddress obtained via FQDN
        boolean hasInetAddr = false;
        if (LOG.isDebugEnabled()) {
            LOG.debug("fqdnString is {}", fqdnString);
        }
        for (InetAddress addr : hosts) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to match addr, ip: {}, FQDN: {}",
                        addr.getHostAddress(), addr.getCanonicalHostName());
            }
            if (addr.getCanonicalHostName().equals(uncheckedInetAddress.getCanonicalHostName())) {
                hasInetAddr = true;
                break;
            }
        }

        if (hasInetAddr) {
            localAddr = uncheckedInetAddress;
        } else {
            LOG.error("Fail to find right address to start fe by using fqdn");
            System.exit(-1);
        }
        LOG.info("Use FQDN init local addr, FQDN: {}, IP: {}",
                localAddr.getCanonicalHostName(), localAddr.getHostAddress());
    }

    public static String getLocalHostAddress() {
        if (useFqdn) {
            // localAddr.getHostName() is same as run `hostname`
            // localAddr.getCanonicalHostName() is same as the first domain of cat `/etc/hosts|grep 'self ip'`
            // when on k8s
            // run `cat /etc/hosts` return
            // '172.16.0.61 doris-be-cluster1-0.doris-be-cluster1.default.svc.cluster.local doris-be-cluster1-0'
            // run `hostname`
            // return 'doris-be-cluster1-0'
            // node on k8s communication with each other use like
            // 'doris-be-cluster1-0.doris-be-cluster1.default.svc.cluster.local'
            // so we call localAddr.getCanonicalHostName() at here
            return localAddr.getCanonicalHostName();
        }
        return InetAddresses.toAddrString(localAddr);
    }

    private static void analyzePriorityCidrs() {
        String priorCidrs = Config.priority_networks;
        if (Strings.isNullOrEmpty(priorCidrs)) {
            return;
        }
        LOG.info("configured prior_cidrs value: {}", priorCidrs);

        String[] cidrList = priorCidrs.split(PRIORITY_CIDR_SEPARATOR);
        List<String> priorNetworks = Lists.newArrayList(cidrList);
        for (String cidrStr : priorNetworks) {
            priorityCidrs.add(new CIDR(cidrStr));
        }
    }

    private static boolean isInPriorNetwork(String ip) {
        for (CIDR cidr : priorityCidrs) {
            if (cidr.contains(ip)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isBindIPV6() {
        return localAddr instanceof Inet6Address;
    }

}

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
import org.apache.commons.validator.routines.InetAddressValidator;
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

    public static void init() throws UnknownHostException {
        localAddr = null;
        if (!"0.0.0.0".equals(Config.frontend_address)) {
            if (!InetAddressValidator.getInstance().isValidInet4Address(Config.frontend_address)) {
                throw new UnknownHostException("invalid frontend_address: " + Config.frontend_address);
            }
            localAddr = InetAddress.getByName(Config.frontend_address);
            LOG.info("use configured address. {}", localAddr);
            return;
        }

        List<InetAddress> hosts = new ArrayList<>();
        NetUtils.getHosts(hosts);
        if (hosts.isEmpty()) {
            LOG.error("fail to get localhost");
            System.exit(-1);
        }
        if (Config.enable_fqdn_mode) {
            initAddrUseFqdn(hosts);
        } else {
            initAddrUseIp(hosts);
        }
    }


    static void initAddrUseIp(List<InetAddress> hosts) {
        analyzePriorityCidrs();
        // if not set frontend_address, get a non-loopback ip
        InetAddress loopBack = null;
        boolean hasMatchedIp = false;
        for (InetAddress addr : hosts) {
            LOG.debug("check ip address: {}", addr);
            if (addr instanceof Inet4Address) {
                if (addr.isLoopbackAddress()) {
                    loopBack = addr;
                } else if (!priorityCidrs.isEmpty()) {
                    if (isInPriorNetwork(addr.getHostAddress())) {
                        localAddr = addr;
                        hasMatchedIp = true;
                        break;
                    }
                } else {
                    localAddr = addr;
                    break;
                }
            }
        }
        //if all ips not match the priority_networks then print the warning log
        if (!priorityCidrs.isEmpty() && !hasMatchedIp) {
            LOG.warn("ip address range configured for priority_networks does not include the current IP address");
        }
        // nothing found, use loopback addr
        if (localAddr == null) {
            localAddr = loopBack;
        }
        LOG.info("local address: {}.", localAddr);
    }

    static void initAddrUseFqdn(List<InetAddress> hosts) throws UnknownHostException {
        InetAddress uncheckedLocalAddr = InetAddress.getLocalHost();
        if (null == uncheckedLocalAddr) {
            LOG.error("get a null localhost when start fe use fqdn");
            System.exit(-1);
        }
        String uncheckedFqdn = uncheckedLocalAddr.getCanonicalHostName();
        if (null == uncheckedFqdn) {
            LOG.error("get a null canonicalHostName when start fe use fqdn");
            System.exit(-1);
        }
        String uncheckeddIp = InetAddress.getByName(uncheckedFqdn).getHostAddress();
        boolean hasInetAddr = false;
        for (InetAddress addr : hosts) {
            if (uncheckeddIp.equals(addr.getHostAddress())) {
                hasInetAddr = true;
            }
        }
        if (hasInetAddr) {
            localAddr = uncheckedLocalAddr;
        } else {
            LOG.error("fail to find right localhost when start fe use fqdn");
            System.exit(-1);
        }
    }


    public static InetAddress getLocalHost() {
        return localAddr;
    }

    public static String getLocalHostAddress() {
        return InetAddresses.toAddrString(localAddr);
    }

    public static String getHostName() {
        return localAddr.getHostName();
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

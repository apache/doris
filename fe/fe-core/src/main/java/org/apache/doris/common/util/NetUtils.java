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

package org.apache.doris.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {
    private static final Logger LOG = LogManager.getLogger(NetUtils.class);

    // Target format is "host:port"
    public static InetSocketAddress createSocketAddr(String target) {
        int colonIndex = target.indexOf(':');
        if (colonIndex < 0) {
            throw new RuntimeException("Not a host:port pair : " + target);
        }

        String hostname = target.substring(0, colonIndex);
        int port = Integer.parseInt(target.substring(colonIndex + 1));

        return new InetSocketAddress(hostname, port);
    }

    public static void getHosts(List<InetAddress> hosts) {
        Enumeration<NetworkInterface> n = null;

        try {
            n = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e1) {
            throw new RuntimeException("failed to get network interfaces");
        }

        while (n.hasMoreElements()) {
            NetworkInterface e = n.nextElement();
            Enumeration<InetAddress> a = e.getInetAddresses();
            while (a.hasMoreElements()) {
                InetAddress addr = a.nextElement();
                hosts.add(addr);
            }
        }
    }

    public static String getHostnameByIp(String ip) {
        String hostName;
        try {
            InetAddress address = InetAddress.getByName(ip);
            hostName = address.getHostName();
        } catch (UnknownHostException e) {
            LOG.info("unknown host for {}", ip, e);
            hostName = "unknown";
        }
        return hostName;
    }
}

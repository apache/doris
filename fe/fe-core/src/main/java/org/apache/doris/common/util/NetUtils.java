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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.SystemInfoService;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {
    private static final Logger LOG = LogManager.getLogger(NetUtils.class);

    public static final String EDIT_LOG_PORT_SUGGESTION = "Please change the 'edit_log_port' in fe.conf and try again."
            + " But if this is not the first time your start this FE, please DO NOT change it. "
            + " You need to find the service that occupies the port and shut it down,"
            + " and then return the port to Doris.";
    public static final String QUERY_PORT_SUGGESTION = "Please change the 'query_port' in fe.conf and try again.";
    public static final String HTTP_PORT_SUGGESTION = "Please change the 'http_port' in fe.conf and try again. "
            + "But you need to make sure that ALL FEs http_port are same.";
    public static final String HTTPS_PORT_SUGGESTION = "Please change the 'https_port' in fe.conf and try again. "
            + "But you need to make sure that ALL FEs https_port are same.";
    public static final String RPC_PORT_SUGGESTION = "Please change the 'rpc_port' in fe.conf and try again.";
    public static final String ARROW_FLIGHT_SQL_SUGGESTION =
            "Please change the 'arrow_flight_sql_port' in fe.conf and try again.";

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

    public static String getIpByHost(String host) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(host);
        return inetAddress.getHostAddress();
    }

    // This is the implementation is inspired by Apache camel project:
    public static boolean isPortAvailable(String host, int port, String portName, String suggestion) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            LOG.warn("{} {} is already in use. {}", portName, port, suggestion, e);
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }
        return false;
    }

    // assemble an accessible HostPort str, the addr maybe an ipv4/ipv6/FQDN
    // if ip is ipv6 return: [$addr}]:$port
    // if ip is ipv4 or FQDN return: $addr:$port
    public static String getHostPortInAccessibleFormat(String addr, int port) {
        if (InetAddressValidator.getInstance().isValidInet6Address(addr)) {
            return "[" + addr + "]:" + port;
        }
        return addr + ":" + port;
    }

    public static SystemInfoService.HostInfo resolveHostInfoFromHostPort(String hostPort) throws AnalysisException {
        String[] pair;
        if (hostPort.charAt(0) == '[') {
            pair = hostPort.substring(1).split("]:");
        } else {
            pair = hostPort.split(":");
        }
        if (pair.length != 2) {
            throw new AnalysisException("invalid host port: " + hostPort);
        }
        return new SystemInfoService.HostInfo(pair[0], Integer.valueOf(pair[1]));
    }

}

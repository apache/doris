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

package com.baidu.palo.service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.common.util.NetUtils;


public class FrontendOptions {
    private static final Logger LOG = LogManager.getLogger(FrontendOptions.class);
    
    public static void init() {
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        NetUtils.getHosts(hosts);
        if (hosts.isEmpty()) {
            LOG.error("fail to get localhost");
            System.exit(-1);
        }
        
        InetAddress loopBack = null;
        for (InetAddress addr : hosts) {
            if (addr instanceof Inet4Address) {
                if (addr.isLoopbackAddress()) {
                    loopBack = addr;
                } else {
                    localHost = addr;
                    break;
                }
            }
        }
        
        if (localHost == null) {
            localHost = loopBack;
        }
    }
    
    public static InetAddress getLocalHost() {
        return localHost;
    }
    
    public static String getLocalHostAddress() {
        return localHost.getHostAddress();
    }

    private static InetAddress localHost;
};


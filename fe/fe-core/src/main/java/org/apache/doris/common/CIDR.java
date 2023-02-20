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

package org.apache.doris.common;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class CIDR {
    private static final Logger LOG = LogManager.getLogger(CIDR.class);

    private final InetAddress baseIpAddress;
    private BigInteger startIp;
    private BigInteger endIp;
    private BigInteger mask;
    private final int cidrPrefix;

    public CIDR(String cidr) {
        if (!cidr.contains("/")) {
            if (InetAddressValidator.getInstance().isValidInet6Address(cidr)) {
                cidr += "/128";
            } else if (InetAddressValidator.getInstance().isValidInet4Address(cidr)) {
                cidr += "/32";
            } else {
                throw new IllegalArgumentException("Can not parse " + cidr);
            }
        }

        int index = cidr.indexOf("/");
        String addrStr = cidr.substring(0, index);
        String subnet = cidr.substring(index + 1);

        try {
            baseIpAddress = InetAddress.getByName(addrStr);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Can not parse base IP:" + addrStr);
        }
        cidrPrefix = Integer.parseInt(subnet);
        if (cidrPrefix < 0) {
            throw new IllegalArgumentException("Invalid mask length used: " + cidrPrefix);
        }
        if (baseIpAddress instanceof Inet4Address) {
            if (cidrPrefix > 32) {
                throw new IllegalArgumentException("Invalid mask length used: " + cidrPrefix);
            }
        }

        if (baseIpAddress instanceof Inet6Address) {
            if (cidrPrefix > 128) {
                throw new IllegalArgumentException("Invalid mask length used: " + cidrPrefix);
            }
        }

        init();
    }

    public boolean contains(String ip) {
        InetAddress address = null;
        try {
            address = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            LOG.warn("Can not parse ip:" + e.getMessage());
            return false;
        }
        if (address instanceof Inet4Address && baseIpAddress instanceof Inet6Address) {
            return false;
        }

        if (address instanceof Inet6Address && baseIpAddress instanceof Inet4Address) {
            return false;
        }

        BigInteger target = new BigInteger(1, address.getAddress());
        int st = startIp.compareTo(target);
        int te = target.compareTo(endIp);

        return st <= 0 && te <= 0;
    }

    public String getIP() {
        return baseIpAddress.getHostAddress();
    }

    public BigInteger getMask() {
        return mask;
    }

    private void init() {
        ByteBuffer maskBuffer;
        if (baseIpAddress instanceof Inet4Address) {
            maskBuffer = ByteBuffer.allocate(4).putInt(-1);
        } else {
            maskBuffer = ByteBuffer.allocate(16).putLong(-1L).putLong(-1L);
        }

        mask = (new BigInteger(1, maskBuffer.array()).not().shiftRight(cidrPrefix));
        ByteBuffer buffer = ByteBuffer.wrap(baseIpAddress.getAddress());
        BigInteger ipVal = new BigInteger(1, buffer.array());
        startIp = ipVal.and(mask);
        endIp = startIp.add(mask.not());
    }

}

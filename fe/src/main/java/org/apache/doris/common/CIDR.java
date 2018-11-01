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

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CIDR {
    private static final Logger LOG = LogManager.getLogger(CIDR.class);

    // Mask to convert unsigned int to a long (i.e. keep 32 bits)
    private static final long UNSIGNED_INT_MASK = 0x0FFFFFFFFL;

    private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    private static final String SLASH_FORMAT = IP_ADDRESS + "/(\\d{1,2})";
    private static final Pattern addressPattern = Pattern.compile(IP_ADDRESS);
    private static final Pattern cidrPattern = Pattern.compile(SLASH_FORMAT);

    private int address;
    private int netmask;

    // Count the number of 1-bits in a 32-bit integer
    private static ImmutableMap<Integer, Integer> maskBitNumMap;
    static {
        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
        builder.put(0, 0);
        builder.put(0xFFFFFFFF, 32);
        int value = 0x80000000;
        for (int i = 1; i < 32; ++i) {
            builder.put(value, i);
            value = value >> 1;
        }
        maskBitNumMap = builder.build();
        LOG.debug("maskMap={}", maskBitNumMap);
    }

    // Specify IP in CIDR format like: new IPv4("192.168.0.8/16");
    public CIDR(String cidrNotation) {
        // if there is no mask, fill "/32" as suffix
        if(cidrNotation.indexOf("/") == -1) {
            cidrNotation += "/32";
        }

        Matcher matcher = cidrPattern.matcher(cidrNotation);
        if (matcher.matches()) {
            address = matchAddress(matcher);
            // Create a binary netmask from the number of bits specification /x
            int cidrPart = rangeCheck(Integer.parseInt(matcher.group(5)), 0, 32);
            netmask = 0xffffffff;
            netmask = netmask << (32 - cidrPart);
        } else {
            throw new IllegalArgumentException(
                    "Could not parse [" + cidrNotation + "]");
        }
    }

    // Checks if the given IP address contains in subnet
    public boolean contains(String address) {
        return contains(toInteger(address));
    }

     // Get the IP in symbolic form, i.e. xxx.xxx.xxx.xxx
    public String getIP() {
        return format(address);
    }

    // Get the net mask in symbolic form, i.e. xxx.xxx.xxx.xxx
    public String getNetmask() {
        return format(netmask);
    }

    private String format(int val) {
        StringBuilder sb = new StringBuilder(15);

        for (int shift = 24; shift > 0; shift -= 8) {
            // process 3 bytes, from high order byte down.
            sb.append((val >>> shift) & 0xff);
            sb.append('.');
        }
        sb.append(val & 0xff);

        return sb.toString();
    }

     // Get the IP and netmask in CIDR form, i.e. xxx.xxx.xxx.xxx/xx
    public String getCIDR() {
        int numberOfBits = maskBitNumMap.get(netmask);
        return format(address & netmask) + "/" + numberOfBits;
    }

    public String getIpRange() {
        int numberOfBits = maskBitNumMap.get(netmask);
        int numberOfIPs = 0;
        for (int n = 0; n < (32 - numberOfBits); n++) {
            numberOfIPs = numberOfIPs << 1;
            numberOfIPs = numberOfIPs | 0x01;
        }

        int baseIP = address & netmask;
        String firstIP = format(baseIP + 1);
        String lastIP = format(baseIP + numberOfIPs - 1);
        return firstIP + " - " + lastIP;
    }

    // Convenience method to extract the components of a dotted decimal address and
    // pack into an integer using a regex match
    private int matchAddress(Matcher matcher) {
        int addr = 0;
        for (int i = 1; i <= 4; ++i) {
            int n = (rangeCheck(Integer.parseInt(matcher.group(i)), 0, 255));
            addr |= ((n & 0xff) << 8 * (4 - i));
        }
        return addr;
    }

    // Convenience function to check integer boundaries.
    // Checks if a value x is in the range [begin,end].
    // Returns x if it is in range, throws an exception otherwise.
    private int rangeCheck(int value, int begin, int end) {
        if (value >= begin && value <= end) { // [begin,end]
            return value;
        }

        throw new IllegalArgumentException(
                "Value [" + value + "] not in range [" + begin + "," + end + "]");
    }

    // long versions of the values (as unsigned int) which are more suitable for range checking
    private long networkLong()  {
        long network = (address & netmask);
        return network & UNSIGNED_INT_MASK;
    }

    private long broadcastLong(){
        long network = (address & netmask);
        long broadcast = network | ~(netmask);
        return broadcast & UNSIGNED_INT_MASK;
    }

    private int low() {
        int network = (address & netmask);
        return broadcastLong() - networkLong() > 1 ? (network + 1) : 0;
    }

    private int high() {
        int network = (address & netmask);
        int broadcast = network | ~(netmask);
        return broadcastLong() - networkLong() > 1 ? (broadcast - 1) : 0;
    }

    private boolean contains(int ipInt) {
        long addrLong = ipInt & UNSIGNED_INT_MASK;
        long lowLong = low() & UNSIGNED_INT_MASK;
        long highLong = high() & UNSIGNED_INT_MASK;
        return addrLong >= lowLong && addrLong <= highLong;
    }


    // Convert a dotted decimal format address to a packed integer format
    private int toInteger(String address) {
        Matcher matcher = addressPattern.matcher(address);
        if (matcher.matches()) {
            return matchAddress(matcher);
        } else {
            throw new IllegalArgumentException("Could not parse [" + address + "]");
        }
    }
}

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
package org.apache.doris.udf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class IPV6TypeTest {
    // input ipv6
    public String evaluate(InetAddress x) {
        if (x == null) {
            return "null";
        }
        return x.toString();
    }

    // output ipv6
    public InetAddress evaluate(String s) {
        try {
            InetAddress ipv6Address = InetAddress.getByName(s);
            if (ipv6Address.getAddress().length == 16) {
                return ipv6Address;
            } else {
                return null;
            }
        } catch (UnknownHostException e) {
            return null;
        }
    }

    // input array<ipv6>
    public String evaluate(ArrayList<InetAddress> s) {
        String ret = "";
        for (InetAddress ip : s) {
            ret += evaluate(ip) + "udf";
        }
        return ret;
    }

    // output array<ipv6>
    public ArrayList<InetAddress> evaluate() {
        ArrayList<InetAddress> ret = new ArrayList<InetAddress>();
        InetAddress DEFAULT_IPV = null;
        try {
            DEFAULT_IPV = InetAddress.getByName("::1");
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ret.add(DEFAULT_IPV);
        ret.add(DEFAULT_IPV);
        ret.add(DEFAULT_IPV);
        ret.add(null);
        ret.add(null);
        ret.add(DEFAULT_IPV);
        return ret;
    }
}

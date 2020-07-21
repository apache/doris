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

package org.apache.doris.metric;

import org.apache.doris.common.FeConstants;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * Save system metrics such as CPU, MEM, IO, Networks.
 * TODO: Add them gradually
 */
public class SystemMetrics {
    private static final Logger LOG = LogManager.getLogger(SystemMetrics.class);

    // NOTICE: The following 2 tcp metrics is got from /proc/net/snmp
    // So they can only be got on Linux system.
    // All TCP packets retransmitted
    protected long tcpRetransSegs = 0;
    // The number of all problematic TCP packets received
    protected long tcpInErrs = 0;
    // All received TCP packets
    protected long tcpInSegs = 0;
    // All send TCP packets with RST mark
    protected long tcpOutSegs = 0;

    public synchronized void update() {
        updateSnmpMetrics();
    }

    private void updateSnmpMetrics() {
        String procFile = "/proc/net/snmp";
        if (FeConstants.runningUnitTest) {
            procFile = getClass().getClassLoader().getResource("data/net_snmp_normal").getFile();
        }
        try (FileReader fileReader = new FileReader(procFile);
                BufferedReader br = new BufferedReader(fileReader)) {
            String line = null;
            boolean found = false;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("Tcp: ")) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new Exception("can not find tcp metrics");
            }

            // parse the header of TCP
            String[] headers = line.split(" ");
            Map<String, Integer> headerMap = Maps.newHashMap();
            int pos = 0;
            for (int i = 0; i < headers.length; i++) {
                headerMap.put(headers[i], pos++);
            }

            // read the metrics of TCP
            if ((line = br.readLine()) == null) {
                throw new Exception("failed to read metrics of TCP");
            }
            
            // eg: Tcp: 1 200 120000 -1 38920626 10487279 105581903 300009 305 18079291213 15411998945 11808180 22905 4174570 0
            String[] parts = line.split(" ");
            if (parts.length != headerMap.size()) {
                throw new Exception("invalid tcp metrics: " + line + ". header size: " + headerMap.size());
            }

            tcpRetransSegs = Long.valueOf(parts[headerMap.get("RetransSegs")]);
            tcpInErrs = Long.valueOf(parts[headerMap.get("InErrs")]);
            tcpInSegs = Long.valueOf(parts[headerMap.get("InSegs")]);
            tcpOutSegs = Long.valueOf(parts[headerMap.get("OutSegs")]);

        } catch (Exception e) {
            LOG.warn("failed to get /proc/net/snmp", e);
        }
    }

}

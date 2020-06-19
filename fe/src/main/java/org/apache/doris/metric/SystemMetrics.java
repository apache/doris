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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;

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
            // skip tcp header line
            if ((line = br.readLine()) == null) {
                throw new Exception("failed to skip tcp metrics header");
            }
            
            // eg: Tcp: 1 200 120000 -1 38920626 10487279 105581903 300009 305 18079291213 15411998945 11808180 22905 4174570 0
            String[] parts = line.split(" ");
            if (parts.length != 16) {
                throw new Exception("invalid tcp metrics: " + line);
            }

            tcpRetransSegs = Long.valueOf(parts[12]);
            tcpInErrs = Long.valueOf(parts[13]);

        } catch (Exception e) {
            LOG.warn("failed to get /proc/net/snmp", e);
        }
    }

}

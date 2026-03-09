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
import java.util.regex.Pattern;

/**
 * Save system metrics such as CPU, MEM, IO, Networks.
 * TODO: Add them gradually
 */
public class SystemMetrics {
    private static final Logger LOG = LogManager.getLogger(SystemMetrics.class);
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

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
    // Total usable memory
    protected long memTotal = 0;
    // The amount of physical memory not used by the system
    protected long memFree = 0;
    // An estimate of how much memory is available for starting new applications, without swapping
    protected long memAvailable = 0;
    // Memory in buffer cache, so relatively temporary storage for raw disk blocks
    protected long buffers = 0;
    // Memory in the pagecache (Diskcache and Shared Memory)
    protected long cached = 0;
    // CPU time metrics (in USER_HZ, typically 1/100th of a second)
    // Time spent in user mode
    protected long cpuUser = 0;
    // Time spent in user mode with low priority (nice)
    protected long cpuNice = 0;
    // Time spent in system/kernel mode
    protected long cpuSystem = 0;
    // Time spent idle
    protected long cpuIdle = 0;
    // Time waiting for I/O to complete
    protected long cpuIowait = 0;
    // Time servicing hardware interrupts
    protected long cpuIrq = 0;
    // Time servicing software interrupts
    protected long cpuSoftirq = 0;
    // Time stolen by hypervisor (for VMs)
    protected long cpuSteal = 0;
    // Time spent running virtual CPU for guest OS
    protected long cpuGuest = 0;
    // Time spent running niced guest
    protected long cpuGuestNice = 0;

    // Package-private: allows tests to override the /proc/stat resource path
    String cpuStatTestFile = "data/stat_normal";

    // Previous values for calculating deltas
    protected long prevCpuUser = 0;
    protected long prevCpuNice = 0;
    protected long prevCpuSystem = 0;
    protected long prevCpuIdle = 0;
    protected long prevCpuIowait = 0;
    protected long prevCpuIrq = 0;
    protected long prevCpuSoftirq = 0;
    protected long prevCpuSteal = 0;

    // Derived metrics
    // Overall CPU usage percentage
    protected double cpuUsagePercent = 0.0;
    // User mode percentage
    protected double cpuUserPercent = 0.0;
    // System mode percentage
    protected double cpuSystemPercent = 0.0;
    // I/O wait percentage
    protected double cpuIowaitPercent = 0.0;
    // Steal time percentage (important for cloud)
    protected double cpuStealPercent = 0.0;

    // Context switches and process metrics
    protected long ctxt = 0;
    protected long processes = 0;
    protected long procsRunning = 0;
    protected long procsBlocked = 0;

    // Previous values for rate calculation
    protected long prevCtxt = 0;
    protected long prevProcesses = 0;

    // Derived rate metrics
    protected double ctxtRate = 0.0;  // Context switches per second
    protected double processesRate = 0.0;  // Process forks per second
    protected long lastUpdateTime = System.currentTimeMillis();

    public synchronized void update() {
        updateSnmpMetrics();
        updateMemoryMetrics();
        updateCpuMetrics();
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

            // eg: Tcp: 1 200 120000 -1 38920626 10487279 105581903 300009 305
            // 18079291213 15411998945 11808180 22905 4174570 0
            String[] parts = line.split(" ");
            if (parts.length != headerMap.size()) {
                throw new Exception("invalid tcp metrics: " + line + ". header size: " + headerMap.size());
            }

            tcpRetransSegs = Long.valueOf(parts[headerMap.get("RetransSegs")]);
            tcpInErrs = Long.valueOf(parts[headerMap.get("InErrs")]);
            tcpInSegs = Long.valueOf(parts[headerMap.get("InSegs")]);
            tcpOutSegs = Long.valueOf(parts[headerMap.get("OutSegs")]);

        } catch (Exception e) {
            LOG.warn("failed to get /proc/net/snmp: ", e.getMessage());
        }
    }

    private void updateMemoryMetrics() {
        String procFile = "/proc/meminfo";
        String[] memoryMetrics = {"MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached"};
        Map<String, Long> memInfoMap = Maps.newHashMap();

        try (FileReader fileReader = new FileReader(procFile);
                BufferedReader br = new BufferedReader(fileReader)) {
            String[] parts;
            String line = null;
            while ((line = br.readLine()) != null) {
                for (String memoryMetric : memoryMetrics) {
                    if (!memInfoMap.containsKey(memoryMetric) && line.startsWith(memoryMetric)) {
                        parts = WHITESPACE.split(line);
                        if (parts.length != 3) {
                            throw new Exception("invalid memory metrics: " + line);
                        } else {
                            memInfoMap.put(memoryMetric, new Long(parts[1]) * 1024);
                            break;
                        }
                    }
                }
                if (memInfoMap.size() == memoryMetrics.length) {
                    break;
                }
            }
            // if can not get metrics from /proc/meminfo, we will set -1 as default value
            memTotal = memInfoMap.getOrDefault("MemTotal", -1L);
            memFree = memInfoMap.getOrDefault("MemFree", -1L);
            memAvailable = memInfoMap.getOrDefault("MemAvailable", -1L);
            buffers = memInfoMap.getOrDefault("Buffers", -1L);
            cached = memInfoMap.getOrDefault("Cached", -1L);
        } catch (Exception e) {
            LOG.warn("failed to get /proc/meminfo: ", e.getMessage());
        }
    }

    private void updateCpuMetrics() {
        String procFile = "/proc/stat";
        if (FeConstants.runningUnitTest) {
            procFile = getClass().getClassLoader().getResource(cpuStatTestFile).getFile();
        }

        try (FileReader fileReader = new FileReader(procFile);
                BufferedReader br = new BufferedReader(fileReader)) {
            String line;
            boolean cpuLineFound = false;

            // Store previous values for delta calculation
            long prevTotal = prevCpuUser + prevCpuNice + prevCpuSystem + prevCpuIdle
                    + prevCpuIowait + prevCpuIrq + prevCpuSoftirq + prevCpuSteal;

            while ((line = br.readLine()) != null) {
                if (line.startsWith("cpu ")) {  // Overall CPU stats (not per-core)
                    cpuLineFound = true;
                    String[] parts = WHITESPACE.split(line);
                    if (parts.length >= 11) {
                        // Full format with guest/guest_nice (kernel >= 2.6.24 with guest, >= 2.6.33 with guest_nice)
                        cpuUser = Long.parseLong(parts[1]);
                        cpuNice = Long.parseLong(parts[2]);
                        cpuSystem = Long.parseLong(parts[3]);
                        cpuIdle = Long.parseLong(parts[4]);
                        cpuIowait = Long.parseLong(parts[5]);
                        cpuIrq = Long.parseLong(parts[6]);
                        cpuSoftirq = Long.parseLong(parts[7]);
                        cpuSteal = Long.parseLong(parts[8]);
                        cpuGuest = Long.parseLong(parts[9]);
                        cpuGuestNice = Long.parseLong(parts[10]);
                    } else if (parts.length >= 9) {
                        // Format with steal but without guest/guest_nice (kernel >= 2.6.11)
                        cpuUser = Long.parseLong(parts[1]);
                        cpuNice = Long.parseLong(parts[2]);
                        cpuSystem = Long.parseLong(parts[3]);
                        cpuIdle = Long.parseLong(parts[4]);
                        cpuIowait = Long.parseLong(parts[5]);
                        cpuIrq = Long.parseLong(parts[6]);
                        cpuSoftirq = Long.parseLong(parts[7]);
                        cpuSteal = Long.parseLong(parts[8]);
                        cpuGuest = 0;
                        cpuGuestNice = 0;
                    } else if (parts.length >= 8) {
                        // Older format without steal (kernel < 2.6.11)
                        cpuUser = Long.parseLong(parts[1]);
                        cpuNice = Long.parseLong(parts[2]);
                        cpuSystem = Long.parseLong(parts[3]);
                        cpuIdle = Long.parseLong(parts[4]);
                        cpuIowait = Long.parseLong(parts[5]);
                        cpuIrq = Long.parseLong(parts[6]);
                        cpuSoftirq = Long.parseLong(parts[7]);
                        cpuSteal = 0;
                        cpuGuest = 0;
                        cpuGuestNice = 0;
                    }
                } else if (line.startsWith("ctxt ")) {
                    ctxt = parseSingleLongFromLine(line);
                } else if (line.startsWith("processes ")) {
                    processes = parseSingleLongFromLine(line);
                } else if (line.startsWith("procs_running ")) {
                    procsRunning = parseSingleLongFromLine(line);
                } else if (line.startsWith("procs_blocked ")) {
                    procsBlocked = parseSingleLongFromLine(line);
                }
            }

            // Validate that CPU line was found
            if (!cpuLineFound) {
                LOG.warn("failed to get /proc/stat: cpu line not found");
                return;
            }

            // Calculate percentages based on delta (skip on first call to avoid inflated values)
            long total = cpuUser + cpuNice + cpuSystem + cpuIdle + cpuIowait
                    + cpuIrq + cpuSoftirq + cpuSteal;

            // Only calculate percentages if this is not the first call (prevTotal > 0)
            if (prevTotal > 0) {
                long delta = total - prevTotal;

                if (delta > 0) {
                    // Calculate usage as percentage of non-idle time
                    long idleDelta = cpuIdle - prevCpuIdle;
                    long userDelta = cpuUser - prevCpuUser;
                    long systemDelta = cpuSystem - prevCpuSystem;
                    long iowaitDelta = cpuIowait - prevCpuIowait;
                    long stealDelta = cpuSteal - prevCpuSteal;

                    cpuUsagePercent = 100.0 * (delta - idleDelta) / delta;
                    cpuUserPercent = 100.0 * userDelta / delta;
                    cpuSystemPercent = 100.0 * systemDelta / delta;
                    cpuIowaitPercent = 100.0 * iowaitDelta / delta;
                    cpuStealPercent = 100.0 * stealDelta / delta;
                }
            }

            // Store current CPU values as previous for next iteration
            prevCpuUser = cpuUser;
            prevCpuNice = cpuNice;
            prevCpuSystem = cpuSystem;
            prevCpuIdle = cpuIdle;
            prevCpuIowait = cpuIowait;
            prevCpuIrq = cpuIrq;
            prevCpuSoftirq = cpuSoftirq;
            prevCpuSteal = cpuSteal;

            // Calculate context switch and process fork rates (skip on first call)
            long currentTime = System.currentTimeMillis();
            long timeDelta = currentTime - lastUpdateTime;

            if (timeDelta > 0) {
                double timeInSeconds = timeDelta / 1000.0;
                if (prevCtxt > 0) {
                    ctxtRate = (ctxt - prevCtxt) / timeInSeconds;
                }
                if (prevProcesses > 0) {
                    processesRate = (processes - prevProcesses) / timeInSeconds;
                }
            }

            // Store current values as previous for next iteration
            prevCtxt = ctxt;
            prevProcesses = processes;
            lastUpdateTime = currentTime;

        } catch (Exception e) {
            LOG.warn("failed to get /proc/stat: {}", e.getMessage(), e);
        }
    }
    private long parseSingleLongFromLine(String line) {
        String[] parts = WHITESPACE.split(line);
        return parts.length >= 2 ? Long.parseLong(parts[1]) : 0;
    }

}

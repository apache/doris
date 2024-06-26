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

package org.apache.doris.httpv2.controller;

import org.apache.doris.common.Version;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HWPartition;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.FileSystem;
import oshi.software.os.NetworkParams;
import oshi.software.os.OSFileStore;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.software.os.OperatingSystem.ProcessSorting;
import oshi.util.FormatUtil;
import oshi.util.Util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class HardwareInfoController {

    @RequestMapping(path = "/hardware_info/fe", method = RequestMethod.GET)
    public Object index() {
        Map<String, Map<String, String>> map = new HashMap<>();
        appendVersionInfo(map);
        appendHardwareInfo(map);
        return ResponseEntityBuilder.ok(map);
    }

    private void appendVersionInfo(Map<String, Map<String, String>> content) {
        Map<String, String> map = new HashMap<>();
        map.put("Version", Version.DORIS_BUILD_VERSION);
        map.put("Git", Version.DORIS_BUILD_HASH);
        map.put("BuildInfo", Version.DORIS_BUILD_INFO);
        map.put("BuildTime", Version.DORIS_BUILD_TIME);
        content.put("VersionInfo", map);
    }

    private void appendHardwareInfo(Map<String, Map<String, String>> content) {
        SystemInfo si = new SystemInfo();
        OperatingSystem os = si.getOperatingSystem();
        HardwareAbstractionLayer hal = si.getHardware();
        CentralProcessor processor = hal.getProcessor();
        GlobalMemory memory = hal.getMemory();
        Map<String, String> map = new HashMap<>();
        map.put("OS", String.join("<br>", getOperatingSystem(os)));
        map.put("Processor", String.join("<br>", getProcessor(processor)));
        map.put("Memory", String.join("<br>", getMemory(memory)));
        map.put("Processes", String.join("<br>", getProcesses(os, memory)));
        map.put("Disk", String.join("<br>", getDisks(hal.getDiskStores())));
        map.put("FileSystem", String.join("<br>", getFileSystem(os.getFileSystem())));
        map.put("NetworkInterface", String.join("<br>", getNetworkInterfaces(hal.getNetworkIFs())));
        map.put("NetworkParameter", String.join("<br>", getNetworkParameters(os.getNetworkParams())));
        content.put("HardwareInfo", map);
    }

    private List<String> getOperatingSystem(OperatingSystem os) {
        List<String> osInfo = new ArrayList<>();
        osInfo.add(String.valueOf(os));
        osInfo.add("Booted: " + Instant.ofEpochSecond(os.getSystemBootTime()));
        osInfo.add("Uptime: " + FormatUtil.formatElapsedSecs(os.getSystemUptime()));
        osInfo.add("Running with" + (os.isElevated() ? "" : "out") + " elevated permissions.");
        return osInfo;
    }

    private List<String> getProcessor(CentralProcessor processor) {
        List<String> processorInfo = new ArrayList<>();
        processorInfo.add(String.valueOf(processor));
        processorInfo.add(" " + processor.getPhysicalPackageCount() + " physical CPU package(s)");
        processorInfo.add(" " + processor.getPhysicalProcessorCount() + " physical CPU core(s)");
        processorInfo.add(" " + processor.getLogicalProcessorCount() + " logical CPU(s)");

        processorInfo.add("Identifier:&nbsp;&nbsp; " + processor.getProcessorIdentifier().getIdentifier());
        processorInfo.add("ProcessorID:&nbsp;&nbsp; " + processor.getProcessorIdentifier().getProcessorID());
        processorInfo.add("Context Switches/Interrupts:&nbsp;&nbsp; " + processor.getContextSwitches()
                + " / " + processor.getInterrupts() + "<br>");

        long[] prevTicks = processor.getSystemCpuLoadTicks();
        long[][] prevProcTicks = processor.getProcessorCpuLoadTicks();
        processorInfo.add("CPU, IOWait, and IRQ ticks @ 0 sec:&nbsp;&nbsp;" + Arrays.toString(prevTicks));
        // Wait a second...
        Util.sleep(1000);
        long[] ticks = processor.getSystemCpuLoadTicks();
        processorInfo.add("CPU, IOWait, and IRQ ticks @ 1 sec:&nbsp;&nbsp;" + Arrays.toString(ticks));
        long user = ticks[CentralProcessor.TickType.USER.getIndex()]
                - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long nice = ticks[CentralProcessor.TickType.NICE.getIndex()]
                - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long sys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()]
                - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()]
                - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()]
                - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()]
                - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
        long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()]
                - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
        long steal = ticks[CentralProcessor.TickType.STEAL.getIndex()]
                - prevTicks[CentralProcessor.TickType.STEAL.getIndex()];
        long totalCpu = user + nice + sys + idle + iowait + irq + softirq + steal;

        processorInfo.add(String.format(
                "User: %.1f%% Nice: %.1f%% System: %.1f%% Idle:"
                        + " %.1f%% IOwait: %.1f%% IRQ: %.1f%% SoftIRQ: %.1f%% Steal: %.1f%%",
                100d * user / totalCpu, 100d * nice / totalCpu, 100d * sys / totalCpu, 100d * idle / totalCpu,
                100d * iowait / totalCpu, 100d * irq / totalCpu, 100d * softirq / totalCpu, 100d * steal / totalCpu));
        processorInfo.add(String.format("CPU load:&nbsp;&nbsp; %.1f%%",
                processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100));
        double[] loadAverage = processor.getSystemLoadAverage(3);
        processorInfo.add("CPU load averages:&nbsp;&nbsp;"
                + (loadAverage[0] < 0 ? " N/A" : String.format(" %.2f", loadAverage[0]))
                + (loadAverage[1] < 0 ? " N/A" : String.format(" %.2f", loadAverage[1]))
                + (loadAverage[2] < 0 ? " N/A" : String.format(" %.2f", loadAverage[2])));
        // per core CPU
        StringBuilder procCpu = new StringBuilder("CPU load per processor:&nbsp;&nbsp;");
        double[] load = processor.getProcessorCpuLoadBetweenTicks(prevProcTicks);
        for (double avg : load) {
            procCpu.append(String.format(" %.1f%%", avg * 100));
        }
        processorInfo.add(procCpu.toString());
        long freq = processor.getProcessorIdentifier().getVendorFreq();
        if (freq > 0) {
            processorInfo.add("Vendor Frequency:&nbsp;&nbsp; " + FormatUtil.formatHertz(freq));
        }
        freq = processor.getMaxFreq();
        if (freq > 0) {
            processorInfo.add("Max Frequency:&nbsp;&nbsp; " + FormatUtil.formatHertz(freq));
        }
        long[] freqs = processor.getCurrentFreq();
        if (freqs[0] > 0) {
            StringBuilder sb = new StringBuilder("Current Frequencies:&nbsp;&nbsp; ");
            for (int i = 0; i < freqs.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(FormatUtil.formatHertz(freqs[i]));
            }
            processorInfo.add(sb.toString());
        }
        return processorInfo;
    }

    private List<String> getMemory(GlobalMemory memory) {
        List<String> memoryInfo = new ArrayList<>();
        memoryInfo.add("Memory:&nbsp;&nbsp; " + FormatUtil.formatBytes(memory.getAvailable()) + "/"
                + FormatUtil.formatBytes(memory.getTotal()));
        VirtualMemory vm = memory.getVirtualMemory();
        memoryInfo.add("Swap used:&nbsp;&nbsp; " + FormatUtil.formatBytes(vm.getSwapUsed()) + "/"
                + FormatUtil.formatBytes(vm.getSwapTotal()));
        return memoryInfo;
    }

    private List<String> getProcesses(OperatingSystem os, GlobalMemory memory) {
        List<String> processInfo = new ArrayList<>();
        processInfo.add("Processes:&nbsp;&nbsp; " + os.getProcessCount()
                + ", Threads:&nbsp;&nbsp; " + os.getThreadCount());
        // Sort by highest CPU

        List<OSProcess> procs = os.getProcesses((osProcess) -> true, ProcessSorting.CPU_DESC, 5);

        processInfo.add("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; PID  %CPU %MEM       VSZ       RSS Name");
        for (int i = 0; i < procs.size() && i < 5; i++) {
            OSProcess p = procs.get(i);
            processInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; %5d %5.1f %4.1f %9s %9s %s",
                    p.getProcessID(),
                    100d * (p.getKernelTime() + p.getUserTime()) / p.getUpTime(),
                    100d * p.getResidentSetSize() / memory.getTotal(), FormatUtil.formatBytes(p.getVirtualSize()),
                    FormatUtil.formatBytes(p.getResidentSetSize()), p.getName()));
        }
        return processInfo;
    }

    private List<String> getDisks(List<HWDiskStore> diskStores) {
        List<String> diskInfo = new ArrayList<>();
        diskInfo.add("Disks:&nbsp;&nbsp;");
        for (HWDiskStore disk : diskStores) {
            boolean readwrite = disk.getReads() > 0 || disk.getWrites() > 0;
            diskInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; %s:"
                            + " (model: %s - S/N: %s) size: %s, reads: %s (%s), writes: %s (%s), xfer: %s ms",
                    disk.getName(), disk.getModel(), disk.getSerial(),
                    disk.getSize() > 0 ? FormatUtil.formatBytesDecimal(disk.getSize()) : "?",
                    readwrite ? disk.getReads() : "?", readwrite ? FormatUtil.formatBytes(disk.getReadBytes()) : "?",
                    readwrite ? disk.getWrites() : "?", readwrite ? FormatUtil.formatBytes(disk.getWriteBytes()) : "?",
                    readwrite ? disk.getTransferTime() : "?"));
            List<HWPartition> partitions = disk.getPartitions();
            for (HWPartition part : partitions) {
                diskInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
                                + " |-- %s: %s (%s) Maj:Min=%d:%d, size: %s%s", part.getIdentification(),
                        part.getName(), part.getType(), part.getMajor(), part.getMinor(),
                        FormatUtil.formatBytesDecimal(part.getSize()),
                        part.getMountPoint().isEmpty() ? "" : " @ " + part.getMountPoint()));
            }
        }
        return diskInfo;
    }

    private List<String> getFileSystem(FileSystem fileSystem) {
        List<String> fsInfo = new ArrayList<>();
        fsInfo.add("File System:&nbsp;&nbsp;");

        fsInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;File Descriptors: %d/%d", fileSystem.getOpenFileDescriptors(),
                fileSystem.getMaxFileDescriptors()));

        List<OSFileStore> fsList = fileSystem.getFileStores();
        for (OSFileStore fs : fsList) {
            long usable = fs.getUsableSpace();
            long total = fs.getTotalSpace();
            fsInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
                            + "%s (%s) [%s] %s of %s free (%.1f%%), %s of %s files free (%.1f%%) is %s "
                            + (fs.getLogicalVolume() != null && !fs.getLogicalVolume().isEmpty() ? "[%s]" : "%s")
                            + " and is mounted at %s",
                    fs.getName(), fs.getDescription().isEmpty() ? "file system" : fs.getDescription(), fs.getType(),
                    FormatUtil.formatBytes(usable), FormatUtil.formatBytes(fs.getTotalSpace()), 100d * usable / total,
                    FormatUtil.formatValue(fs.getFreeInodes(), ""), FormatUtil.formatValue(fs.getTotalInodes(), ""),
                    100d * fs.getFreeInodes() / fs.getTotalInodes(), fs.getVolume(), fs.getLogicalVolume(),
                    fs.getMount()));
        }
        return fsInfo;
    }

    private List<String> getNetworkInterfaces(List<NetworkIF> networkIFs) {
        List<String> getNetwork = new ArrayList<>();
        getNetwork.add("Network interfaces:&nbsp;&nbsp;");
        for (NetworkIF net : networkIFs) {
            getNetwork.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;Name: %s (%s)",
                    net.getName(), net.getDisplayName()));
            getNetwork.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MAC Address: %s",
                    net.getMacaddr()));
            getNetwork.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MTU: %s, Speed: %s",
                    net.getMTU(), FormatUtil.formatValue(net.getSpeed(), "bps")));
            getNetwork.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;IPv4: %s",
                    Arrays.toString(net.getIPv4addr())));
            getNetwork.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;IPv6: %s",
                    Arrays.toString(net.getIPv6addr())));
            boolean hasData = net.getBytesRecv() > 0 || net.getBytesSent() > 0 || net.getPacketsRecv() > 0
                    || net.getPacketsSent() > 0;
            getNetwork.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Traffic:"
                            + " received %s/%s%s; transmitted %s/%s%s",
                    hasData ? net.getPacketsRecv() + " packets" : "?",
                    hasData ? FormatUtil.formatBytes(net.getBytesRecv()) : "?",
                    hasData ? " (" + net.getInErrors() + " err)" : "",
                    hasData ? net.getPacketsSent() + " packets" : "?",
                    hasData ? FormatUtil.formatBytes(net.getBytesSent()) : "?",
                    hasData ? " (" + net.getOutErrors() + " err)" : ""));
        }
        return getNetwork;
    }

    private List<String> getNetworkParameters(NetworkParams networkParams) {
        List<String> networkParameterInfo = new ArrayList<>();
        networkParameterInfo.add("Network parameters:&nbsp;&nbsp;&nbsp;&nbsp;");
        networkParameterInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Host name: %s",
                networkParams.getHostName()));
        networkParameterInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Domain name: %s",
                networkParams.getDomainName()));
        networkParameterInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; DNS servers: %s",
                Arrays.toString(networkParams.getDnsServers())));
        networkParameterInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; IPv4 Gateway: %s",
                networkParams.getIpv4DefaultGateway()));
        networkParameterInfo.add(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; IPv6 Gateway: %s",
                networkParams.getIpv6DefaultGateway()));
        return networkParameterInfo;
    }

}

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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_s3_rate_limiter_cgroup_cpu_resize", "docker") {
    long getQpsPerCore = 11
    long putQpsPerCore = 13
    long getBytesPerCore = 1024 * 1024
    long putBytesPerCore = 2 * 1024 * 1024

    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 1
    options.beNum = 1
    options.msNum = 0
    options.recyclerNum = 0
    options.beConfigs += [
            "enable_s3_rate_limiter=true",
            "s3_rate_limiter_cpu_cores=0",
            "s3_get_qps_per_core=${getQpsPerCore}",
            "s3_put_qps_per_core=${putQpsPerCore}",
            "s3_get_qps_max=0",
            "s3_put_qps_max=0",
            "s3_get_bytes_per_second_per_core=${getBytesPerCore}",
            "s3_put_bytes_per_second_per_core=${putBytesPerCore}",
            "s3_get_bytes_per_second_max=0",
            "s3_put_bytes_per_second_max=0"
    ]

    docker(options) {
        // This case isolates the process-wide cgroup-to-limiter rebuild. Internal-vault
        // request mapping and throttling are covered by test_s3_rate_limiter.
        def be = cluster.getBeByIndex(1)
        assertNotNull(be)

        String beContainer = "doris-${cluster.name}-be-1"
        File beLog = new File(be.getLogFilePath())
        assertTrue(beLog.exists(), "BE log does not exist: ${beLog}")

        String cgroupV2Enabled = cmd("docker exec ${beContainer} sh -c " +
                "'if [ -f /sys/fs/cgroup/cgroup.controllers ]; then echo true; else echo false; fi'")
                .trim()
        assertEquals("true", cgroupV2Enabled, "this case requires a cgroup v2 Docker host")

        int onlineCpus = cmd("docker exec ${beContainer} getconf _NPROCESSORS_ONLN")
                .trim().toInteger()
        assertTrue(onlineCpus >= 2,
                "the Docker host needs at least 2 online CPUs, actual=${onlineCpus}")

        String cgroupMembership = cmd("docker exec ${beContainer} cat /proc/self/cgroup").trim()
        logger.info("BE container cgroup membership: ${cgroupMembership}")

        def readCpuQuotaCores = {
            String cpuMax = cmd("docker exec ${beContainer} cat /sys/fs/cgroup/cpu.max").trim()
            logger.info("BE container cpu.max: ${cpuMax}")
            def fields = cpuMax.split(/\s+/)
            assertEquals(2, fields.size(), "invalid cpu.max content: ${cpuMax}")
            assertFalse(fields[0] == "max", "cpu.max is unlimited: ${cpuMax}")

            long quota = fields[0].toLong()
            long period = fields[1].toLong()
            assertTrue(quota > 0 && period > 0, "invalid cpu.max content: ${cpuMax}")
            return (quota + period - 1).intdiv(period)
        }

        def expectedResetLogs = { int cores ->
            long getQps = getQpsPerCore * cores
            long putQps = putQpsPerCore * cores
            long getBytes = getBytesPerCore * cores
            long putBytes = putBytesPerCore * cores
            return [
                    "reset S3 get QPS rate limiter, qps=${getQps}, burst=${getQps}, " +
                            "count_limit=0, cores=${cores}",
                    "reset S3 put QPS rate limiter, qps=${putQps}, burst=${putQps}, " +
                            "count_limit=0, cores=${cores}",
                    "reset S3 get bytes rate limiter, bytes_per_second=${getBytes}, cores=${cores}",
                    "reset S3 put bytes rate limiter, bytes_per_second=${putBytes}, cores=${cores}"
            ]
        }

        def updateCpuQuotaAndWaitForLimiter = { int cores ->
            int logOffset = beLog.getText("UTF-8").length()
            cmd("docker update --cpus=${cores} ${beContainer}")
            assertEquals(cores as long, readCpuQuotaCores(),
                    "docker update did not apply a ${cores}-CPU quota")

            List<String> expectedLogs = expectedResetLogs(cores)
            awaitUntil(35, 1) {
                String currentLog = beLog.getText("UTF-8")
                String newLog = currentLog.substring(Math.min(logOffset, currentLog.length()))
                return expectedLogs.every { newLog.contains(it) }
            }
        }

        // Ensure the daemon's first 10-second refresh has initialized the manager before
        // changing the cgroup quota, so both updates exercise an in-place limiter rebuild.
        sleep(12000)
        updateCpuQuotaAndWaitForLimiter(1)
        updateCpuQuotaAndWaitForLimiter(2)
    }
}

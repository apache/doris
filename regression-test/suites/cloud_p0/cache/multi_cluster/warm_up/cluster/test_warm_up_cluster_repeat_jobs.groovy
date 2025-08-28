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
import groovy.json.JsonSlurper

suite('test_warm_up_cluster_repeat_jobs', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.cloudMode = true

    docker(options) {
        def clusterName1 = "cluster1"
        def clusterName2 = "cluster2"
        def clusterName3 = "cluster3"

        // Add two clusters
        cluster.addBackend(1, clusterName1)
        cluster.addBackend(1, clusterName2)
        cluster.addBackend(1, clusterName3)

        def jobId_ = sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """
        def jobId = jobId_[0][0]

        logger.info("JobID = {}", jobId)

        // For periodic jobs, it's not allowed to start a job
        // with same src cluster, dst cluster, and sync_mode
        try {
            sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                PROPERTIES (
                    "sync_mode" = "periodic",
                    "sync_interval_sec" = "10"
                )
            """
            assertTrue(false, "expected exception")
        } catch (java.sql.SQLException e) {
            assertTrue(e.getMessage().contains("already has a runnable job"), e.getMessage());
        }

        // It's allowed to start a job with same dst cluster
        sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName3}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """

        // It's allowed to start a ONCE job with same src dst cluster
        sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "once"
            )
        """

        // It's allowed to start a ONCE job with same src dst cluster
        sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
        """

        // It's allowed to create a job in the opposite direction
        sql """
            WARM UP CLUSTER ${clusterName1} WITH CLUSTER ${clusterName2}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """

        // after cancelling the old job, we can create another job with same attributes
        sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
        sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """

        // It's allowed to start a event_driven job alongside with a periodic job
        sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "event_driven",
                "sync_event" = "load"
            )
        """

        // For event driven jobs, it's not allowed to start a job
        // with same src cluster, dst cluster, and sync_mode
        try {
            sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            assertTrue(false, "expected exception")
        } catch (java.sql.SQLException e) {
            assertTrue(e.getMessage().contains("already has a runnable job"), e.getMessage());
        }

        // For event driven jobs, it's allowed to start a job with same dst cluster
        sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName3}
            PROPERTIES (
                "sync_mode" = "event_driven",
                "sync_event" = "load"
            )
        """
    }
}

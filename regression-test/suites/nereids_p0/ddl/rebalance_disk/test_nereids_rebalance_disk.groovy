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

suite("test_nereids_rebalance_disk") {
    //cloud-mode
    if (isCloudMode()) {
        def String error_in_cloud = "Unsupported"
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())

        test {
            sql """ admin rebalance disk; """
            exception "${error_in_cloud}"
        }
        test {
            sql """ admin rebalance disk ON ("127.0.0.1:9050"); """
            exception "${error_in_cloud}"
        }
        test {
            sql """ admin rebalance disk ON ("192.168.0.1:9050", "127.0.0.1:9050", "192.168.0.2:9050"); """
            exception "${error_in_cloud}"
        }

        test {
            sql """ admin cancel rebalance disk;  """
            exception "${error_in_cloud}"
        }
        test {
            sql """ admin cancel rebalance disk ON ("127.0.0.1:9050"); """
            exception "${error_in_cloud}"
        }
        test {
            sql """ admin cancel rebalance disk ON ("192.168.0.1:9050", "127.0.0.1:9050", "192.168.0.2:9050"); """
            exception "${error_in_cloud}"
        }
    } else {
        // can not use qt command since the output change based on cluster and backend ip
        checkNereidsExecute(""" admin rebalance disk; """)
        checkNereidsExecute(""" admin rebalance disk ON ("127.0.0.1:9050"); """)
        checkNereidsExecute(""" admin rebalance disk ON ("192.168.0.1:9050", "127.0.0.1:9050", "192.168.0.2:9050"); """)

        checkNereidsExecute(""" admin cancel rebalance disk; """)
        checkNereidsExecute(""" admin cancel rebalance disk ON ("127.0.0.1:9050"); """)
        checkNereidsExecute(""" admin cancel rebalance disk ON ("192.168.0.1:9050", "127.0.0.1:9050", "192.168.0.2:9050"); """)
    }

}
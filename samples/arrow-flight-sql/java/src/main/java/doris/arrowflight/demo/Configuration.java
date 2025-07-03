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

package doris.arrowflight.demo;

public class Configuration {
    public String sql = ""; // require
    public String ip = "127.0.0.1"; // require
    public String arrowFlightPort = "9090"; // require
    public String mysqlPort = "9030";
    public int retryTimes = 2; // The first execution is cold run
    public String user = "root";
    public String password = "";

    Configuration(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (i) {
                case 0 -> sql = args[i];
                case 1 -> ip = args[i];
                case 2 -> arrowFlightPort = args[i];
                case 3 -> mysqlPort = args[i];
                case 4 -> retryTimes = Integer.parseInt(args[i]);
                case 5 -> user = args[i];
                case 6 -> password = args[i];
            }
        }
    }
}

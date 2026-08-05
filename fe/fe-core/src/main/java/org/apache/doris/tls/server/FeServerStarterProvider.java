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

package org.apache.doris.tls.server;

import org.apache.doris.qe.ConnectScheduler;

// FE startup splits at the starter provider boundary. OSS and TLS-enabled builds
// return different protocol starters while DorisFE and QeService keep a stable
// startup flow.
public interface FeServerStarterProvider {
    ServerStarter createThriftServerStarter(int port);

    ServerStarter createMysqlServerStarter(int port, ConnectScheduler scheduler);

    ServerStarter createFlightServerStarter(int port);
}

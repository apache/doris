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

package org.apache.doris.system;

import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.thrift.HeartbeatServiceConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// This class is to manage the control flag in heartbeat message between FE and BE.
// The control flag is for FE to control some behaviors of BE.
// Now the flag is represented by 64-bit long type, each bit can be used to control
// one behavior. The first bit is used for set default rowset type to beta flag.
public class HeartbeatFlags {
    private static final Logger LOG = LogManager.getLogger(HeartbeatFlags.class);

    public static boolean isValidRowsetType(String rowsetType) {
        return "alpha".equalsIgnoreCase(rowsetType) || "beta".equalsIgnoreCase(rowsetType);
    }

    public long getHeartbeatFlags() {
        long heartbeatFlags = 0;
        // If user set default rowset type to ALPHA, then convert it to beta, because
        // alpha rowset will be removed
        if ("beta".equalsIgnoreCase(GlobalVariable.defaultRowsetType)
                || "alpha".equalsIgnoreCase(GlobalVariable.defaultRowsetType)) {
            heartbeatFlags |= HeartbeatServiceConstants.IS_SET_DEFAULT_ROWSET_TO_BETA_BIT;
        } else {
            throw new IllegalArgumentException("unknown DEFAULT_ROWSET_TYPE in global variable");
        }

        return heartbeatFlags;
    }
}

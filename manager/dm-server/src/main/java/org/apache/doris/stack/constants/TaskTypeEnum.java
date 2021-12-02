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

package org.apache.doris.stack.constants;

import org.apache.doris.manager.common.domain.ServiceRole;

/**
 * task type
 **/
public enum TaskTypeEnum {
    INSTALL_AGENT,
    INSTALL_FE,
    INSTALL_BE,
    DEPLOY_FE_CONFIG,
    DEPLOY_BE_CONFIG,
    START_FE,
    START_BE,
    STOP_FE,
    STOP_BE,
    JOIN_BE,
    INSTALL_BROKER,
    DEPLOY_BROKER_CONFIG,
    START_BROKER,
    STOP_BROKER;

    /**
     * agent side task
     */
    public boolean agentTask() {
        return this != INSTALL_AGENT && this != JOIN_BE;
    }

    /**
     * parse task role: FE BE BROKER
     */
    public String parseTaskRole() {
        if (this == INSTALL_FE
                || this == DEPLOY_FE_CONFIG
                || this == START_FE
                || this == STOP_FE) {
            return ServiceRole.FE.name();
        } else if (this == INSTALL_BE
                || this == DEPLOY_BE_CONFIG
                || this == START_BE
                || this == STOP_BE) {
            return ServiceRole.BE.name();
        } else if (this == INSTALL_BROKER
                || this == DEPLOY_BROKER_CONFIG
                || this == START_BROKER
                || this == STOP_BROKER) {
            return ServiceRole.BROKER.name();
        } else {
            return null;
        }
    }
}

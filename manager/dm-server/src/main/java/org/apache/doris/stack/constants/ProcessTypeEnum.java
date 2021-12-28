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

/**
 * process step type
 **/
public enum ProcessTypeEnum {
    INSTALL_AGENT(0, "install agent"),
    INSTALL_SERVICE(1, "install service"),
    DEPLOY_CONFIG(2, "deploy config"),
    START_SERVICE(3, "start service");

    private int code;
    private String desc;

    ProcessTypeEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static ProcessTypeEnum findByName(String name) {
        for (ProcessTypeEnum type : ProcessTypeEnum.values()) {
            if (type.name().equals(name)) {
                return type;
            }
        }
        return null;
    }

    public static ProcessTypeEnum findByCode(int code) {
        for (ProcessTypeEnum type : ProcessTypeEnum.values()) {
            if (type.code == code) {
                return type;
            }
        }
        return null;
    }

    public static ProcessTypeEnum findParent(ProcessTypeEnum processType) {
        if (processType != null) {
            int parentCode = processType.getCode() - 1;
            return findByCode(parentCode);
        } else {
            return processType;
        }
    }

    public boolean endProcess() {
        return this == START_SERVICE;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}

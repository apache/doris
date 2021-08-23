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

package org.apache.doris.stack.model.request.space;

import lombok.Data;
import org.springframework.util.StringUtils;

@Data
public class ClusterCreateReq {
    private String address;

    private int httpPort;

    private int queryPort;

    private String user;

    private String passwd;

    /**
     * The type of the engine (currently only Doris / MySQL / DAE is supported).
     * If it is empty, it means the default Doris
     */
    private ClusterType type;

    /**
     * check empty field
     *
     * @return boolean
     */
    public boolean hasEmptyField() {
        if (StringUtils.isEmpty(address) || StringUtils.isEmpty(user)
                || passwd == null || queryPort <= 0) {
            return true;
        }
        return false;
    }
}

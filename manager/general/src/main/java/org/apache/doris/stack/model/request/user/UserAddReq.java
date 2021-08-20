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

package org.apache.doris.stack.model.request.user;

import lombok.Data;
import org.springframework.util.StringUtils;

/**
 * @Description：Request to add a new user
 */
@Data
public class UserAddReq {
    private String name;

    private String email;

    private String password;

    /**
     * check empty field
     * @param ldap ldap
     * @return boolean
     */
    public boolean hasEmptyField(boolean ldap) {
        if (ldap) {
            if (StringUtils.isEmpty(email)) {
                return true;
            } else {
                return false;
            }
        } else {
            if (StringUtils.isEmpty(name) || StringUtils.isEmpty(email)) {
                return true;
            } else {
                return false;
            }
        }
    }
}

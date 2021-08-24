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

package org.apache.doris.stack.model.activity;

/**
 * @Description：All user activity topic definitions
 */
public class Topic {

    // First startup record after installation
    public static final String INSTALL = "install";

    // Activity record of user related operations
    // Record of user's first login
    public static final String USE_JOINED =  "user-joined";

    // Record of the user's latest login for the first time
    public static final String USE_LOGIN =  "user-login";

}

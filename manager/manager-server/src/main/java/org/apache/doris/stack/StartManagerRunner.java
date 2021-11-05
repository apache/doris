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

package org.apache.doris.stack;

import org.apache.doris.stack.service.config.SettingService;
import org.apache.doris.stack.service.impl.ServerProcessImpl;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.service.user.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(value = 1)
@Slf4j
public class StartManagerRunner implements CommandLineRunner {
    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private SettingService settingService;

    @Autowired
    private UserService userService;

    @Autowired
    private ServerProcessImpl serverProcess;

    @Override
    public void run(String... args) {
        try {
            authenticationService.initSuperUser();
            settingService.initConfig();
            userService.initDefaultUserGroup();
        } catch (Exception e) {
            log.error("start manager failed, error detail:{}", e);
        }
    }
}

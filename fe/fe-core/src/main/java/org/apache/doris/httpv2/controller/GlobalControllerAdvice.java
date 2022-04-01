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
package org.apache.doris.httpv2.controller;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.InitBinder;
@Order(10000)
@ControllerAdvice
public class GlobalControllerAdvice {
    private static final Logger LOG = LogManager.getLogger(GlobalControllerAdvice.class);
    @InitBinder
    public void setDisallowedFields(WebDataBinder dataBinder){
        String[] str = new String[] { "class.*", "Class.*", "*.class.*", "*.Class.*"};
        dataBinder.setDisallowedFields(str);
        LOG.info("spring setDisallowedFields invoke...");
    }
}

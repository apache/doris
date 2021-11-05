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

import org.apache.doris.stack.constant.PropertyDefine;
import org.apache.doris.stack.util.PropertyUtil;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Map;

@SpringBootApplication
@EnableScheduling
@EnableTransactionManagement(proxyTargetClass = true)
@EnableConfigurationProperties
@EnableSwagger2
@ServletComponentScan
public class DorisManager extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(DorisManager.class);
    }

    public static void main(String[] args) {
        DorisManager studio = new DorisManager();
        SpringApplicationBuilder builder = getBuilder();
        studio.configure(builder);
        builder.run(args);
    }

    public static SpringApplicationBuilder getBuilder() {
        Map<String, Object> properties = PropertyUtil.getProperties();

        // Configure the service name. The default is manager
        properties.put(PropertyDefine.DEPLOY_TYPE_PROPERTY, PropertyDefine.DEPLOY_TYPE_MANAGER);

        // Static resource allocation
        properties.put("server.servlet.context-path", "/");
        properties.put("spring.resources.static-locations", "classpath:/web-resource");

        return new SpringApplicationBuilder().properties(properties);
    }
}

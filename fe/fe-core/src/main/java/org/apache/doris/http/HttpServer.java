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

package org.apache.doris.http;

import org.apache.doris.http.config.SpringLog4j2Config;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableConfigurationProperties
@ServletComponentScan
public class HttpServer extends SpringBootServletInitializer {

    private int port;

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(HttpServer.class);
    }

    public void start(String dorisHome) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("server.port", port);
        properties.put("server.servlet.context-path", "/");
        properties.put("spring.resources.static-locations", "classpath:/static");
        properties.put("spring.http.encoding.charset", "UTF-8");
        properties.put("spring.http.encoding.enabled", true);
        properties.put("spring.http.encoding.force", true);
        properties.put("logging.config", dorisHome + "/conf/" + SpringLog4j2Config.SPRING_LOG_XML_FILE);
        new SpringApplicationBuilder()
                .sources(HttpServer.class)
                .properties(properties)
                .run(new String[]{});
    }
}

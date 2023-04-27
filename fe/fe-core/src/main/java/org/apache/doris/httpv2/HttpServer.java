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

package org.apache.doris.httpv2;

import org.apache.doris.DorisFE;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.httpv2.config.SpringLog4j2Config;
import org.apache.doris.service.FrontendOptions;

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
    private int httpsPort;
    private int acceptors;
    private int selectors;
    private int maxHttpPostSize;
    private int workers;

    private String keyStorePath;
    private String keyStorePassword;
    private String keyStoreType;
    private String keyStoreAlias;
    private boolean enableHttps;

    private int minThreads;
    private int maxThreads;
    private int maxHttpHeaderSize;

    public int getMaxHttpHeaderSize() {
        return maxHttpHeaderSize;
    }

    public void setMaxHttpHeaderSize(int maxHttpHeaderSize) {
        this.maxHttpHeaderSize = maxHttpHeaderSize;
    }

    public int getMinThreads() {
        return minThreads;
    }

    public void setMinThreads(int minThreads) {
        this.minThreads = minThreads;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public void setAcceptors(int acceptors) {
        this.acceptors = acceptors;
    }

    public void setSelectors(int selectors) {
        this.selectors = selectors;
    }

    public void setMaxHttpPostSize(int maxHttpPostSize) {
        this.maxHttpPostSize = maxHttpPostSize;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setHttpsPort(int httpsPort) {
        this.httpsPort = httpsPort;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public void setKeyStoreAlias(String keyStoreAlias) {
        this.keyStoreAlias = keyStoreAlias;
    }

    public void setEnableHttps(boolean enableHttps) {
        this.enableHttps = enableHttps;
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(HttpServer.class);
    }

    public void start() {
        Map<String, Object> properties = new HashMap<>();
        if (enableHttps) {
            properties.put("server.http.port", port);
            properties.put("server.port", httpsPort);
            // ssl config
            properties.put("server.ssl.key-store", keyStorePath);
            properties.put("server.ssl.key-store-password", keyStorePassword);
            properties.put("server.ssl.key-store-type", keyStoreType);
            properties.put("server.ssl.keyalias", keyStoreAlias);
            properties.put("server.ssl.enabled", enableHttps);
        } else {
            properties.put("server.port", port);
            properties.put("server.ssl.enabled", enableHttps);
        }
        if (FrontendOptions.isBindIPV6()) {
            properties.put("server.address", "::0");
        } else {
            properties.put("server.address", "0.0.0.0");
        }
        properties.put("server.servlet.context-path", "/");
        properties.put("spring.resources.static-locations", "classpath:/static");
        properties.put("spring.http.encoding.charset", "UTF-8");
        properties.put("spring.http.encoding.enabled", true);
        properties.put("spring.http.encoding.force", true);
        // enable jetty config
        properties.put("server.jetty.acceptors", this.acceptors);
        properties.put("server.jetty.max-http-post-size", this.maxHttpPostSize);
        properties.put("server.jetty.selectors", this.selectors);
        properties.put("server.jetty.threadPool.maxThreads", this.maxThreads);
        properties.put("server.jetty.threadPool.minThreads", this.minThreads);
        properties.put("server.max-http-header-size", this.maxHttpHeaderSize);
        // Worker thread pool is not set by default, set according to your needs
        if (this.workers > 0) {
            properties.put("server.jetty.workers", this.workers);
        }
        // This is to disable the spring-boot-devtools restart feature.
        // To avoid some unexpected behavior.
        System.setProperty("spring.devtools.restart.enabled", "false");
        // Value of `DORIS_HOME_DIR` is null in unit test.
        if (DorisFE.DORIS_HOME_DIR != null) {
            System.setProperty("spring.http.multipart.location", DorisFE.DORIS_HOME_DIR);
        }
        System.setProperty("spring.banner.image.location", "doris-logo.png");
        if (FeConstants.runningUnitTest) {
            // this is currently only used for unit test
            properties.put("logging.config", getClass().getClassLoader().getResource("log4j2.xml").getPath());
        } else {
            properties.put("logging.config", Config.custom_config_dir + "/" + SpringLog4j2Config.SPRING_LOG_XML_FILE);
        }
        new SpringApplicationBuilder()
                .sources(HttpServer.class)
                .properties(properties)
                .run(new String[]{});
    }
}

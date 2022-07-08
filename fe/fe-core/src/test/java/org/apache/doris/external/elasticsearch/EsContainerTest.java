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

package org.apache.doris.external.elasticsearch;

import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;

@Testcontainers
public class EsContainerTest extends TestWithFeService {

    private DockerComposeContainer<?> compose;

    @Override
    protected void runBeforeAll() throws Exception {
        // upgrade jna fix java.lang.UnsatisfiedLinkError
        // https://hub.docker.com/r/arm64v8/elasticsearch/tags?page=1
        System.out.println("begin start es7");
        ClassPathResource classPathResource = new ClassPathResource("docker/elasticsearch.yaml");
        compose = new DockerComposeContainer<>(classPathResource.getFile()).withPull(true).withTailChildContainers(true)
                .withLogConsumer("es7", outputFrame -> System.out.println(outputFrame.getUtf8String()));
        compose.start();
        compose.waitingFor("es7", Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(120)));
    }

    @Test
    public void testEs() throws IOException {
        String res = HttpUtils.doGet("http://127.0.0.1:19200", null);
        System.out.println(res);
    }
}

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

package org.apache.doris.utframe;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ContainerExtension implements BeforeAllCallback, AfterAllCallback {


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        startContainer(context);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

    }

    private void startContainer(ExtensionContext context) {
        final Class<?> clazz = context.getRequiredTestClass();
        final EnableContainer annotation = clazz.getAnnotation(EnableContainer.class);
        final List<File> files = Stream.of(annotation.composeFiles())
                .map(compose -> EnableContainer.class.getClassLoader().getResource(compose))
                .filter(Objects::nonNull)
                .map(URL::getPath)
                .map(File::new)
                .collect(Collectors.toList());
        DockerComposeContainer<?> compose = new DockerComposeContainer<>(files).withPull(true);
        compose.start();
    }
}

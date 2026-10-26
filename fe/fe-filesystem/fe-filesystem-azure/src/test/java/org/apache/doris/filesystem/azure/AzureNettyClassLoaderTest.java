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

package org.apache.doris.filesystem.azure;

import org.apache.doris.extension.loader.ChildFirstClassLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

class AzureNettyClassLoaderTest {
    private static final String IO_HANDLER_FACTORY = "io.netty.channel.IoHandlerFactory";
    private static final String URING_HANDLER = "io.netty.channel.uring.IoUringIoHandler";

    private ChildFirstClassLoader pluginLoader() throws Exception {
        // Surefire also exposes provided/test dependencies, which can hide a missing plugin runtime dependency.
        String classpath = Files.readString(Path.of(System.getProperty("plugin.runtime.classpath"))).trim();
        List<URL> urls = new ArrayList<>();
        for (String entry : classpath.split(Pattern.quote(File.pathSeparator))) {
            urls.add(Path.of(entry).toUri().toURL());
        }
        return new ChildFirstClassLoader(urls.toArray(new URL[0]), getClass().getClassLoader(),
                ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES);
    }

    @Test
    void ioUringTypesStayInThePluginLoader() throws Exception {
        try (ChildFirstClassLoader loader = pluginLoader()) {
            Class<?> factory = Class.forName(IO_HANDLER_FACTORY, false, loader);
            Class<?> handler = Class.forName(URING_HANDLER, false, loader);
            Assertions.assertSame(loader, factory.getClassLoader());
            Assertions.assertSame(loader, handler.getClassLoader());
            Assertions.assertSame(loader, Class.forName("io.netty.channel.MultiThreadIoEventLoopGroup", false, loader)
                    .getClassLoader());
            // Resolving the signature must use the plugin's interface even when the host also carries Netty.
            Assertions.assertSame(factory, handler.getMethod("newFactory").getReturnType());
        }
    }

    @Test
    void nativeLibrariesAreAvailableWithoutTheHost() throws Exception {
        try (ChildFirstClassLoader loader = pluginLoader()) {
            for (String architecture : List.of("x86_64", "aarch_64")) {
                String name = "META-INF/native/libnetty_transport_native_io_uring42_" + architecture + ".so";
                URL resource = loader.findResource(name);
                Assertions.assertNotNull(resource, "Missing plugin native library: " + name);
                Assertions.assertEquals(List.of(resource), Collections.list(loader.getResources(name)));
            }
        }
    }

    @Test
    void nativeSelectionWorksAfterHostInitialization() throws Exception {
        // Initialize the host first to cover native libraries already loaded by a different ClassLoader.
        Class.forName("io.netty.channel.uring.IoUring").getMethod("isAvailable").invoke(null);
        try (ChildFirstClassLoader loader = pluginLoader()) {
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(loader);
            try {
                Class<?> loops = Class.forName("reactor.netty.resources.LoopResources", true, loader);
                Object resources = loops.getMethod("create", String.class, int.class, boolean.class)
                        .invoke(null, "azure-plugin-test", 1, true);
                try {
                    Object group = loops.getMethod("onClient", boolean.class).invoke(resources, true);
                    Assertions.assertSame(loader, group.getClass().getClassLoader());
                    Assertions.assertEquals("executed", ((ExecutorService) group)
                            .submit(() -> "executed").get(5, TimeUnit.SECONDS));
                } finally {
                    try {
                        Object completion = loops.getMethod("disposeLater", Duration.class, Duration.class)
                                .invoke(resources, Duration.ZERO, Duration.ofSeconds(5));
                        Class.forName("reactor.core.publisher.Mono", true, loader)
                                .getMethod("block", Duration.class).invoke(completion, Duration.ofSeconds(10));
                    } finally {
                        // The disposal timeout initializes plugin-local schedulers that otherwise retain the loader.
                        Class.forName("reactor.core.scheduler.Schedulers", true, loader)
                                .getMethod("shutdownNow").invoke(null);
                    }
                }
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }
    }
}

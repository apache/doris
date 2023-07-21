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

package org.apache.doris.common.classloader;

import org.apache.doris.common.jni.JniScanner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
import java.util.ServiceLoader;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public class ScannerLoader {
    private static final Logger LOG = Logger.getLogger(ScannerLoader.class);

    public static void load() {
        LOG.info("--- scanner load class");
        String basePath = System.getenv("DORIS_HOME");
        File library = new File(basePath, "/lib/java_extensions/");
        listFiles(library).stream().filter(File::isDirectory).forEach(e -> {
            JniScannerClassLoader classLoader = createScannerClassLoader(e.getName(), buildClassPath(e));
            LOG.info("scanner Classpath for plugin: " + e.getName());
            for (URL url : classLoader.getURLs()) {
                LOG.info("scanner " + url.getPath());
            }
            try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
                ServiceLoader<JniScanner> serviceLoader = ServiceLoader.load(JniScanner.class, classLoader);
                List<JniScanner> scanners = ImmutableList.copyOf(serviceLoader);
                if (scanners.isEmpty()) {
                    LOG.warn("No service providers of type " + JniScanner.class.getName()
                            + " in the classpath: " + Lists.asList(null, classLoader.getURLs()));
                }
                for (JniScanner scanner : scanners) {
                    LOG.info("Loaded " + scanner.getClass().getName());
                }
            }
        });
    }

    private static JniScannerClassLoader createScannerClassLoader(String name, List<URL> classPaths) {
        return new JniScannerClassLoader(name, classPaths);
    }

    private static List<URL> buildClassPath(File path) {
        return listFiles(path).stream()
                .map(ScannerLoader::fileToUrl)
                .collect(Collectors.toList());
    }

    private static URL fileToUrl(File file) {
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<File> listFiles(File library) {
        LOG.info("---scanner dir: " + library.toPath());
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(library.toPath())) {
            return Streams.stream(directoryStream)
                    .map(Path::toFile)
                    .sorted()
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public static void loadJarClass(String path) throws IOException {
        Enumeration<JarEntry> entryEnumeration;
        try (JarFile jar = new JarFile(path)) {
            entryEnumeration = jar.entries();
            while (entryEnumeration.hasMoreElements()) {
                JarEntry entry = entryEnumeration.nextElement();
                String clazzName = entry.getName();
                if (clazzName.endsWith(".class")) {
                    clazzName = clazzName.substring(0, clazzName.length() - 6);
                    clazzName = clazzName.replace("/", ".");
                    Class.forName(clazzName);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}

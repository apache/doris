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

import org.apache.doris.common.jni.utils.ExpiringMap;

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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * BE will load scanners by JNI call, and then the JniConnector on BE will get scanner class by getLoadedClass.
 */
public class ScannerLoader {
    public static final Logger LOG = Logger.getLogger(ScannerLoader.class);
    private static final Map<String, Class<?>> loadedClasses = new HashMap<>();
    private static final ExpiringMap<String, ClassLoader> udfLoadedClasses = new ExpiringMap<String, ClassLoader>();
    private static final String CLASS_SUFFIX = ".class";
    private static final String LOAD_PACKAGE = "org.apache.doris";

    /**
     * Load all classes from $DORIS_HOME/lib/java_extensions/*
     */
    public void loadAllScannerJars() {
        String basePath = System.getenv("DORIS_HOME");
        File library = new File(basePath, "/lib/java_extensions/");
        // TODO: add thread pool to load each scanner
        listFiles(library).stream().filter(File::isDirectory).forEach(sd -> {
            JniScannerClassLoader classLoader = new JniScannerClassLoader(sd.getName(), buildClassPath(sd),
                        this.getClass().getClassLoader());
            try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
                loadJarClassFromDir(sd, classLoader);
            }
        });
    }

    public static ClassLoader getUdfClassLoader(String functionSignature) {
        return udfLoadedClasses.get(functionSignature);
    }

    public static synchronized void cacheClassLoader(String functionSignature, ClassLoader classLoader,
            long expirationTime) {
        LOG.info("cacheClassLoader for: " + functionSignature);
        udfLoadedClasses.put(functionSignature, classLoader, expirationTime * 60 * 1000L);
    }

    public synchronized void cleanUdfClassLoader(String functionSignature) {
        LOG.info("cleanUdfClassLoader for: " + functionSignature);
        udfLoadedClasses.remove(functionSignature);
    }

    /**
     * Get loaded class for JNI scanners
     * @param className JNI scanner class name
     * @return scanner class object
     * @throws ClassNotFoundException JNI scanner class not found
     */
    public Class<?> getLoadedClass(String className) throws ClassNotFoundException {
        String loadedClassName = getPackagePathName(className);
        if (loadedClasses.containsKey(loadedClassName)) {
            return loadedClasses.get(loadedClassName);
        } else {
            throw new ClassNotFoundException("JNI scanner has not been loaded or no such class: " + className);
        }
    }

    private static List<URL> buildClassPath(File path) {
        return listFiles(path).stream()
                .map(ScannerLoader::classFileUrl)
                .collect(Collectors.toList());
    }

    private static URL classFileUrl(File file) {
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<File> listFiles(File library) {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(library.toPath())) {
            return Streams.stream(directoryStream)
                    .map(Path::toFile)
                    .sorted()
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void loadJarClassFromDir(File dir, JniScannerClassLoader classLoader) {
        listFiles(dir).forEach(file -> {
            Enumeration<JarEntry> entryEnumeration;
            List<String> loadClassNames = new ArrayList<>();
            try {
                try (JarFile jar = new JarFile(file)) {
                    entryEnumeration = jar.entries();
                    while (entryEnumeration.hasMoreElements()) {
                        JarEntry entry = entryEnumeration.nextElement();
                        String className = entry.getName();
                        if (!className.endsWith(CLASS_SUFFIX)) {
                            continue;
                        }
                        className = className.substring(0, className.length() - CLASS_SUFFIX.length());
                        String packageClassName = getPackagePathName(className);
                        if (needToLoad(packageClassName)) {
                            loadClassNames.add(packageClassName);
                        }
                    }
                }
                for (String className : loadClassNames) {
                    loadedClasses.putIfAbsent(className, classLoader.loadClass(className));
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });
    }

    private static String getPackagePathName(String className) {
        return className.replace("/", ".");
    }

    private static boolean needToLoad(String className) {
        return className.contains(LOAD_PACKAGE) && !className.contains("$");
    }
}

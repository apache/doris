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

import org.apache.doris.common.jni.utils.UdfClassCache;

import com.google.common.collect.Streams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * BE will load scanners by JNI call, and then the JniConnector on BE will get scanner class by getLoadedClass.
 */
public class ScannerLoader {
    static {
        // Explicitly initialize log4j2 to ensure logging works in JNI environment
        try {
            // Set logPath system property if not already set or normalize it
            String logPath = System.getProperty("logPath");
            if (logPath == null || logPath.isEmpty()) {
                String dorisHome = System.getenv("DORIS_HOME");
                if (dorisHome != null) {
                    logPath = dorisHome + "/log/jni.log";
                }
            }
            // Normalize path to remove double slashes
            if (logPath != null) {
                logPath = logPath.replaceAll("//+", "/");
                System.setProperty("logPath", logPath);
            }

            // Point log4j2 to our configuration file in classpath
            System.setProperty("log4j2.configurationFile", "log4j2.properties");

            // Disable log4j2's shutdown hook to prevent premature shutdown in JNI environment
            System.setProperty("log4j.shutdownHookEnabled", "false");

            // Force log4j2 to reconfigure with our settings
            org.apache.logging.log4j.core.LoggerContext ctx =
                    (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
            ctx.reconfigure();

            // Log initialization success
            Logger logger = LogManager.getLogger(ScannerLoader.class);
            logger.info("Log4j2 initialized successfully. Log file: {}", logPath);

            // Test SLF4J bridge
            org.slf4j.Logger slf4jLogger = org.slf4j.LoggerFactory.getLogger(ScannerLoader.class);
            slf4jLogger.info("SLF4J bridge to log4j2 verified successfully");
        } catch (Exception e) {
            System.err.println("Failed to initialize log4j2: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static final Logger LOG = LogManager.getLogger(ScannerLoader.class);
    private static final Map<String, Class<?>> loadedClasses = new HashMap<>();
    // Cache of UDF class metadata (including the URLClassLoader used to load the UDF).
    // Entries are inserted on first use and only ever removed by an explicit
    // cleanUdfClassLoader() call (triggered by FE on DROP FUNCTION). There is intentionally
    // no time-based eviction: that previously caused two issues —
    //   1) closing a URLClassLoader while another thread was still loading classes from it
    //      led to NoClassDefFoundError;
    //   2) rebuilding a fresh URLClassLoader on every eviction produced multiple coexisting
    //      ClassLoaders for the same UDF, which broke lazy class resolution and reflective
    //      lookups inside user UDF code.
    // NOTE: a cache miss in BaseExecutor.getClassCache() is NOT only reachable after
    // cleanUdfClassLoader() — concurrent first-time loads of the same signature can also
    // both observe a miss. cacheClassLoader() must therefore insert atomically via
    // putIfAbsent and must never close a cache that was already published to the map,
    // because another executor may already be holding it.
    private static final Map<String, UdfClassCache> udfLoadedClasses = new ConcurrentHashMap<>();
    private static final String CLASS_SUFFIX = ".class";
    private static final String LOAD_PACKAGE = "org.apache.doris";

    /**
     * Load all classes from $DORIS_HOME/lib/java_extensions/*
     */
    public void loadAllScannerJars() {
        LOG.info("Starting to load scanner JARs from $DORIS_HOME/lib/java_extensions/");
        String basePath = System.getenv("DORIS_HOME");
        File library = new File(basePath, "/lib/java_extensions/");
        LOG.info("Scanner library path: {}", library.getAbsolutePath());
        // TODO: add thread pool to load each scanner
        listFiles(library).stream().filter(File::isDirectory).forEach(sd -> {
            LOG.info("Loading scanner from directory: {}", sd.getName());
            JniScannerClassLoader classLoader = new JniScannerClassLoader(sd.getName(), buildClassPath(sd),
                        this.getClass().getClassLoader());
            try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
                loadJarClassFromDir(sd, classLoader);
            }
        });
        LOG.info("Finished loading scanner JARs");
    }

    public static UdfClassCache getUdfClassLoader(String functionSignature) {
        return udfLoadedClasses.get(functionSignature);
    }

    /**
     * Cache the UDF class metadata for the given function signature.
     *
     * <p>Insertion is atomic via {@link Map#putIfAbsent}: if another executor thread has
     * already published a cache entry for {@code functionSignature}, the {@code classCache}
     * argument is treated as a redundant build and closed here (it has not yet been handed
     * to any executor, so closing its URLClassLoader is safe). The already-published entry
     * is returned to the caller so the current executor can switch to it.</p>
     *
     * <p>The {@code expirationTime} parameter is kept for backward compatibility with the
     * existing call sites and DDL property {@code expiration_time}, but is no longer used:
     * cached entries are not evicted by time. Removal happens only via
     * {@link #cleanUdfClassLoader(String)} on DROP FUNCTION.</p>
     *
     * @return the {@link UdfClassCache} actually held in the map after this call —
     *         either {@code classCache} (we won the race) or the pre-existing entry
     *         (another thread won; {@code classCache} has been closed and must not be used).
     */
    public static UdfClassCache cacheClassLoader(String functionSignature, UdfClassCache classCache,
            long expirationTime) {
        LOG.info("Cache UDF for: " + functionSignature);
        UdfClassCache existing = udfLoadedClasses.putIfAbsent(functionSignature, classCache);
        if (existing == null) {
            return classCache;
        }
        // Lost the race against a concurrent first-time load. The cache we just built has
        // never been exposed to any executor, so closing its URLClassLoader here cannot
        // affect anyone. Do NOT touch `existing` — another executor may already be using it.
        try {
            classCache.close();
        } catch (Exception e) {
            LOG.warn("Failed to close redundant UdfClassCache for " + functionSignature, e);
        }
        return existing;
    }

    public void cleanUdfClassLoader(String functionSignature) {
        LOG.info("cleanUdfClassLoader for: " + functionSignature);
        UdfClassCache removed = udfLoadedClasses.remove(functionSignature);
        if (removed != null) {
            // Immediately close the URLClassLoader. NOTE: any in-flight query still holding a
            // reference to this cache (e.g. via JNIContext.executor) will fail with
            // NoClassDefFoundError on lazy class resolution after this point. This is the
            // accepted semantic of DROP FUNCTION: the function is gone, queries against it
            // are expected to fail.
            try {
                removed.close();
            } catch (Exception e) {
                LOG.warn("Failed to close UdfClassCache for " + functionSignature, e);
            }
        }
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

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
import org.apache.doris.common.jni.utils.Log4jOutputStream;
import org.apache.doris.common.jni.utils.UdfClassCache;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * ScannerLoader loads all JNI scanner classes from:
 * $DORIS_HOME/lib/java_extensions/*
 * <p>
 * Class loading strategy:
 * - Scanner jars are loaded first
 * - Hadoop dependency jars are loaded as fallback
 * - System ClassLoader is used last
 * <p>
 * No extra ClassLoader is introduced; a single child-first URLClassLoader
 * is used per scanner.
 */
public class ScannerLoader {


    public static final Logger LOG = Logger.getLogger(ScannerLoader.class);


    private static final Map<String, Class<?>> loadedClasses = new HashMap<>();
    private static final ExpiringMap<String, UdfClassCache> udfLoadedClasses = new ExpiringMap<>();
    private static final String CLASS_SUFFIX = ".class";
    private static final String LOAD_PACKAGE = "org.apache.doris";


    /**
     * Load all scanner jars from $DORIS_HOME/lib/java_extensions/*
     */
    public void loadAllScannerJars() {
        redirectStdStreamsToLog4j();
        String basePath = System.getenv("DORIS_HOME");
        File scannerRoot = new File(basePath, "/lib/java_extensions/");
        File hadoopLib = new File(basePath, "/lib/hadoop_deps/lib/");


        listFiles(scannerRoot).stream()
                .filter(File::isDirectory)
                .forEach(scannerDir -> {
                    // Build classpath: scanner jars first, hadoop deps second
                    List<URL> classPath = buildScannerAndHadoopClassPath(scannerDir, hadoopLib);


                    JniScannerClassLoader classLoader = new JniScannerClassLoader(
                            scannerDir.getName(),
                            classPath.toArray(new URL[0]),
                            this.getClass().getClassLoader());


                    try (ThreadClassLoaderContext ignored =
                                 new ThreadClassLoaderContext(classLoader)) {
                        loadJarClassFromDir(scannerDir, classLoader);
                    }
                });
    }

    /**
     * Build a combined classpath where:
     * 1. Scanner jars come first (higher priority)
     * 2. Hadoop dependency jars come second (fallback)
     */
    private static List<URL> buildScannerAndHadoopClassPath(File scannerDir, File hadoopLib) {
        List<URL> urls = new ArrayList<>();
        // Scanner jars (highest priority)
        urls.addAll(listFiles(scannerDir).stream()
                .filter(f -> f.getName().endsWith(".jar"))
                .map(ScannerLoader::classFileUrl)
                .collect(Collectors.toList()));
        // Hadoop dependency jars (fallback)
        urls.addAll(listFiles(hadoopLib).stream()
                .filter(f -> f.getName().endsWith(".jar"))
                .map(ScannerLoader::classFileUrl)
                .collect(Collectors.toList()));
        return urls;
    }


    private void redirectStdStreamsToLog4j() {
        Logger outLogger = Logger.getLogger("stdout");
        System.setOut(new PrintStream(new Log4jOutputStream(outLogger, Level.INFO)));
        Logger errLogger = Logger.getLogger("stderr");
        System.setErr(new PrintStream(new Log4jOutputStream(errLogger, Level.ERROR)));
    }

    public static UdfClassCache getUdfClassLoader(String functionSignature) {
        return udfLoadedClasses.get(functionSignature);
    }

    public static synchronized void cacheClassLoader(String functionSignature, UdfClassCache classCache,
                                                     long expirationTime) {
        LOG.info("Cache UDF for: " + functionSignature);
        udfLoadedClasses.put(functionSignature, classCache, expirationTime * 60 * 1000L);
    }

    public synchronized void cleanUdfClassLoader(String functionSignature) {
        LOG.info("cleanUdfClassLoader for: " + functionSignature);
        udfLoadedClasses.remove(functionSignature);
    }

    /**
     * Get loaded class for JNI scanners
     *
     * @param className JNI scanner class name
     * @return scanner class object
     * @throws ClassNotFoundException JNI scanner class not found
     */
    public Class<?> getLoadedClass(String className) throws ClassNotFoundException {
        String fullName = className.replace("/", ".");
        Class<?> clazz = loadedClasses.get(fullName);
        if (clazz == null) {
            throw new ClassNotFoundException("JNI scanner class not found: " + className);
        }
        return clazz;
    }

    /**
     * Load all eligible classes from scanner jars in the given directory.
     */
    public static void loadJarClassFromDir(File dir, ClassLoader classLoader) {
        listFiles(dir).stream()
                .filter(f -> f.getName().endsWith(".jar"))
                .forEach(jarFile -> {
                    try (JarFile jar = new JarFile(jarFile)) {
                        Enumeration<JarEntry> entries = jar.entries();
                        while (entries.hasMoreElements()) {
                            JarEntry entry = entries.nextElement();
                            String name = entry.getName();
                            if (!name.endsWith(CLASS_SUFFIX)) {
                                continue;
                            }


                            String className = name
                                    .substring(0, name.length() - CLASS_SUFFIX.length())
                                    .replace("/", ".");


                            if (needToLoad(className)) {
                                loadedClasses.putIfAbsent(
                                        className,
                                        classLoader.loadClass(className));
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static boolean needToLoad(String className) {
        return className.contains(LOAD_PACKAGE) && !className.contains("$");
    }

    public static List<File> listFiles(File dir) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath())) {
            List<File> files = new ArrayList<>();
            for (Path path : stream) {
                files.add(path.toFile());
            }
            files.sort(Comparator.comparing(File::getName));
            return files;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static URL classFileUrl(File file) {
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }
}

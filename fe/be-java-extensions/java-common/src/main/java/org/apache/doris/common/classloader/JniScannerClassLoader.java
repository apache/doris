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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class JniScannerClassLoader extends URLClassLoader {

    private static final List<String> ICEBERG_FILE_IO_FACTORIES = List.of(
            "org.apache.iceberg.CatalogUtil", "org.apache.iceberg.io.ResolvingFileIO");

    private final String scannerName;

    public JniScannerClassLoader(String scannerName, List<URL> urls, ClassLoader parent) {
        super(urls.toArray(new URL[0]), parent);
        this.scannerName = scannerName;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!isScannerLocalFactory(name)) {
            return super.loadClass(name, resolve);
        }
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                try {
                    loaded = findClass(name);
                } catch (ClassNotFoundException ignored) {
                    return super.loadClass(name, resolve);
                }
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    private boolean isScannerLocalFactory(String name) {
        if (!"iceberg-metadata-scanner".equals(scannerName)) {
            return false;
        }
        // CatalogUtil uses its defining loader, not TCCL, to instantiate FileIO implementations.
        // A cold ResolvingFileIO from preload therefore cannot see this scanner's ADLSFileIO.
        // Keep those two factories local, including their nested classes, while FileIO interfaces,
        // metadata tasks, Hadoop/SDK dependencies and all other scanners stay parent-first.
        for (String factory : ICEBERG_FILE_IO_FACTORIES) {
            if (name.equals(factory) || name.startsWith(factory + "$")) {
                return true;
            }
        }
        return false;
    }

    public synchronized void addURLIfAbsent(URL url) {
        for (URL existingUrl : getURLs()) {
            if (existingUrl.equals(url)) {
                return;
            }
        }
        super.addURL(url);
    }

    @Override
    public String toString() {
        return "JniScannerClassLoader{"
                + "scannerName='" + scannerName
                + '}';
    }
}

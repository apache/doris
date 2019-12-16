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

package org.apache.doris.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.common.util.Version;

public class PluginInfo {

    private static final String DEFAULT_PLUGIN_PROPERTIES = "plugin.properties";

    private String name;

    private PluginType type;

    private String description;

    private Version version;

    private Version javaVersion;

    private String className;

    private String soName;

    private String source;

    private String installPath;

    public PluginInfo(String name, PluginType type, String description, Version version, Version javaVersion,
                      String className, String soName, String source) {

        this.name = name;
        this.type = type;
        this.description = description;
        this.version = version;
        this.javaVersion = javaVersion;
        this.className = className;
        this.soName = soName;
        this.source = source;
    }

    public static PluginInfo readFromProperties(final Path propertiesPath, final String source) throws IOException {
        final Path descriptor = propertiesPath.resolve(DEFAULT_PLUGIN_PROPERTIES);

        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptor)) {
                props.load(stream);
            }
            propsMap = props.stringPropertyNames().stream()
                    .collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        final String name = propsMap.remove("name");
        if (null == name || name.isEmpty()) {
            throw new IllegalArgumentException(
                    "property [name] is missing in [" + descriptor + "]");
        }

        final String description = propsMap.remove("description");
        if (null == description) {
            throw new IllegalArgumentException(
                    "property [description] is missing for plugin [" + name + "]");
        }

        final PluginType type;
        final String typeStr = propsMap.remove("type");
        try {
            type = PluginType.valueOf(StringUtils.upperCase(typeStr));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("property [type] is missing for plugin [" + typeStr + "]");
        }

        final String versionString = propsMap.remove("version");
        if (null == versionString) {
            throw new IllegalArgumentException(
                    "property [version] is missing for plugin [" + name + "]");
        }

        Version version = Version.fromString(versionString);

        final String javaVersionString = propsMap.remove("java.version");
        Version javaVersion = Version.JDK_1_8_0;
        if (null != javaVersionString) {
            javaVersion = Version.fromString(javaVersionString);
        }

        final String className = propsMap.remove("classname");

        final String soName = propsMap.remove("soname");

        // version check
        if (version.before(Version.CURRENT_DORIS_VERSION)) {
            throw new IllegalArgumentException("plugin version is too old. plz recompile and modify property "
                    + "[version]");
        }

        // java version check
//        if (javaVersion.after(Version.JDK_1_8_0)) {
//        }

        PluginInfo p = new PluginInfo(name, type, description, version, javaVersion, className, soName, source);
        p.setInstallPath(propertiesPath.toString());
        return p;
    }

    public String getName() {
        return name;
    }

    public PluginType getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public Version getVersion() {
        return version;
    }

    public Version getJavaVersion() {
        return javaVersion;
    }

    public String getClassName() {
        return className;
    }

    public String getSoName() { return soName; }

    public String getSource() {
        return source;
    }

    public void setInstallPath(String installPath) {
        this.installPath = installPath;
    }

    public String getInstallPath() {
        return installPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PluginInfo that = (PluginInfo) o;
        return Objects.equals(name, that.name) &&
                type == that.type &&
                Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}

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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PluginInfo implements Writable {
    public static final Logger LOG = LoggerFactory.getLogger(PluginInfo.class);

    private static final String DEFAULT_PLUGIN_PROPERTIES = "plugin.properties";

    /**
     * Describe the type of plugin
     */
    public enum PluginType {
        AUDIT,
        IMPORT,
        STORAGE,
        DIALECT;

        public static int MAX_PLUGIN_TYPE_SIZE = PluginType.values().length;
    }

    @SerializedName("name")
    protected String name;

    @SerializedName("type")
    protected PluginType type;

    @SerializedName("description")
    protected String description;

    @SerializedName("version")
    protected DigitalVersion version;

    @SerializedName("javaVersion")
    protected DigitalVersion javaVersion;

    @SerializedName("className")
    protected String className;

    @SerializedName("soName")
    protected String soName;

    // this source field is only used for persisting. it should be passed to the source field in 'PluginLoader',
    // and then use 'source' in 'PluginLoader' to get the source.
    @SerializedName("source")
    protected String source;

    @SerializedName("properties")
    protected Map<String, String> properties = Maps.newHashMap();

    public PluginInfo() { }

    // used for persisting uninstall operation
    public PluginInfo(String name) {
        this.name = name;
    }

    public PluginInfo(String name, PluginType type, String description) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.version = DigitalVersion.CURRENT_PLUGIN_VERSION;
        this.javaVersion = DigitalVersion.JDK_1_8_0;
    }

    public PluginInfo(String name, PluginType type, String description, DigitalVersion version,
                         DigitalVersion javaVersion,
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
        if (!descriptor.toFile().exists()) {
            throw new IOException(descriptor.getFileName() + " does not exist");
        }

        final Map<String, String> propsMap;
        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(descriptor)) {
            props.load(stream);
        }
        propsMap = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(Function.identity(), props::getProperty));

        final String name = propsMap.remove("name");
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException(
                    "property [name] is missing in [" + descriptor + "]");
        }

        final String description = propsMap.remove("description");

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

        DigitalVersion version = DigitalVersion.fromString(versionString);

        final String javaVersionString = propsMap.remove("java.version");
        DigitalVersion javaVersion = DigitalVersion.JDK_1_8_0;
        if (null != javaVersionString) {
            javaVersion = DigitalVersion.fromString(javaVersionString);
        }

        final String className = propsMap.remove("classname");

        final String soName = propsMap.remove("soname");

        // version check
        if (version.before(DigitalVersion.CURRENT_DORIS_VERSION)) {
            throw new IllegalArgumentException("plugin version is too old. plz recompile and modify property "
                    + "[version]");
        }

        if (!Strings.isNullOrEmpty(soName)) {
            throw new IllegalArgumentException("Only support FE plugin");
        }

        if (Strings.isNullOrEmpty(className)) {
            throw new IllegalArgumentException("property [className] is missing for plugin [" + name + "]");
        }

        return new PluginInfo(name, type, description, version, javaVersion, className, soName, source);
    }

    public String getName() {
        return name;
    }

    public PluginType getType() {
        return type;
    }

    public int getTypeId() {
        return type.ordinal();
    }

    public String getDescription() {
        return description;
    }

    public DigitalVersion getVersion() {
        return version;
    }

    public DigitalVersion getJavaVersion() {
        return javaVersion;
    }

    public String getClassName() {
        return className;
    }

    public String getSoName() {
        return soName;
    }

    public String getSource() {
        return source;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
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
        return Objects.equals(name, that.name)
                && type == that.type
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static PluginInfo read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, PluginInfo.class);
    }
}

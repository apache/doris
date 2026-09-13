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

package org.apache.doris.connector.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Reads a connector plugin's own configuration file, {@code <pluginDir>/<name>.conf}.
 *
 * <p><b>Engine side only.</b> A connector never calls this: the engine loads the file once at plugin
 * load and serves the parsed map through {@link ConnectorContext#getConnectorConfig()}, which is what
 * connectors read (via {@link ConnectorConf}). Keeping the parsing here means one implementation of
 * "find it, parse it, decide what an absent file means" instead of one per plugin.
 *
 * <p><b>No path validation is needed or wanted here.</b> The file name is {@code name + ".conf"} where
 * {@code name} is a plugin name that {@code PluginNames.validate} has already confined to
 * {@code [a-zA-Z0-9._-]}. No separator can appear in it, so no traversal is expressible. A redundant
 * check added later would only suggest the constraint lives here, when it lives there.
 */
public final class ConnectorConfFile {

    /** The suffix that makes a plugin directory entry a configuration file. */
    public static final String SUFFIX = ".conf";

    private ConnectorConfFile() {
    }

    /** The configuration file name for a plugin called {@code name}. For logs and error messages. */
    public static String fileName(String name) {
        return name + SUFFIX;
    }

    /**
     * Parses {@code <pluginDir>/<name>.conf} in {@link Properties} text format, the same shape as fe.conf.
     *
     * <p>An absent file is <b>not</b> an error and yields an empty map: a connector whose settings all
     * have defaults (or a fe.conf fallback) never needs the file, and requiring one would mean every
     * deployment carries a file full of commented-out lines.
     *
     * <p>Values are trimmed. A key present with an empty value is <b>kept</b> rather than dropped, so the
     * map reflects the file as written; {@link ConnectorConf#get} is where "written but blank" is decided
     * to mean "not set".
     *
     * @return an immutable map, never null
     * @throws IOException if the file exists but cannot be read or parsed
     */
    public static Map<String, String> load(Path pluginDir, String name) throws IOException {
        Objects.requireNonNull(pluginDir, "pluginDir");
        Objects.requireNonNull(name, "name");
        Path file = pluginDir.resolve(fileName(name));
        if (!Files.isRegularFile(file)) {
            return Collections.emptyMap();
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file);
                Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IllegalArgumentException e) {
            // Properties.load throws this (unchecked) on a malformed \\uXXXX escape. Rethrown as
            // IOException so the caller's single "the file is unusable" branch covers every way it can be.
            throw new IOException("malformed content in " + file, e);
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String key : properties.stringPropertyNames()) {
            parsed.put(key, properties.getProperty(key).trim());
        }
        return Collections.unmodifiableMap(parsed);
    }
}

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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ConnectorConfFileTest {

    @TempDir
    private Path pluginDir;

    private Map<String, String> write(String content) throws IOException {
        Files.write(pluginDir.resolve("demo.conf"), content.getBytes(StandardCharsets.UTF_8));
        return ConnectorConfFile.load(pluginDir, "demo");
    }

    @Test
    public void fileName_isNamePlusSuffix() {
        // The plugin's name IS the file name; anything else and the engine looks for a file no plugin ships.
        Assertions.assertEquals("hms.conf", ConnectorConfFile.fileName("hms"));
        Assertions.assertEquals("trino-connector.conf", ConnectorConfFile.fileName("trino-connector"));
    }

    @Test
    public void missingFile_isEmptyMapNotAnError() throws IOException {
        // Deliberate: a connector whose settings all have defaults or a fe.conf fallback ships no conf at
        // all. Making this an error would force every deployment to carry a file of commented-out lines.
        Assertions.assertTrue(ConnectorConfFile.load(pluginDir, "demo").isEmpty());
    }

    @Test
    public void directoryNamedLikeTheConfFile_isEmptyMapNotAnError() throws IOException {
        // isRegularFile, not exists: a directory that happens to be called demo.conf must not blow up
        // plugin loading -- it is not a conf file, so the connector simply has none.
        Files.createDirectory(pluginDir.resolve("demo.conf"));
        Assertions.assertTrue(ConnectorConfFile.load(pluginDir, "demo").isEmpty());
    }

    @Test
    public void parsesCommentsBlankLinesAndTrimsValues() throws IOException {
        Map<String, String> conf = write("# a comment\n"
                + "\n"
                + "drivers_dir =   /opt/drivers   \n"
                + "! another comment style\n"
                + "timeout=30\n");
        Assertions.assertEquals(2, conf.size(), conf.toString());
        // Trimmed, because an operator lining up '=' signs is not configuring a path with spaces in it.
        Assertions.assertEquals("/opt/drivers", conf.get("drivers_dir"));
        Assertions.assertEquals("30", conf.get("timeout"));
    }

    @Test
    public void blankValueIsKeptSoTheMapReflectsTheFile() throws IOException {
        // The loader reports the file as written; deciding that "written but blank" means "not set" is
        // ConnectorConf.get's job. Dropping the key here would erase that distinction before anyone
        // could act on it -- and would make a future information_schema view lie about the file.
        Map<String, String> conf = write("drivers_dir=\nother=   \n");
        Assertions.assertTrue(conf.containsKey("drivers_dir"));
        Assertions.assertEquals("", conf.get("drivers_dir"));
        Assertions.assertEquals("", conf.get("other"));
    }

    @Test
    public void readsUtf8NotIso8859() throws IOException {
        // Properties.load(InputStream) would decode as ISO-8859-1 and mangle this; the reader overload
        // with an explicit UTF-8 charset is what keeps a non-ASCII path usable.
        Map<String, String> conf = write("warehouse=/数据/仓库\n");
        Assertions.assertEquals("/数据/仓库", conf.get("warehouse"));
    }

    @Test
    public void malformedContent_throwsIoExceptionNamingTheFile() throws IOException {
        // Properties.load throws an unchecked IllegalArgumentException on a bad \\uXXXX escape. It is
        // rethrown as IOException so the engine's single "this file is unusable" catch covers every way
        // the file can be bad -- an unchecked escape would instead abort plugin loading entirely.
        Files.write(pluginDir.resolve("demo.conf"), "k=\\uZZZZ\n".getBytes(StandardCharsets.UTF_8));
        IOException e = Assertions.assertThrows(IOException.class,
                () -> ConnectorConfFile.load(pluginDir, "demo"));
        Assertions.assertTrue(e.getMessage().contains("demo.conf"), e.getMessage());
    }

    @Test
    public void returnedMapIsImmutable() throws IOException {
        // It is handed to plugin code through getConnectorConfig(); a plugin must not be able to edit
        // what another catalog of the same type will read next.
        Map<String, String> conf = write("k=v\n");
        Assertions.assertThrows(UnsupportedOperationException.class, () -> conf.put("k2", "v2"));
    }
}

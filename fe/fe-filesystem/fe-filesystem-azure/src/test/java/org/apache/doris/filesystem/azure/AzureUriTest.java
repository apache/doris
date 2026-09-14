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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

class AzureUriTest {

    @Test
    void parseWasbScheme() throws IOException {
        AzureUri uri = AzureUri.parse("wasb://mycontainer@myaccount.blob.core.windows.net/path/to/file");
        Assertions.assertEquals("wasb", uri.scheme());
        Assertions.assertEquals("myaccount", uri.accountName());
        Assertions.assertEquals("mycontainer", uri.container());
        Assertions.assertEquals("path/to/file", uri.key());
    }

    @Test
    void parseWasbsScheme() throws IOException {
        AzureUri uri = AzureUri.parse("wasbs://container@account.blob.core.windows.net/key");
        Assertions.assertEquals("wasbs", uri.scheme());
        Assertions.assertEquals("account", uri.accountName());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("key", uri.key());
    }

    @Test
    void parseAbfsScheme() throws IOException {
        AzureUri uri = AzureUri.parse("abfs://container@account.dfs.core.windows.net/dir/file");
        Assertions.assertEquals("abfs", uri.scheme());
        Assertions.assertEquals("account", uri.accountName());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("dir/file", uri.key());
    }

    @Test
    void parseAbfssScheme() throws IOException {
        AzureUri uri = AzureUri.parse("abfss://container@account.dfs.core.windows.net/");
        Assertions.assertEquals("abfss", uri.scheme());
        Assertions.assertEquals("account", uri.accountName());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void parseHttpsScheme() throws IOException {
        AzureUri uri = AzureUri.parse("https://myaccount.blob.core.windows.net/mycontainer/path");
        Assertions.assertEquals("https", uri.scheme());
        Assertions.assertEquals("myaccount", uri.accountName());
        Assertions.assertEquals("mycontainer", uri.container());
        Assertions.assertEquals("path", uri.key());
    }

    @Test
    void parseS3CompatScheme() throws IOException {
        AzureUri uri = AzureUri.parse("s3://container/key/path");
        Assertions.assertEquals("s3", uri.scheme());
        Assertions.assertEquals("", uri.accountName());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("key/path", uri.key());
    }

    @Test
    void parseWasbNoPath() throws IOException {
        AzureUri uri = AzureUri.parse("wasb://container@account.blob.core.windows.net");
        Assertions.assertEquals("wasb", uri.scheme());
        Assertions.assertEquals("account", uri.accountName());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void toStringReconstructsUri() throws IOException {
        AzureUri uri = AzureUri.parse("wasbs://mycontainer@myaccount.blob.core.windows.net/path/key");
        Assertions.assertEquals("wasbs://mycontainer@myaccount.blob.core.windows.net/path/key", uri.toString());
    }

    @Test
    void nullPathThrows() {
        Assertions.assertThrows(IOException.class, () -> AzureUri.parse(null));
    }

    @Test
    void emptyPathThrows() {
        Assertions.assertThrows(IOException.class, () -> AzureUri.parse(""));
    }

    @Test
    void noSchemeThrows() {
        Assertions.assertThrows(IOException.class, () -> AzureUri.parse("container/path"));
    }

    @Test
    void unsupportedSchemeThrows() {
        Assertions.assertThrows(IOException.class, () -> AzureUri.parse("ftp://host/path"));
    }

    @Test
    void wasbMissingAtSignThrows() {
        Assertions.assertThrows(IOException.class, () -> AzureUri.parse("wasb://container.blob.core.windows.net/path"));
    }

    // ---------------------------------------------------------------------
    // F18 — percent-encoding/decoding, query/fragment stripping, container validation
    // ---------------------------------------------------------------------

    @Test
    void parse_preservesLiteralAdlsPercentSequences() throws IOException {
        AzureUri uri = AzureUri.parse(
                "wasbs://mycontainer@myaccount.blob.core.windows.net/dir/with%20space/a%2Bb.csv");
        Assertions.assertEquals("dir/with%20space/a%2Bb.csv", uri.key());
        Assertions.assertEquals("mycontainer", uri.container());
    }

    @Test
    void parse_stripsQueryAndFragment() throws IOException {
        AzureUri withQuery = AzureUri.parse(
                "wasbs://mycontainer@myaccount.blob.core.windows.net/dir/file.csv?sig=token&se=2030");
        Assertions.assertEquals("dir/file.csv", withQuery.key());

        AzureUri withFragment = AzureUri.parse(
                "wasbs://mycontainer@myaccount.blob.core.windows.net/dir/file.csv#anchor");
        Assertions.assertEquals("dir/file.csv", withFragment.key());

        AzureUri withBoth = AzureUri.parse(
                "wasbs://mycontainer@myaccount.blob.core.windows.net/dir/file.csv?sig=t#frag");
        Assertions.assertEquals("dir/file.csv", withBoth.key());
    }

    @Test
    void parse_rejectsInvalidContainerName() {
        // Uppercase chars are not allowed in Azure container names.
        IOException upperEx = Assertions.assertThrows(IOException.class, () -> AzureUri.parse(
                "wasbs://BadName@account.blob.core.windows.net/key"));
        Assertions.assertTrue(upperEx.getMessage().contains("Invalid Azure container name"),
                "expected container validation message, got: " + upperEx.getMessage());

        // Trailing hyphen is also invalid.
        Assertions.assertThrows(IOException.class, () -> AzureUri.parse(
                "wasbs://bad-@account.blob.core.windows.net/key"));
    }

    @Test
    void parse_emptyKeyWithTrailingSlash() throws IOException {
        AzureUri uri = AzureUri.parse("wasbs://c@a.host/");
        Assertions.assertEquals("wasbs", uri.scheme());
        Assertions.assertEquals("c", uri.container());
        Assertions.assertEquals("a", uri.accountName());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void toString_preservesLiteralAdlsName() throws IOException {
        // ADLSLocation treats these percent sequences as object-name characters.
        // Rendering must not change them before the next SDK call.
        AzureUri uri = AzureUri.parse(
                "wasbs://mycontainer@myaccount.blob.core.windows.net/dir/with%20space/a%2Bb.csv");
        Assertions.assertEquals(
                "wasbs://mycontainer@myaccount.blob.core.windows.net/dir/with%20space/a%2Bb.csv",
                uri.toString());
    }

    @ParameterizedTest
    @CsvSource({
            "abfs, dfs, core.windows.net",
            "abfss, dfs, core.chinacloudapi.cn",
            "abfss, dfs, core.usgovcloudapi.net",
            "abfss, dfs, core.cloudapi.de",
            "wasb, blob, core.windows.net",
            "wasbs, blob, core.chinacloudapi.cn",
            "wasbs, blob, core.usgovcloudapi.net",
            "wasbs, blob, core.cloudapi.de"
    })
    void parse_preservesAccountHostAndCloudSuffix(String scheme, String service, String suffix) throws IOException {
        String location = scheme + "://container@account." + service + "." + suffix + "/dir/file";
        AzureUri uri = AzureUri.parse(location);
        AzureAccountHost host = uri.accountHost().orElseThrow();

        Assertions.assertEquals("account", uri.accountName());
        Assertions.assertEquals("account", host.accountName());
        Assertions.assertEquals(suffix, host.cloudSuffix());
        Assertions.assertEquals("account.dfs." + suffix, host.dfsHost());
        Assertions.assertEquals("https://account.blob." + suffix, host.blobEndpoint());
        Assertions.assertEquals(location, uri.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net", "core.cloudapi.de"})
    void parse_preservesHttpsAuthorityAndContainerPath(String suffix) throws IOException {
        String endpoint = "https://account.blob." + suffix + ":8443";
        String location = endpoint + "/container/dir/file";
        AzureUri uri = AzureUri.parse(location);

        Assertions.assertEquals("account", uri.accountName());
        Assertions.assertEquals(suffix, uri.accountHost().orElseThrow().cloudSuffix());
        Assertions.assertEquals(endpoint, uri.accountHost().orElseThrow().blobEndpoint());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("dir/file", uri.key());
        Assertions.assertEquals(location, uri.toString());
    }

    @Test
    void parse_preservesExplicitHttpTransportAndPort() throws IOException {
        AzureUri uri = AzureUri.parse("http://account.dfs.core.chinacloudapi.cn:10000/container/dir/file");

        Assertions.assertEquals("http://account.blob.core.chinacloudapi.cn:10000",
                uri.accountHost().orElseThrow().blobEndpoint());
        Assertions.assertEquals("http://account.dfs.core.chinacloudapi.cn:10000/container/dir/file", uri.toString());
    }

    @Test
    void parse_preservesCustomAuthorityWithoutInventingCloudSuffix() throws IOException {
        String location = "wasbs://container@storage.example.test:8443/dir/file";
        AzureUri uri = AzureUri.parse(location);

        Assertions.assertEquals("storage", uri.accountName());
        Assertions.assertEquals("", uri.accountHost().orElseThrow().cloudSuffix());
        Assertions.assertEquals("storage.example.test", uri.accountHost().orElseThrow().blobHost());
        Assertions.assertEquals(location, uri.toString());
    }

    @Test
    void parse_preservesOneLakeAuthority() throws IOException {
        String location = "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/data/file";
        AzureUri uri = AzureUri.parse(location);

        Assertions.assertEquals("onelake", uri.accountName());
        Assertions.assertEquals("onelake.dfs.fabric.microsoft.com", uri.accountHost().orElseThrow().dfsHost());
        Assertions.assertEquals("workspace", uri.container());
        Assertions.assertEquals("lakehouse/Tables/data/file", uri.key());
        // Parsing a location must not rewrite OneLake to a Blob endpoint or choose its reader.
        Assertions.assertEquals(location, uri.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3", "s3a", "s3n"})
    void parse_preservesLegacyS3CompatibleLocations(String scheme) throws IOException {
        String location = scheme + "://container/dir/file";
        AzureUri uri = AzureUri.parse(location);

        Assertions.assertEquals("", uri.accountName());
        Assertions.assertTrue(uri.accountHost().isEmpty());
        Assertions.assertEquals("container", uri.container());
        Assertions.assertEquals("dir/file", uri.key());
        Assertions.assertEquals(location, uri.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://account.blob.core.windows.net/container/", "s3://container/"})
    void parse_decodesPathOnceWithoutChangingPlusOrSeparators(String prefix) throws IOException {
        AzureUri uri = AzureUri.parse(prefix + "/dir//http://example/a+b%2Bc%252F%20file%3F%23");

        Assertions.assertEquals("/dir//http://example/a+b+c%2F file?#", uri.key());
        AzureUri roundTrip = AzureUri.parse(uri.toString());
        Assertions.assertEquals(uri.key(), roundTrip.key());
        Assertions.assertEquals(uri.accountName(), roundTrip.accountName());
        Assertions.assertEquals(uri.container(), roundTrip.container());
    }

    @ParameterizedTest
    @ValueSource(strings = {"abfs", "abfss", "wasb", "wasbs"})
    void parse_preservesAdlsNamesWithoutPercentDecoding(String scheme) throws IOException {
        String key = "/dir//http://example/p=a%2Fb/a+b%2Bc%252F%20file%3F%23";
        String location = scheme + "://container@account.dfs.core.windows.net/" + key;
        AzureUri uri = AzureUri.parse(location);

        Assertions.assertEquals(key, uri.key());
        Assertions.assertEquals(location, uri.toString());
        Assertions.assertEquals(key, AzureUri.parse(uri.toString()).key());
    }

    @ParameterizedTest
    @ValueSource(strings = {"%", "%2", "%GG"})
    void parse_acceptsLiteralPercentCharactersInAdlsNames(String name) throws IOException {
        String location = "abfss://container@account.dfs.core.windows.net/" + name;
        AzureUri uri = AzureUri.parse(location);

        Assertions.assertEquals(name, uri.key());
        Assertions.assertEquals(location, uri.toString());
    }

    @Test
    void parse_normalizesSchemeOnly() throws IOException {
        AzureUri uri = AzureUri.parse("ABFSS://container@account.dfs.core.windows.net/Dir/File");

        Assertions.assertEquals("abfss", uri.scheme());
        Assertions.assertEquals("Dir/File", uri.key());
        Assertions.assertEquals("abfss://container@account.dfs.core.windows.net/Dir/File", uri.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"abfss://container@/file", "abfss://@account.dfs.core.windows.net/file",
            "abfss://container@account@other.dfs.core.windows.net/file",
            "abfss://container/file@account.dfs.core.windows.net/file",
            "https:///container/file", "https://user@account.blob.core.windows.net/container/file"})
    void parse_rejectsMalformedAuthorityWithoutLeakingQuery(String location) {
        IOException error = Assertions.assertThrows(IOException.class,
                () -> AzureUri.parse(location + "?sig=secret-signature"));

        Assertions.assertFalse(error.getMessage().contains("secret-signature"));
        Assertions.assertNull(error.getCause());
    }

    @ParameterizedTest
    @ValueSource(strings = {"%", "%2", "%GG"})
    void parse_reportsMalformedHttpEscapeWithoutLeakingQuery(String malformedEscape) {
        IOException error = Assertions.assertThrows(IOException.class, () -> AzureUri.parse(
                "https://account.blob.core.windows.net/container/" + malformedEscape + "?sig=secret-signature"));

        Assertions.assertEquals("Invalid percent encoding in Azure object path", error.getMessage());
        Assertions.assertNull(error.getCause());
    }
}

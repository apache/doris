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

package org.apache.doris.connector.es;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents one ES node with HTTP publish address.
 * Adapted from fe-core's EsNodeInfo — uses plain host+port instead of TNetworkAddress.
 */
public class EsNodeInfo {

    private static final Logger LOG = LogManager.getLogger(EsNodeInfo.class);

    private final String id;
    private final String name;
    private final String host;
    private final String ip;
    private String publishHost;
    private int publishPort;
    private final boolean hasHttp;
    private final boolean isClient;
    private final boolean isData;
    private final boolean isIngest;

    /**
     * Construct from ES _nodes API response entry.
     */
    @SuppressWarnings("unchecked")
    public EsNodeInfo(String id, Map<String, Object> map, boolean httpSslEnabled) {
        this.id = id;
        EsMajorVersion version = EsMajorVersion.parse((String) map.get("version"));
        this.name = (String) map.get("name");
        this.host = (String) map.get("host");
        this.ip = (String) map.get("ip");
        if (version.before(EsMajorVersion.V_5_X)) {
            Map<String, Object> attributes = (Map<String, Object>) map.get("attributes");
            if (attributes == null) {
                this.isClient = false;
                this.isData = true;
            } else {
                String data = (String) attributes.get("data");
                this.isClient = data == null ? true : !Boolean.parseBoolean(data);
                this.isData = data == null ? true : Boolean.parseBoolean(data);
            }
            this.isIngest = false;
        } else {
            List<String> roles = (List<String>) map.get("roles");
            this.isClient = !roles.contains("data");
            this.isData = roles.contains("data");
            this.isIngest = roles.contains("ingest");
        }
        Map<String, Object> httpMap = (Map<String, Object>) map.get("http");
        if (httpMap != null) {
            String address = (String) httpMap.get("publish_address");
            if (address != null) {
                address = address.substring(address.lastIndexOf('/') + 1);
                EsHostAddress.ParsedAddress parsed = EsHostAddress.parse(address, -1);
                this.publishHost = (httpSslEnabled ? "https://" : "") + parsed.getHost();
                this.publishPort = parsed.getPort();
                this.hasHttp = true;
            } else {
                this.hasHttp = false;
            }
        } else {
            this.hasHttp = false;
        }
    }

    /**
     * Construct from a seed address string like "host:port" or "http://host:port".
     */
    public EsNodeInfo(String id, String seed) {
        this.id = id;
        String schemePrefix = EsHostAddress.extractSchemePrefix(seed);
        EsHostAddress.ParsedAddress parsed = EsHostAddress.parse(seed, 80);
        String remoteHost = schemePrefix + parsed.getHost();
        int port = parsed.getPort();
        this.name = remoteHost;
        this.host = remoteHost;
        this.ip = remoteHost;
        this.isClient = true;
        this.isData = true;
        this.isIngest = true;
        this.publishHost = remoteHost;
        this.publishPort = port;
        this.hasHttp = true;
    }

    public boolean hasHttp() {
        return hasHttp;
    }

    public boolean isClient() {
        return isClient;
    }

    public boolean isData() {
        return isData;
    }

    public boolean isIngest() {
        return isIngest;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getPublishHost() {
        return publishHost;
    }

    public int getPublishPort() {
        return publishPort;
    }

    /** Returns "host:port" string for convenience. */
    public String getPublishAddress() {
        return EsHostAddress.formatHostPort(publishHost, publishPort);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EsNodeInfo that = (EsNodeInfo) o;
        return hasHttp == that.hasHttp
                && isClient == that.isClient
                && isData == that.isData
                && publishPort == that.publishPort
                && Objects.equals(id, that.id)
                && Objects.equals(name, that.name)
                && Objects.equals(host, that.host)
                && Objects.equals(ip, that.ip)
                && Objects.equals(publishHost, that.publishHost);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + ip.hashCode();
        result = 31 * result + (publishHost != null ? publishHost.hashCode() : 0);
        result = 31 * result + publishPort;
        result = 31 * result + (hasHttp ? 1 : 0);
        result = 31 * result + (isClient ? 1 : 0);
        result = 31 * result + (isData ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EsNodeInfo{"
                + "id='" + id + '\''
                + ", name='" + name + '\''
                + ", host='" + host + '\''
                + ", ip='" + ip + '\''
                + ", publishAddress=" + getPublishAddress()
                + ", hasHttp=" + hasHttp
                + ", isClient=" + isClient
                + ", isData=" + isData
                + ", isIngest=" + isIngest
                + '}';
    }
}

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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.Config;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.collect.Maps;
import org.apache.http.ssl.SSLContexts;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

public class HttpURLUtil {

    public static HttpURLConnection getConnectionWithNodeIdent(String request) throws IOException {
        HttpURLConnection conn = getConnection(request);
        // Must use Env.getServingEnv() instead of getCurrentEnv(),
        // because here we need to obtain selfNode through the official service catalog.
        HostInfo selfNode = Env.getServingEnv().getSelfNode();
        conn.setRequestProperty(Env.CLIENT_NODE_HOST_KEY, selfNode.getHost());
        conn.setRequestProperty(Env.CLIENT_NODE_PORT_KEY, selfNode.getPort() + "");
        return conn;
    }

    public static HttpURLConnection getConnection(String request) throws IOException {
        if (Config.enable_tls && request.startsWith("http://")) {
            request = "https://" + request.substring(7);
        }
        try {
            SecurityChecker.getInstance().startSSRFChecking(request);
            URL url = new URL(request);
            if (Config.enable_tls) {
                KeyStore keyStore;
                KeyStore trustStore;
                SSLContext sslContext;
                try {
                    keyStore = KeyStore.getInstance("JKS");
                    trustStore = KeyStore.getInstance("JKS");
                    InputStream keyInput = new FileInputStream(Config.tls_certificate_p12_path);
                    keyStore.load(keyInput, Config.tls_private_key_password.toCharArray());
                    InputStream trustInput = new FileInputStream(Config.tls_ca_certificate_p12_path);
                    trustStore.load(trustInput, Config.tls_private_key_password.toCharArray());
                    keyInput.close();
                    trustInput.close();
                } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }

                try {
                    sslContext = SSLContexts.custom().setProtocol("TLSv1.2").setKeyStoreType("JKS")
                        .loadKeyMaterial(keyStore, Config.tls_private_key_password.toCharArray())
                        .loadTrustMaterial(trustStore, null)
                        .build();
                } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException
                         | KeyManagementException e) {
                    throw new RuntimeException(e);
                }
                HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
                conn.setSSLSocketFactory(sslContext.getSocketFactory());
                return conn;
            } else {
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                return conn;
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    }

    public static Map<String, String> getNodeIdentHeaders() throws IOException {
        Map<String, String> headers = Maps.newHashMap();
        // Must use Env.getServingEnv() instead of getCurrentEnv(),
        // because here we need to obtain selfNode through the official service catalog.
        HostInfo selfNode = Env.getServingEnv().getSelfNode();
        headers.put(Env.CLIENT_NODE_HOST_KEY, selfNode.getHost());
        headers.put(Env.CLIENT_NODE_PORT_KEY, selfNode.getPort() + "");
        return headers;
    }

}

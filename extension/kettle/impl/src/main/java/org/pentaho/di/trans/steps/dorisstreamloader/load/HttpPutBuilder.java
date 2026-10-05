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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Builder for HttpPut. */
public class HttpPutBuilder {
    String url;
    Map<String, String> header;
    HttpEntity httpEntity;

    public HttpPutBuilder() {
        header = new HashMap<>();
    }

    public HttpPutBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpPutBuilder addCommonHeader() {
        header.put(HttpHeaders.EXPECT, "100-continue");
        return this;
    }

    public HttpPutBuilder addHiddenColumns(boolean add) {
        if (add) {
            header.put("hidden_columns", LoadConstants.DORIS_DELETE_SIGN);
        }
        return this;
    }

    public HttpPutBuilder addFileName(String fileName) {
        header.put("fileName", fileName);
        return this;
    }

    public HttpPutBuilder enable2PC() {
        header.put("two_phase_commit", "true");
        return this;
    }

    public HttpPutBuilder baseAuth(String user, String password) {
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        header.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
        return this;
    }

    public HttpPutBuilder addTxnId(long txnID) {
        header.put("txn_id", String.valueOf(txnID));
        return this;
    }

    public HttpPutBuilder commit() {
        header.put("txn_operation", "commit");
        return this;
    }

    public HttpPutBuilder abort() {
        header.put("txn_operation", "abort");
        return this;
    }

    public HttpPutBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpPutBuilder setEmptyEntity() {
        try {
            this.httpEntity = new StringEntity("");
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public HttpPutBuilder addProperties(Properties properties) {
        // TODO: check duplicate key.
        properties.forEach((key, value) -> header.put(String.valueOf(key), String.valueOf(value)));
        return this;
    }

    public HttpPutBuilder setLabel(String label) {
        if (label != null) {
            header.put("label", label);
        }
        return this;
    }

    public String getLabel() {
        return header.get("label");
    }

    public HttpPut build() {
        Preconditions.checkNotNull(url);
        Preconditions.checkNotNull(httpEntity);
        HttpPut put = new HttpPut(url);
        header.forEach(put::setHeader);
        put.setEntity(httpEntity);
        return put;
    }
}

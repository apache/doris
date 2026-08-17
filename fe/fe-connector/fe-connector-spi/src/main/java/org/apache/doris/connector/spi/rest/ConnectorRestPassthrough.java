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

package org.apache.doris.connector.spi.rest;

/**
 * A connector that can forward an HTTP request to the source it fronts, returning the source's response
 * verbatim. Exposed through {@link org.apache.doris.connector.spi.Connector#getRestPassthrough()}, which
 * returns {@code null} for the connectors that cannot.
 *
 * <p>This exists for FE HTTP endpoints that deliberately speak a specific source's HTTP dialect — today
 * {@code ESCatalogAction}, whose two endpoints proxy an Elasticsearch mapping lookup and search. Such an
 * endpoint narrows to the catalog type it emulates BEFORE reaching for this capability; the capability itself
 * says only "this connector can forward an HTTP request", and it is the caller that knows what shape of
 * request the source understands.</p>
 *
 * <p>Consequently a connector implementing this is NOT agreeing to serve arbitrary requests from arbitrary
 * engine code: the path is composed by an endpoint that was written for that specific source.</p>
 */
public interface ConnectorRestPassthrough {

    /**
     * Forwards one request and returns the raw response body.
     *
     * @param path the source-relative path, already composed by the caller in the source's own shape
     * @param body the request body, or {@code null} for a GET-style request
     * @return the response body, verbatim
     */
    String executeRestRequest(String path, String body);
}

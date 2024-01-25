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
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/tokens/TokenManager.java
// and modified by Doris

package org.apache.doris.service.arrowflight.tokens;

import org.apache.doris.service.arrowflight.auth2.FlightAuthResult;

/**
 * Token manager.
 */
public interface FlightTokenManager extends AutoCloseable {

    /**
     * Generate a securely random token.
     *
     * @return a token string
     */
    String newToken();

    /**
     * Create a token for the session, and return details about the token.
     *
     * @param username user name
     * @param flightAuthResult auth result
     * @return token details
     */
    FlightTokenDetails createToken(String username, FlightAuthResult flightAuthResult);

    /**
     * Validate the token, and return details about the token.
     *
     * @param token session token
     * @return token details
     * @throws IllegalArgumentException if the token is invalid or expired
     */
    FlightTokenDetails validateToken(String token) throws IllegalArgumentException;

    /**
     * Invalidate the token.
     *
     * @param token session token
     */
    void invalidateToken(String token);

}

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

package org.apache.doris.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

/**
 * Helper class for SSL engine operations.
 */
public class SslEngineHelper {
    private static final Logger LOG = LogManager.getLogger(SslEngineHelper.class);

    /**
     * Check if SSL engine operation has made progress when closed.
     * @param operation operation name for logging ("wrap" or "unwrap")
     * @param sslEngineResult the SSL engine result to check
     * @param sslEngine the SSL engine instance
     * @param closeInbound whether to close inbound (true for unwrap, false for wrap)
     * @throws SSLException if no progress was made
     */
    public static void checkClosedProgress(String operation, SSLEngineResult sslEngineResult,
                                           SSLEngine sslEngine, boolean closeInbound) throws SSLException {
        int consumed = sslEngineResult.bytesConsumed();
        int produced = sslEngineResult.bytesProduced();
        if (consumed == 0 && produced == 0) {
            LOG.warn("SSLEngine {} closed with no progress. status={}, handshake={}, "
                    + "bytesConsumed={}, bytesProduced={}", operation,
                    sslEngineResult.getStatus(), sslEngineResult.getHandshakeStatus(),
                    consumed, produced);
            if (closeInbound) {
                try {
                    sslEngine.closeInbound();
                } catch (SSLException e) {
                    LOG.warn("Error when closing SSL inbound during " + operation, e);
                }
            }
            sslEngine.closeOutbound();
            throw new SSLException("SSL " + operation + " closed with no progress (handshakeStatus="
                    + sslEngineResult.getHandshakeStatus() + ", bytesConsumed="
                    + consumed + ", bytesProduced=" + produced + ")");
        }
        if (closeInbound) {
            try {
                sslEngine.closeInbound();
            } catch (SSLException e) {
                LOG.debug("closeInbound on normal " + operation + " close failed", e);
            }
        }
        LOG.debug("SSLEngine {} closed normally. status={}, handshake={}, "
                + "bytesConsumed={}, bytesProduced={}", operation,
                sslEngineResult.getStatus(), sslEngineResult.getHandshakeStatus(),
                consumed, produced);
        sslEngine.closeOutbound();
    }
}

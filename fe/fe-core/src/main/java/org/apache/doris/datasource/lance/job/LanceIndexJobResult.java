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

package org.apache.doris.datasource.lance.job;

import com.google.gson.annotations.SerializedName;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The immutable typed result of the one-shot invocation, persisted on the job.
 * Contains only what is needed to classify the invocation: the saved typed
 * code, the classified completion reason, a bounded sanitized message, and
 * whether trusted pre-invocation revalidation observed relevant external
 * metadata advancement. Never carries secrets, raw provider responses, or
 * unbounded text.
 */
public class LanceIndexJobResult {
    public static final int MAX_MESSAGE_BYTES = 1024;

    @SerializedName(value = "rc")
    private LanceIndexJobResultCode resultCode = LanceIndexJobResultCode.NO_TRUSTED_RESULT;

    @SerializedName(value = "cr")
    private LanceIndexJobCompletionReason completionReason = LanceIndexJobCompletionReason.NONE;

    @SerializedName(value = "msg")
    private String sanitizedMessage;

    @SerializedName(value = "ema")
    private boolean externalMetadataAdvanced;

    /**
     * No-arg constructor for Gson replay only; missing fields keep the safe
     * defaults declared above.
     */
    public LanceIndexJobResult() {
    }

    public LanceIndexJobResult(LanceIndexJobResultCode resultCode, LanceIndexJobCompletionReason completionReason,
            String sanitizedMessage, boolean externalMetadataAdvanced) {
        this.resultCode = Objects.requireNonNull(resultCode, "resultCode");
        this.completionReason = completionReason == null ? LanceIndexJobCompletionReason.NONE : completionReason;
        this.sanitizedMessage = checkMessageBytes(sanitizedMessage);
        this.externalMetadataAdvanced = externalMetadataAdvanced;
    }

    private static String checkMessageBytes(String message) {
        if (message != null && message.getBytes(StandardCharsets.UTF_8).length > MAX_MESSAGE_BYTES) {
            throw new IllegalArgumentException(
                    "sanitized message exceeds " + MAX_MESSAGE_BYTES + " UTF-8 bytes");
        }
        return message;
    }

    public LanceIndexJobResultCode getResultCode() {
        return resultCode;
    }

    public LanceIndexJobCompletionReason getCompletionReason() {
        return completionReason;
    }

    public String getSanitizedMessage() {
        return sanitizedMessage;
    }

    public boolean isExternalMetadataAdvanced() {
        return externalMetadataAdvanced;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceIndexJobResult)) {
            return false;
        }
        LanceIndexJobResult that = (LanceIndexJobResult) o;
        return externalMetadataAdvanced == that.externalMetadataAdvanced
                && resultCode == that.resultCode
                && completionReason == that.completionReason
                && Objects.equals(sanitizedMessage, that.sanitizedMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultCode, completionReason, sanitizedMessage, externalMetadataAdvanced);
    }

    @Override
    public String toString() {
        return "LanceIndexJobResult{resultCode=" + resultCode + ", completionReason=" + completionReason
                + ", externalMetadataAdvanced=" + externalMetadataAdvanced + '}';
    }
}

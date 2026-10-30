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


package org.apache.doris.datasource.credentials;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Translates the Azure SAS that an Iceberg REST catalog vends per table (e.g. Databricks Unity
 * Catalog) into the Azure storage properties Doris binds.
 *
 * <p>Iceberg's ADLS FileIO names the token by the storage account host it is valid for, so no
 * storage provider can bind it as is:
 * <pre>
 *   adls.sas-token.acct.dfs.core.windows.net               = sv=...&amp;se=...&amp;sig=...
 *   adls.sas-token-expires-at-ms.acct.dfs.core.windows.net = 1792400400000
 * </pre>
 * becomes {@code azure.auth_type=SAS}, {@code azure.account_name=acct},
 * {@code azure.endpoint=https://acct.blob.core.windows.net}, {@code azure.sas_token} and
 * {@code azure.sas_token_expires_at_ms}. BE reaches the account through its Blob endpoint, so a
 * dfs host maps to the blob host of the same cloud.
 *
 * <p>One storage account per table: a BE request carries a single Azure client config. Tokens
 * vended for several accounts are rejected instead of guessing which account the data files use.
 */
public final class IcebergAdlsVendedCredentials {

    static final String SAS_TOKEN_PREFIX = "adls.sas-token.";
    static final String SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";

    private IcebergAdlsVendedCredentials() {
    }

    /**
     * Returns the Azure storage properties for the vended SAS, or an empty map when the
     * credentials carry no Azure SAS.
     *
     * @throws IllegalArgumentException when SAS tokens are vended for more than one storage
     *         account, or one account is vended different tokens
     */
    public static Map<String, String> toAzureProperties(Map<String, String> rawVendedCredentials) {
        // Keyed by account; TreeMap keeps the error message stable.
        Map<String, String> tokenByAccount = new TreeMap<>();
        Map<String, String> hostByAccount = new HashMap<>();
        Map<String, Long> expiryByAccount = new HashMap<>();
        for (Map.Entry<String, String> entry : rawVendedCredentials.entrySet()) {
            // "adls.sas-token-expires-at-ms." does not start with "adls.sas-token.".
            if (!entry.getKey().startsWith(SAS_TOKEN_PREFIX) || StringUtils.isBlank(entry.getValue())) {
                continue;
            }
            String host = entry.getKey().substring(SAS_TOKEN_PREFIX.length());
            String account = accountOf(host);
            // A dfs and a blob host of one account may both be vended; they must carry one token.
            String previous = tokenByAccount.putIfAbsent(account, entry.getValue());
            if (previous != null && !previous.equals(entry.getValue())) {
                throw new IllegalArgumentException("Iceberg REST catalog vended different Azure SAS tokens "
                        + "for storage account " + account);
            }
            hostByAccount.putIfAbsent(account, host);
            String expiry = rawVendedCredentials.get(SAS_TOKEN_EXPIRES_AT_MS_PREFIX + host);
            if (StringUtils.isNotBlank(expiry)) {
                expiryByAccount.merge(account, Long.parseLong(expiry.trim()), Math::min);
            }
        }
        if (tokenByAccount.isEmpty()) {
            return new HashMap<>();
        }
        if (tokenByAccount.size() > 1) {
            throw new IllegalArgumentException("Iceberg REST catalog vended Azure SAS tokens for storage accounts "
                    + tokenByAccount.keySet() + "; Doris supports one storage account per table");
        }
        String account = tokenByAccount.keySet().iterator().next();
        Map<String, String> props = new HashMap<>();
        props.put("azure.auth_type", "SAS");
        props.put("azure.account_name", account);
        props.put("azure.endpoint", blobEndpointOf(hostByAccount.get(account)));
        props.put("azure.sas_token", tokenByAccount.get(account));
        Long expiresAtMs = expiryByAccount.get(account);
        if (expiresAtMs != null) {
            props.put("azure.sas_token_expires_at_ms", Long.toString(expiresAtMs));
        }
        return props;
    }

    // "acct.dfs.core.windows.net" -> "acct".
    private static String accountOf(String host) {
        int dot = host.indexOf('.');
        if (dot <= 0) {
            throw new IllegalArgumentException("Iceberg REST catalog vended an Azure SAS token for an invalid "
                    + "storage account host: " + host);
        }
        return host.substring(0, dot);
    }

    // "acct.dfs.core.chinacloudapi.cn" -> "https://acct.blob.core.chinacloudapi.cn"; a host
    // without a dfs/blob label (a custom endpoint) is used as is.
    private static String blobEndpointOf(String host) {
        String[] labels = host.split("\\.", 3);
        if (labels.length == 3 && (labels[1].equals("dfs") || labels[1].equals("blob"))) {
            return "https://" + labels[0] + ".blob." + labels[2];
        }
        return "https://" + host;
    }
}

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

package org.apache.doris.stack.model.request.config;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public class EmailInfo {
    private String smtpHost;

    private String smtpPort;

    private String smtpSecurity;

    private String smtpUsername;

    private String smtpPassword;

    private String fromAddress;

    /**
     * check empty field
     * @return boolean
     */
    public boolean hasEmptyField() {
        if (smtpHost == null || smtpPort == null || smtpSecurity == null || fromAddress == null) {
            return true;
        }

        if (!smtpSecurity.equals(SmtpSecurity.none.name()) && (smtpUsername == null || smtpPassword == null)) {
            return true;
        }

        return false;
    }

    /**
     * @return string
     */
    @JSONField(name = "email-smtp-host")
    @JsonProperty("email-smtp-host")
    public String getSmtpHost() {
        if (smtpHost != null) {
            return smtpHost.trim();
        } else {
            return smtpHost;
        }
    }

    @JSONField(name = "email-smtp-host")
    @JsonProperty("email-smtp-host")
    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    /**
     * @return string
     */
    @JSONField(name = "email-smtp-port")
    @JsonProperty("email-smtp-port")
    public String getSmtpPort() {
        if (smtpPort != null) {
            return smtpPort.trim();
        } else {
            return smtpPort;
        }
    }

    @JSONField(name = "email-smtp-port")
    @JsonProperty("email-smtp-port")
    public void setSmtpPort(String smtpPort) {
        this.smtpPort = smtpPort;
    }

    /**
     * @return string
     */
    @JSONField(name = "email-smtp-security")
    @JsonProperty("email-smtp-security")
    public String getSmtpSecurity() {
        if (smtpSecurity != null) {
            return smtpSecurity.trim();
        } else {
            return smtpSecurity;
        }
    }

    @JSONField(name = "email-smtp-security")
    @JsonProperty("email-smtp-security")
    public void setSmtpSecurity(String smtpSecurity) {
        this.smtpSecurity = smtpSecurity;
    }

    /**
     * @return string
     */
    @JSONField(name = "email-smtp-username")
    @JsonProperty("email-smtp-username")
    public String getSmtpUsername() {
        if (smtpUsername != null) {
            return smtpUsername.trim();
        } else {
            return smtpUsername;
        }
    }

    @JSONField(name = "email-smtp-username")
    @JsonProperty("email-smtp-username")
    public void setSmtpUsername(String smtpUsername) {
        this.smtpUsername = smtpUsername;
    }

    @JSONField(name = "email-smtp-password")
    @JsonProperty("email-smtp-password")
    public String getSmtpPassword() {
        return smtpPassword;
    }

    @JSONField(name = "email-smtp-password")
    @JsonProperty("email-smtp-password")
    public void setSmtpPassword(String smtpPassword) {
        this.smtpPassword = smtpPassword;
    }

    /**
     * @return string
     */
    @JSONField(name = "email-from-address")
    @JsonProperty("email-from-address")
    public String getFromAddress() {
        if (fromAddress != null) {
            return fromAddress.trim();
        } else {
            return fromAddress;
        }
    }

    @JSONField(name = "email-from-address")
    @JsonProperty("email-from-address")
    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    /**
     * smtp security policy
     */
    public enum SmtpSecurity {
        none,
        ssl,
        tls,
        starttls
    }
}

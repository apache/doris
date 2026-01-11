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

#pragma once

#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <string>

namespace doris {

// Information extracted from a client TLS certificate.
// Used to forward certificate-based authentication info from BE to FE.
struct ClientCertInfo {
    std::string cert_pem;            // Certificate in PEM format (for debugging)
    std::string subject;             // Certificate Subject DN
    std::string san;                 // Subject Alternative Names (formatted string)
    std::string issuer;              // Issuer DN
    std::string cipher;              // SSL cipher suite used
    std::string validity_not_before; // Certificate validity start time
    std::string validity_not_after;  // Certificate validity end time

    bool is_empty() const { return san.empty() && subject.empty(); }
};

// Extract client certificate information from an SSL connection.
// Returns an empty ClientCertInfo if no client certificate is present.
ClientCertInfo extract_client_cert_info(SSL* ssl);

// Helper functions for certificate processing
std::string x509_to_pem(X509* cert);
std::string extract_san(X509* cert);
std::string x509_name_to_string(X509_NAME* name);
std::string extract_validity_not_before(X509* cert);
std::string extract_validity_not_after(X509* cert);

} // namespace doris

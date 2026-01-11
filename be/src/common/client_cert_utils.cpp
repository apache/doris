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

#include "common/client_cert_utils.h"

#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <memory>

#include "common/logging.h"

namespace doris {

std::string x509_to_pem(X509* cert) {
    if (!cert) {
        return "";
    }

    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) {
        return "";
    }

    std::string result;
    if (PEM_write_bio_X509(bio, cert) == 1) {
        char* data = nullptr;
        long len = BIO_get_mem_data(bio, &data);
        if (data && len > 0) {
            result.assign(data, len);
        }
    }

    BIO_free(bio);
    return result;
}

std::string extract_san(X509* cert) {
    if (!cert) {
        return "";
    }

    int san_idx = X509_get_ext_by_NID(cert, NID_subject_alt_name, -1);
    if (san_idx < 0) {
        return "";
    }

    X509_EXTENSION* ext = X509_get_ext(cert, san_idx);
    if (!ext) {
        return "";
    }

    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) {
        return "";
    }

    std::string result;
    // X509V3_EXT_print outputs format like: "email:xxx, DNS:xxx, URI:xxx, IP Address:xxx"
    // This format is consistent with Java's TlsCertificateUtils.extractSubjectAlternativeNames()
    if (X509V3_EXT_print(bio, ext, 0, 0) == 1) {
        char* data_ptr = nullptr;
        long len = BIO_get_mem_data(bio, &data_ptr);
        if (data_ptr && len > 0) {
            result.assign(data_ptr, len);
        }
    }

    BIO_free(bio);
    return result;
}

std::string x509_name_to_string(X509_NAME* name) {
    if (!name) {
        return "";
    }

    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) {
        return "";
    }

    // XN_FLAG_ONELINE: outputs like "CN = Test Client, O = MyOrg"
    X509_NAME_print_ex(bio, name, 0, XN_FLAG_ONELINE);

    char* data = nullptr;
    long len = BIO_get_mem_data(bio, &data);
    std::string result;
    if (data && len > 0) {
        result.assign(data, len);
    }

    BIO_free(bio);
    return result;
}

std::string extract_validity_not_before(X509* cert) {
    if (!cert) {
        return "";
    }

    const ASN1_TIME* not_before = X509_get0_notBefore(cert);
    if (!not_before) {
        return "";
    }

    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) {
        return "";
    }

    ASN1_TIME_print(bio, not_before);

    char* data = nullptr;
    long len = BIO_get_mem_data(bio, &data);
    std::string result;
    if (data && len > 0) {
        result.assign(data, len);
    }

    BIO_free(bio);
    return result;
}

std::string extract_validity_not_after(X509* cert) {
    if (!cert) {
        return "";
    }

    const ASN1_TIME* not_after = X509_get0_notAfter(cert);
    if (!not_after) {
        return "";
    }

    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) {
        return "";
    }

    ASN1_TIME_print(bio, not_after);

    char* data = nullptr;
    long len = BIO_get_mem_data(bio, &data);
    std::string result;
    if (data && len > 0) {
        result.assign(data, len);
    }

    BIO_free(bio);
    return result;
}

ClientCertInfo extract_client_cert_info(SSL* ssl) {
    ClientCertInfo info;
    if (!ssl) {
        return info;
    }

    // SSL_get_peer_certificate returns a certificate that must be freed by the caller
    // Use unique_ptr with custom deleter for RAII
    std::unique_ptr<X509, decltype(&X509_free)> cert(SSL_get_peer_certificate(ssl), X509_free);

    if (!cert) {
        // No client certificate provided
        return info;
    }

    info.cert_pem = x509_to_pem(cert.get());
    info.san = extract_san(cert.get());
    info.subject = x509_name_to_string(X509_get_subject_name(cert.get()));
    info.issuer = x509_name_to_string(X509_get_issuer_name(cert.get()));

    const char* cipher_name = SSL_get_cipher(ssl);
    if (cipher_name) {
        info.cipher = cipher_name;
    }

    info.validity_not_before = extract_validity_not_before(cert.get());
    info.validity_not_after = extract_validity_not_after(cert.get());

    LOG(INFO) << "Extracted client certificate info: subject=" << info.subject
              << ", san=" << info.san << ", issuer=" << info.issuer;

    return info;
}

} // namespace doris

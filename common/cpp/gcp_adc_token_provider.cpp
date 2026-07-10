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

#include "gcp_adc_token_provider.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <glog/logging.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <cstdlib>
#include <fstream>
#include <sstream>

namespace doris {

namespace {

// Refresh this long before the token actually expires.
constexpr std::chrono::seconds kRefreshMargin {300};
// Objects read/write/delete on the bucket, matching what vault I/O needs.
constexpr const char* kGcsScope = "https://www.googleapis.com/auth/devstorage.read_write";
// Overridable for tests, mirroring the convention of Google client libraries.
constexpr const char* kMetadataHostEnv = "GCE_METADATA_HOST";
constexpr const char* kDefaultMetadataHost = "metadata.google.internal";
constexpr const char* kCredentialsFileEnv = "GOOGLE_APPLICATION_CREDENTIALS";

std::string base64url_encode(const unsigned char* data, size_t len) {
    static constexpr char kAlphabet[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string out;
    out.reserve((len + 2) / 3 * 4);
    for (size_t i = 0; i < len; i += 3) {
        unsigned int chunk = data[i] << 16;
        if (i + 1 < len) chunk |= data[i + 1] << 8;
        if (i + 2 < len) chunk |= data[i + 2];
        out.push_back(kAlphabet[(chunk >> 18) & 0x3f]);
        out.push_back(kAlphabet[(chunk >> 12) & 0x3f]);
        if (i + 1 < len) out.push_back(kAlphabet[(chunk >> 6) & 0x3f]);
        if (i + 2 < len) out.push_back(kAlphabet[chunk & 0x3f]);
    }
    return out;
}

std::string base64url_encode(const std::string& data) {
    return base64url_encode(reinterpret_cast<const unsigned char*>(data.data()), data.size());
}

// RS256-sign `input` with the PEM private key. Returns an empty string on failure.
std::string rs256_sign(const std::string& pem_private_key, const std::string& input) {
    std::string signature;
    BIO* bio = BIO_new_mem_buf(pem_private_key.data(), static_cast<int>(pem_private_key.size()));
    if (bio == nullptr) {
        return signature;
    }
    EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (pkey == nullptr) {
        LOG(WARNING) << "GCP ADC: failed to parse service account private key";
        return signature;
    }
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx != nullptr && EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) == 1) {
        size_t sig_len = 0;
        if (EVP_DigestSign(ctx, nullptr, &sig_len,
                           reinterpret_cast<const unsigned char*>(input.data()),
                           input.size()) == 1) {
            std::string buf(sig_len, '\0');
            if (EVP_DigestSign(ctx, reinterpret_cast<unsigned char*>(buf.data()), &sig_len,
                               reinterpret_cast<const unsigned char*>(input.data()),
                               input.size()) == 1) {
                buf.resize(sig_len);
                signature = std::move(buf);
            }
        }
    }
    if (ctx != nullptr) {
        EVP_MD_CTX_free(ctx);
    }
    EVP_PKEY_free(pkey);
    if (signature.empty()) {
        LOG(WARNING) << "GCP ADC: failed to RS256-sign the JWT assertion";
    }
    return signature;
}

// The default ClientConfiguration constructor probes EC2 instance metadata
// for the region, which stalls off-AWS; construct it once and copy, the same
// workaround S3ClientFactory uses.
Aws::Client::ClientConfiguration http_client_configuration() {
    static Aws::Client::ClientConfiguration base_config;
    Aws::Client::ClientConfiguration config = base_config;
    config.connectTimeoutMs = 2000;
    config.requestTimeoutMs = 5000;
    return config;
}

// Issues the request and parses an OAuth2 token response
// ({"access_token": ..., "expires_in": ...}) out of the body.
bool fetch_oauth2_token(const std::shared_ptr<Aws::Http::HttpRequest>& request,
                        const std::string& source, std::string* token,
                        std::chrono::seconds* expires_in) {
    auto client = Aws::Http::CreateHttpClient(http_client_configuration());
    auto response = client->MakeRequest(request);
    if (response == nullptr || response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
        LOG(WARNING) << "GCP ADC: token request to " << source << " failed, http_code="
                     << (response == nullptr ? -1 : static_cast<int>(response->GetResponseCode()));
        return false;
    }
    std::stringstream body;
    body << response->GetResponseBody().rdbuf();
    Aws::Utils::Json::JsonValue json(Aws::String(body.str()));
    if (!json.WasParseSuccessful()) {
        LOG(WARNING) << "GCP ADC: failed to parse token response from " << source;
        return false;
    }
    auto view = json.View();
    if (!view.ValueExists("access_token") || !view.ValueExists("expires_in")) {
        LOG(WARNING) << "GCP ADC: token response from " << source
                     << " misses access_token/expires_in";
        return false;
    }
    *token = view.GetString("access_token");
    *expires_in = std::chrono::seconds(view.GetInteger("expires_in"));
    return !token->empty();
}

} // namespace

std::string GcpAdcTokenProvider::get_token() {
    std::lock_guard<std::mutex> lock(_mutex);
    auto now = std::chrono::steady_clock::now();
    if (!_cached_token.empty() && now + kRefreshMargin < _expire_at) {
        return _cached_token;
    }
    std::string token;
    std::chrono::seconds expires_in {0};
    if (!refresh_token(&token, &expires_in)) {
        // Keep serving a still-valid cached token through transient refresh
        // failures; the margin gives us kRefreshMargin of retries.
        if (!_cached_token.empty() && now < _expire_at) {
            return _cached_token;
        }
        LOG(WARNING) << "GCP ADC: no credential source could provide an access token; "
                        "checked "
                     << kCredentialsFileEnv << " and the metadata server";
        return "";
    }
    _cached_token = std::move(token);
    _expire_at = now + expires_in;
    return _cached_token;
}

bool GcpAdcTokenProvider::refresh_token(std::string* token, std::chrono::seconds* expires_in) {
    const char* credentials_file = std::getenv(kCredentialsFileEnv);
    if (credentials_file != nullptr && credentials_file[0] != '\0') {
        return fetch_from_service_account_file(credentials_file, token, expires_in);
    }
    return fetch_from_metadata_server(token, expires_in);
}

bool GcpAdcTokenProvider::fetch_from_service_account_file(const std::string& credentials_file,
                                                          std::string* token,
                                                          std::chrono::seconds* expires_in) {
    std::ifstream file(credentials_file);
    if (!file.is_open()) {
        LOG(WARNING) << "GCP ADC: cannot open credentials file " << credentials_file;
        return false;
    }
    std::stringstream content;
    content << file.rdbuf();
    Aws::Utils::Json::JsonValue json(Aws::String(content.str()));
    if (!json.WasParseSuccessful()) {
        LOG(WARNING) << "GCP ADC: credentials file " << credentials_file << " is not valid json";
        return false;
    }
    auto view = json.View();
    if (!view.ValueExists("client_email") || !view.ValueExists("private_key") ||
        !view.ValueExists("token_uri")) {
        LOG(WARNING) << "GCP ADC: credentials file " << credentials_file
                     << " misses client_email/private_key/token_uri (service account "
                        "key expected)";
        return false;
    }
    std::string client_email = view.GetString("client_email");
    std::string private_key = view.GetString("private_key");
    std::string token_uri = view.GetString("token_uri");

    auto now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
    Aws::Utils::Json::JsonValue claims;
    claims.WithString("iss", Aws::String(client_email))
            .WithString("scope", kGcsScope)
            .WithString("aud", Aws::String(token_uri))
            .WithInt64("iat", now)
            .WithInt64("exp", now + 3600);
    std::string signing_input = base64url_encode(R"({"alg":"RS256","typ":"JWT"})") + "." +
                                base64url_encode(std::string(claims.View().WriteCompact()));
    std::string signature = rs256_sign(private_key, signing_input);
    if (signature.empty()) {
        return false;
    }
    std::string assertion =
            signing_input + "." +
            base64url_encode(reinterpret_cast<const unsigned char*>(signature.data()),
                             signature.size());

    auto request =
            Aws::Http::CreateHttpRequest(Aws::String(token_uri), Aws::Http::HttpMethod::HTTP_POST,
                                         Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    // The assertion is base64url, no form-encoding needed.
    std::string body_str =
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=" +
            assertion;
    auto body = std::make_shared<std::stringstream>(body_str);
    request->AddContentBody(body);
    request->SetContentLength(std::to_string(body_str.size()));
    request->SetContentType("application/x-www-form-urlencoded");
    return fetch_oauth2_token(request, "the OAuth2 token endpoint", token, expires_in);
}

bool GcpAdcTokenProvider::fetch_from_metadata_server(std::string* token,
                                                     std::chrono::seconds* expires_in) {
    const char* host_override = std::getenv(kMetadataHostEnv);
    std::string host = (host_override != nullptr && host_override[0] != '\0')
                               ? host_override
                               : kDefaultMetadataHost;
    std::string url =
            "http://" + host + "/computeMetadata/v1/instance/service-accounts/default/token";
    auto request =
            Aws::Http::CreateHttpRequest(Aws::String(url), Aws::Http::HttpMethod::HTTP_GET,
                                         Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    request->SetHeaderValue("Metadata-Flavor", "Google");
    return fetch_oauth2_token(request, "the metadata server", token, expires_in);
}

std::shared_ptr<GcpAdcTokenProvider> global_gcp_adc_token_provider() {
    static auto provider = std::make_shared<GcpAdcTokenProvider>();
    return provider;
}

} // namespace doris

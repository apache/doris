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

#include "common/certificate_manager.h"

#include <fmt/format.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <sstream>
#include <system_error>
#include <thread>
#include <utility>

#include "common/logging.h"
#include "util/time.h"

namespace doris {

std::string read_file_to_string(const std::filesystem::path& path, const char* label) {
    if (path.empty()) {
        auto msg = fmt::format("{} path is empty", label);
        LOG(WARNING) << msg;
        return {};
    }

    int retry_count {}, max_retry_num {30};
    std::error_code ec;
    while (!std::filesystem::exists(path, ec)) {
        if (ec) {
            LOG(WARNING) << "Failed to check " << label << " existence: " << path
                         << ", error: " << ec.message();
            return {};
        }
        if (retry_count++ == max_retry_num) {
            LOG(WARNING) << "retry too many times, canle waiting";
            return {};
        }
        LOG(WARNING) << fmt::format("{} does not exist: {}, retry {} times for waiting recreate...",
                                    label, path.native(), retry_count);
        SleepForMs(1000);
    }

    if (!std::filesystem::is_regular_file(path, ec)) {
        if (ec) {
            LOG(WARNING) << "Failed to determine if " << label << " is a regular file: " << path
                         << ", error: " << ec.message();
            return {};
        }
        auto msg = fmt::format("{} is not a regular file: {}", label, path.native());
        LOG(WARNING) << msg;
        return {};
    }

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        auto msg = fmt::format("Unable to open {}: {}", label, path.native());
        LOG(WARNING) << msg;
        return {};
    }

    std::ostringstream buffer;
    buffer << file.rdbuf();
    if (!file.good() && !file.eof()) {
        auto msg = fmt::format("Failed to read {}: {}", label, path.native());
        LOG(WARNING) << msg;
        return {};
    }

    return buffer.str();
}

std::string fetch_openssl_error() {
    std::string combined;
    unsigned long err = ERR_get_error();
    while (err != 0) {
        char buf[256];
        ERR_error_string_n(err, buf, sizeof(buf));
        if (!combined.empty()) {
            combined.append(" | ");
        }
        combined.append(buf);
        err = ERR_get_error();
    }
    return combined;
}

X509* CertificateManager::load_ca(std::filesystem::path path) {
    auto pem_res = read_file_to_string(path, "CA certificate");
    if (pem_res.empty()) {
        LOG(WARNING) << "CA certificate file is empty: " << path.native();
        return nullptr;
    }

    BIO* bio = BIO_new_mem_buf(pem_res.data(), static_cast<int>(pem_res.size()));
    if (bio == nullptr) {
        auto msg = fmt::format("Failed to create BIO for CA certificate: {}", path.native());
        LOG(WARNING) << msg;
        return nullptr;
    }

    X509* ca = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (ca == nullptr) {
        auto err_msg = fetch_openssl_error();
        LOG(WARNING) << "Failed to parse CA certificate: " << path
                     << (err_msg.empty() ? "" : ", openssl: " + err_msg);
        BIO_free(bio);
        return nullptr;
    }

    BIO_free(bio);
    return ca;
}

X509* CertificateManager::load_cert(std::filesystem::path path) {
    auto pem_res = read_file_to_string(path, "certificate");
    if (pem_res.empty()) {
        auto msg = fmt::format("certificate file is empty: {}", path.native());
        return nullptr;
    }

    BIO* bio = BIO_new_mem_buf(pem_res.data(), static_cast<int>(pem_res.size()));
    if (bio == nullptr) {
        auto msg = fmt::format("Failed to create BIO for certificate: {}", path.native());
        LOG(WARNING) << msg;
        return nullptr;
    }

    X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (cert == nullptr) {
        auto err_msg = fetch_openssl_error();
        LOG(WARNING) << "Failed to parse certificate: " << path
                     << (err_msg.empty() ? "" : ", openssl: " + err_msg);
        BIO_free(bio);
        return nullptr;
    }

    BIO_free(bio);

    return cert;
}

EVP_PKEY* CertificateManager::load_key(std::filesystem::path path, std::string passwd) {
    auto pem_res = read_file_to_string(path, "private key");
    if (pem_res.empty()) {
        auto msg = fmt::format("private key file is empty: {}", path.native());
        LOG(WARNING) << msg;
        return nullptr;
    }

    BIO* bio = BIO_new_mem_buf(pem_res.data(), static_cast<int>(pem_res.size()));
    if (bio == nullptr) {
        auto msg = fmt::format("Failed to create BIO for private key: {}", path.native());
        LOG(WARNING) << msg;
        return nullptr;
    }

    // Custom callback to provide password without interactive prompt
    auto password_callback = [](char* buf, int size, int rwflag, void* userdata) -> int {
        if (userdata == nullptr) {
            return 0;
        }
        const char* password = static_cast<const char*>(userdata);
        int len = strlen(password);
        len = std::min(len, size);
        memcpy(buf, password, len);
        return len;
    };

    void* passwd_ptr = passwd.empty() ? nullptr : const_cast<char*>(passwd.c_str());
    EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, nullptr, password_callback, passwd_ptr);
    if (pkey == nullptr) {
        auto err_msg = fetch_openssl_error();
        LOG(WARNING) << "Failed to parse private key: " << path
                     << (err_msg.empty() ? "" : ", openssl: " + err_msg);
        BIO_free(bio);
        return nullptr;
    }
    BIO_free(bio);
    return pkey;
}

std::string CertificateManager::load_key_string(std::filesystem::path path, std::string passwd) {
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(load_key(path, passwd), EVP_PKEY_free);
    if (key == nullptr) {
        return {};
    }
    std::string parsed_key;
    using BIOBufferPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
    BIOBufferPtr buffer(BIO_new(BIO_s_mem()), BIO_free);
    if (!buffer) {
        LOG(ERROR) << "Fail to allocate memory BIO for private key";
        return {};
    }

    if (PEM_write_bio_PrivateKey(buffer.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr) !=
        1) {
        LOG(ERROR) << "Fail to write private key to buffer";
        return {};
    }

    char* data = nullptr;
    long len = BIO_get_mem_data(buffer.get(), &data);
    if (len <= 0 || data == nullptr) {
        LOG(ERROR) << "Fail to retrieve private key buffer";
        return {};
    }

    parsed_key.assign(data, static_cast<size_t>(len));
    return parsed_key;
}

bool CertificateManager::check_certificate_file(const std::string& path,
                                                CertFileMonitorState* state, const char* label,
                                                const std::function<bool()>& should_stop) {
    if (path.empty()) {
        return false;
    }

    namespace fs = std::filesystem;
    auto should_terminate = [&]() { return should_stop && should_stop(); };

    std::error_code exists_ec;
    bool exists = fs::exists(path, exists_ec);
    if (exists_ec) {
        LOG(WARNING) << "Failed to check existence for " << label << " file: " << path
                     << ", error: " << exists_ec.message();
        if (state != nullptr) {
            state->has_value = false;
        }
        return false;
    }

    if (!exists) {
        if (state != nullptr && state->has_value) {
            LOG(WARNING) << label << " file deleted: " << path << ", waiting for recreation...";
            constexpr int wait_seconds = 30;
            constexpr int interval_seconds = 1;
            for (int waited = 0; waited < wait_seconds; waited += interval_seconds) {
                if (should_terminate()) {
                    return false;
                }
                std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));
                std::error_code retry_exists_ec;
                if (!fs::exists(path, retry_exists_ec) || retry_exists_ec) {
                    continue;
                }
                std::error_code retry_time_ec;
                auto current = fs::last_write_time(path, retry_time_ec);
                if (retry_time_ec) {
                    LOG(WARNING) << "Failed to get last write time after recreation for " << label
                                 << " file: " << path << ", error: " << retry_time_ec.message();
                    continue;
                }
                if (state != nullptr) {
                    state->write_time = current;
                    state->has_value = true;
                }
                LOG(INFO) << label << " file recreated and detected: " << path;
                return true;
            }
            LOG(ERROR) << label << " file was deleted and not recreated within " << wait_seconds
                       << " seconds, certificate reload cancelled: " << path;
        }
        if (state != nullptr) {
            state->has_value = false;
        }
        return false;
    }

    std::error_code time_ec;
    auto current = fs::last_write_time(path, time_ec);
    if (time_ec) {
        LOG(WARNING) << "Failed to get last write time for " << label << " file: " << path
                     << ", error: " << time_ec.message();
        if (state != nullptr) {
            state->has_value = false;
        }
        return false;
    }

    if (state == nullptr) {
        return false;
    }

    if (!state->has_value) {
        state->write_time = current;
        state->has_value = true;
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                              state->write_time.time_since_epoch())
                              .count();
        LOG(INFO) << label << " file write time initialized, ms since epoch: " << millis;
        return true;
    }

    if (current != state->write_time) {
        auto old_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  state->write_time.time_since_epoch())
                                  .count();
        auto new_millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(current.time_since_epoch())
                        .count();
        LOG(INFO) << label << " file write time changed: " << old_millis << " -> " << new_millis;
        state->write_time = current;
        state->has_value = true;
        return true;
    }

    return false;
}

} // namespace doris

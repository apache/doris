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

#include "util/s3_util.h"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <util/string_util.h>

#include "common/config.h"
#include "common/logging.h"
#include "s3_uri.h"

namespace doris {

class DorisAWSLogger final : public Aws::Utils::Logging::LogSystemInterface {
public:
    DorisAWSLogger() : _log_level(Aws::Utils::Logging::LogLevel::Info) {}
    DorisAWSLogger(Aws::Utils::Logging::LogLevel log_level) : _log_level(log_level) {}
    ~DorisAWSLogger() final = default;
    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return _log_level; }
    void Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str,
             ...) final {
        _log_impl(log_level, tag, format_str);
    }
    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char* tag,
                   const Aws::OStringStream& message_stream) final {
        _log_impl(log_level, tag, message_stream.str().c_str());
    }

    void Flush() final {}

private:
    void _log_impl(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* message) {
        switch (log_level) {
        case Aws::Utils::Logging::LogLevel::Off:
            break;
        case Aws::Utils::Logging::LogLevel::Fatal:
            LOG(FATAL) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Error:
            LOG(ERROR) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Warn:
            LOG(WARNING) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Info:
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Debug:
            VLOG_ROW << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Trace:
            VLOG_ROW << "[" << tag << "] " << message;
            break;
        default:
            break;
        }
    }

    std::atomic<Aws::Utils::Logging::LogLevel> _log_level;
};

const static std::string USE_PATH_STYLE = "use_path_style";

ClientFactory::ClientFactory() {
    _aws_options = Aws::SDKOptions {};
    Aws::Utils::Logging::LogLevel logLevel =
            static_cast<Aws::Utils::Logging::LogLevel>(config::aws_log_level);
    _aws_options.loggingOptions.logLevel = logLevel;
    _aws_options.loggingOptions.logger_create_fn = [logLevel] {
        return std::make_shared<DorisAWSLogger>(logLevel);
    };
    Aws::InitAPI(_aws_options);
}

ClientFactory::~ClientFactory() {
    Aws::ShutdownAPI(_aws_options);
}

ClientFactory& ClientFactory::instance() {
    static ClientFactory ret;
    return ret;
}

bool ClientFactory::is_s3_conf_valid(const std::map<std::string, std::string>& prop) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    if (properties.find(S3_AK) == properties.end() || properties.find(S3_SK) == properties.end() ||
        properties.find(S3_ENDPOINT) == properties.end() ||
        properties.find(S3_REGION) == properties.end()) {
        DCHECK(false) << "aws properties is incorrect.";
        LOG(ERROR) << "aws properties is incorrect.";
        return false;
    }
    return true;
}

std::shared_ptr<Aws::S3::S3Client> ClientFactory::create(
        const std::map<std::string, std::string>& prop) {
    if (!is_s3_conf_valid(prop)) {
        return nullptr;
    }
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    Aws::Auth::AWSCredentials aws_cred(properties.find(S3_AK)->second,
                                       properties.find(S3_SK)->second);
    DCHECK(!aws_cred.IsExpiredOrEmpty());

    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = properties.find(S3_ENDPOINT)->second;
    aws_config.region = properties.find(S3_REGION)->second;
    if (properties.find(S3_MAX_CONN_SIZE) != properties.end()) {
        aws_config.maxConnections = std::atoi(properties.find(S3_MAX_CONN_SIZE)->second.c_str());
    }
    if (properties.find(S3_REQUEST_TIMEOUT_MS) != properties.end()) {
        aws_config.requestTimeoutMs =
                std::atoi(properties.find(S3_REQUEST_TIMEOUT_MS)->second.c_str());
    }
    if (properties.find(S3_CONN_TIMEOUT_MS) != properties.end()) {
        aws_config.connectTimeoutMs =
                std::atoi(properties.find(S3_CONN_TIMEOUT_MS)->second.c_str());
    }

    aws_config.verifySSL = false;
    // See https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_s3_1_1_s3_client.html
    bool use_virtual_addressing = true;
    if (properties.find(USE_PATH_STYLE) != properties.end()) {
        use_virtual_addressing = properties.find(USE_PATH_STYLE)->second == "true" ? false : true;
    }
    return std::make_shared<Aws::S3::S3Client>(
            std::move(aws_cred), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, use_virtual_addressing);
}

bool ClientFactory::is_s3_conf_valid(const S3Conf& s3_conf) {
    return !s3_conf.ak.empty() && !s3_conf.sk.empty() && !s3_conf.endpoint.empty();
}

std::shared_ptr<Aws::S3::S3Client> ClientFactory::create(const S3Conf& s3_conf) {
    if (!is_s3_conf_valid(s3_conf)) {
        return nullptr;
    }
    Aws::Auth::AWSCredentials aws_cred(s3_conf.ak, s3_conf.sk);
    DCHECK(!aws_cred.IsExpiredOrEmpty());

    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = s3_conf.endpoint;
    aws_config.region = s3_conf.region;
    if (s3_conf.max_connections > 0) {
        aws_config.maxConnections = s3_conf.max_connections;
    }
    if (s3_conf.request_timeout_ms > 0) {
        aws_config.requestTimeoutMs = s3_conf.request_timeout_ms;
    }
    if (s3_conf.connect_timeout_ms > 0) {
        aws_config.connectTimeoutMs = s3_conf.connect_timeout_ms;
    }
    return std::make_shared<Aws::S3::S3Client>(
            std::move(aws_cred), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            s3_conf.use_virtual_addressing);
}

Status ClientFactory::convert_properties_to_s3_conf(const std::map<std::string, std::string>& prop,
                                                    const S3URI& s3_uri, S3Conf* s3_conf) {
    if (!is_s3_conf_valid(prop)) {
        return Status::InvalidArgument("S3 properties are incorrect, please check properties.");
    }
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    s3_conf->ak = properties.find(S3_AK)->second;
    s3_conf->sk = properties.find(S3_SK)->second;
    s3_conf->endpoint = properties.find(S3_ENDPOINT)->second;
    s3_conf->region = properties.find(S3_REGION)->second;

    if (properties.find(S3_MAX_CONN_SIZE) != properties.end()) {
        s3_conf->max_connections = std::atoi(properties.find(S3_MAX_CONN_SIZE)->second.c_str());
    }
    if (properties.find(S3_REQUEST_TIMEOUT_MS) != properties.end()) {
        s3_conf->request_timeout_ms =
                std::atoi(properties.find(S3_REQUEST_TIMEOUT_MS)->second.c_str());
    }
    if (properties.find(S3_CONN_TIMEOUT_MS) != properties.end()) {
        s3_conf->connect_timeout_ms =
                std::atoi(properties.find(S3_CONN_TIMEOUT_MS)->second.c_str());
    }
    s3_conf->bucket = s3_uri.get_bucket();
    s3_conf->prefix = "";

    // See https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_s3_1_1_s3_client.html
    s3_conf->use_virtual_addressing = true;
    if (properties.find(USE_PATH_STYLE) != properties.end()) {
        s3_conf->use_virtual_addressing =
                properties.find(USE_PATH_STYLE)->second == "true" ? false : true;
    }
    return Status::OK();
}

} // end namespace doris

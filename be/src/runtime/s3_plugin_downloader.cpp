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

#include "runtime/s3_plugin_downloader.h"

#include <fmt/format.h>
#include <zlib.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include "common/logging.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_obj_storage_client.h"
#include "util/s3_util.h"

namespace doris {

std::string S3PluginDownloader::S3Config::to_string() const {
    return fmt::format("S3Config{{endpoint='{}', region='{}', bucket='{}', access_key='{}'}}",
                       endpoint, region, bucket, access_key.empty() ? "null" : "***");
}

S3PluginDownloader::S3PluginDownloader(const S3Config& config) : config_(config) {
    s3_client_ = create_s3_client(config_);
    if (!s3_client_) {
        throw std::runtime_error("Failed to create S3 client");
    }
}

S3PluginDownloader::~S3PluginDownloader() = default;

std::string S3PluginDownloader::download_file(const std::string& remote_s3_path,
                                              const std::string& local_path,
                                              const std::string& expected_md5) {
    for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; ++attempt) {
        try {
            return do_download_file(remote_s3_path, local_path, expected_md5);
        } catch (const std::exception& e) {
            LOG(WARNING) << "Download attempt " << attempt << "/" << MAX_RETRY_ATTEMPTS
                         << " failed for " << remote_s3_path << ": " << e.what();

            if (attempt == MAX_RETRY_ATTEMPTS) {
                LOG(ERROR) << "Download failed after " << MAX_RETRY_ATTEMPTS
                           << " attempts: " << e.what();
                return "";
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS * attempt));
        }
    }
    return "";
}

std::string S3PluginDownloader::download_and_extract_directory(
        const std::string& remote_s3_directory, const std::string& local_directory) {
    try {
        // Ensure directory path ends with /
        std::string remote_dir = remote_s3_directory;
        if (!remote_dir.ends_with("/")) {
            remote_dir += "/";
        }

        // Create local directory
        Status status = create_parent_directory(local_directory + "/dummy");
        if (!status.ok()) {
            throw std::runtime_error("Failed to create parent directory: " + status.to_string());
        }

        // Parse S3 path to get prefix
        S3PathInfo path_info = parse_s3_path(remote_dir);

        // List remote tar.gz files
        std::vector<io::FileInfo> remote_files;
        status = list_remote_files(path_info.key, &remote_files);
        if (!status.ok()) {
            throw std::runtime_error("Failed to list files from " + remote_dir + ": " +
                                     status.to_string());
        }

        bool has_download = false;
        for (const auto& remote_file : remote_files) {
            if (remote_file.is_file && remote_file.file_name.ends_with(".tar.gz")) {
                std::filesystem::path file_path(remote_file.file_name);
                std::string file_name = file_path.filename().string();
                std::string temp_file_path = std::filesystem::path(local_directory) / file_name;

                // Build complete S3 path
                std::string full_s3_path =
                        fmt::format("s3://{}/{}", config_.bucket, remote_file.file_name);

                // Download tar.gz file
                std::string downloaded_file = download_file(full_s3_path, temp_file_path);
                if (!downloaded_file.empty()) {
                    // Extract to target directory
                    status = extract_tar_gz(temp_file_path, local_directory);
                    if (status.ok()) {
                        // Delete temporary tar.gz file
                        std::filesystem::remove(temp_file_path);
                        LOG(INFO) << "Extracted and cleaned up: " << file_name;
                        has_download = true;
                    } else {
                        LOG(WARNING) << "Failed to extract: " << file_name
                                     << ", error: " << status.to_string();
                    }
                }
            }
        }

        return has_download ? local_directory : "";

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to download and extract directory " << remote_s3_directory << ": "
                     << e.what();
        return "";
    }
}

std::string S3PluginDownloader::do_download_file(const std::string& remote_s3_path,
                                                 const std::string& local_path,
                                                 const std::string& expected_md5) {
    // Check if local file exists and is valid
    if (is_local_file_valid(local_path, expected_md5)) {
        LOG(INFO) << "Local file " << local_path << " is up to date, skipping download";
        return local_path;
    }

    // Create parent directory
    Status status = create_parent_directory(local_path);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create parent directory: " + status.to_string());
    }

    // Parse S3 path
    S3PathInfo path_info = parse_s3_path(remote_s3_path);

    // Prepare download parameters
    io::ObjectStoragePathOptions opts;
    opts.bucket = path_info.bucket;
    opts.key = path_info.key;

    // Get file size
    auto head_response = s3_client_->head_object(opts);
    if (head_response.resp.status.code != 0) {
        throw std::runtime_error("Remote file not found: " + remote_s3_path);
    }

    size_t file_size = head_response.file_size;
    if (file_size == 0) {
        throw std::runtime_error("Remote file is empty: " + remote_s3_path);
    }

    // Create local file and download
    std::ofstream local_file(local_path, std::ios::binary);
    if (!local_file.is_open()) {
        throw std::runtime_error("Cannot create local file: " + local_path);
    }

    // Chunked download
    constexpr size_t CHUNK_SIZE = 8 * 1024 * 1024; // 8MB chunks
    std::vector<char> buffer(std::min(file_size, CHUNK_SIZE));

    size_t offset = 0;
    while (offset < file_size) {
        size_t bytes_to_read = std::min(CHUNK_SIZE, file_size - offset);
        size_t bytes_read = 0;

        auto response =
                s3_client_->get_object(opts, buffer.data(), offset, bytes_to_read, &bytes_read);

        if (response.status.code != 0) {
            local_file.close();
            std::filesystem::remove(local_path); // Clean up partial file
            throw std::runtime_error(fmt::format("Failed to download chunk at offset {}: {}",
                                                 offset, response.status.msg));
        }

        local_file.write(buffer.data(), bytes_read);
        if (!local_file.good()) {
            local_file.close();
            std::filesystem::remove(local_path); // Clean up partial file
            throw std::runtime_error("Failed to write to local file: " + local_path);
        }

        offset += bytes_read;
    }

    local_file.close();

    // Verify file size
    if (std::filesystem::file_size(local_path) != file_size) {
        std::filesystem::remove(local_path); // Clean up incomplete file
        throw std::runtime_error("Downloaded file size mismatch");
    }

    // MD5 verification (if expected value is provided)
    if (!expected_md5.empty()) {
        std::string actual_md5 = calculate_file_md5(local_path);
        if (actual_md5.empty() || expected_md5 != actual_md5) {
            std::filesystem::remove(local_path); // Delete invalid file
            throw std::runtime_error(
                    fmt::format("MD5 mismatch: expected={}, actual={}", expected_md5, actual_md5));
        }
    }

    LOG(INFO) << "Successfully downloaded " << remote_s3_path << " to " << local_path;
    return local_path;
}

bool S3PluginDownloader::is_local_file_valid(const std::string& local_path,
                                             const std::string& expected_md5) {
    try {
        std::filesystem::path file_path(local_path);
        if (!std::filesystem::exists(file_path) || std::filesystem::file_size(file_path) == 0) {
            return false;
        }

        // If no MD5 provided, only check file existence and size
        if (expected_md5.empty()) {
            LOG(INFO) << "Local file " << local_path << " exists with size "
                      << std::filesystem::file_size(file_path) << ", assuming valid";
            return true;
        }

        // MD5 verification
        std::string actual_md5 = calculate_file_md5(local_path);
        bool is_valid = !actual_md5.empty() && expected_md5 == actual_md5;

        if (!is_valid) {
            LOG(INFO) << "Local file " << local_path << " MD5 mismatch: expected=" << expected_md5
                      << ", actual=" << actual_md5;
        }

        return is_valid;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Error checking local file validity for " << local_path << ": " << e.what();
        return false;
    }
}

Status S3PluginDownloader::create_parent_directory(const std::string& file_path) {
    try {
        std::filesystem::path path(file_path);
        std::filesystem::path parent_dir = path.parent_path();

        if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
            std::filesystem::create_directories(parent_dir);
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError("Failed to create parent directory: {}", e.what());
    }
}

Status S3PluginDownloader::extract_tar_gz(const std::string& tar_gz_path,
                                          const std::string& target_dir) {
    try {
        // Create target directory
        Status status = create_parent_directory(target_dir + "/dummy");
        RETURN_IF_ERROR(status);

        // Open tar.gz file
        std::ifstream file(tar_gz_path, std::ios::binary);
        if (!file.is_open()) {
            return Status::IOError("Cannot open tar.gz file: {}", tar_gz_path);
        }

        // Read entire file to memory
        std::vector<char> compressed_data((std::istreambuf_iterator<char>(file)),
                                          std::istreambuf_iterator<char>());
        file.close();

        // Use zlib to decompress (gzip format)
        z_stream zs = {};
        if (inflateInit2(&zs, 16 + MAX_WBITS) != Z_OK) {
            return Status::IOError("Failed to initialize zlib decompression");
        }

        // Set input buffer
        zs.next_in = reinterpret_cast<Bytef*>(compressed_data.data());
        zs.avail_in = compressed_data.size();

        // Chunked decompression
        std::vector<char> decompressed_data;
        constexpr size_t CHUNK_SIZE = 16384;
        char out_buffer[CHUNK_SIZE];

        int ret;
        do {
            zs.next_out = reinterpret_cast<Bytef*>(out_buffer);
            zs.avail_out = CHUNK_SIZE;

            ret = inflate(&zs, Z_NO_FLUSH);
            if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
                inflateEnd(&zs);
                return Status::IOError("zlib decompression error: {}", ret);
            }

            size_t bytes_written = CHUNK_SIZE - zs.avail_out;
            decompressed_data.insert(decompressed_data.end(), out_buffer,
                                     out_buffer + bytes_written);
        } while (ret != Z_STREAM_END);

        inflateEnd(&zs);

        // Simple tar parsing (basic implementation of standard tar format)
        const char* tar_data = decompressed_data.data();
        size_t offset = 0;
        size_t tar_size = decompressed_data.size();

        while (offset + 512 <= tar_size) {
            // Read tar header (512 bytes)
            const char* header = tar_data + offset;

            // Check if reached end of archive file (empty header)
            if (header[0] == '\0') {
                break;
            }

            // Extract file name
            std::string file_name(header, std::find(header, header + 100, '\0'));
            if (file_name.empty()) {
                break;
            }

            // Extract file size (octal format, offset 124)
            std::string size_str(header + 124, 12);
            size_t file_size = 0;
            try {
                file_size = std::stoull(size_str, nullptr, 8);
            } catch (...) {
                // Skip invalid entries
                offset += 512;
                continue;
            }

            // Check file type (offset 156)
            char type_flag = header[156];

            offset += 512; // Move past header

            if (type_flag == '0' || type_flag == '\0') {
                // Regular file
                std::filesystem::path full_path = std::filesystem::path(target_dir) / file_name;
                std::filesystem::create_directories(full_path.parent_path());

                std::ofstream out_file(full_path, std::ios::binary);
                if (out_file.is_open() && offset + file_size <= tar_size) {
                    out_file.write(tar_data + offset, file_size);
                    out_file.close();
                }
            } else if (type_flag == '5') {
                // Directory
                std::filesystem::path dir_path = std::filesystem::path(target_dir) / file_name;
                std::filesystem::create_directories(dir_path);
            }

            // Move to next entry (file padded to 512-byte boundary)
            offset += (file_size + 511) & ~511;
        }

        LOG(INFO) << "Successfully extracted " << tar_gz_path << " to " << target_dir;
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::IOError("Failed to extract tar.gz file {}: {}", tar_gz_path, e.what());
    }
}

std::string S3PluginDownloader::calculate_file_md5(const std::string& file_path) {
    try {
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            return "";
        }

        // Simple MD5 implementation, should use a more complete MD5 library in actual projects
        // Return empty string to indicate MD5 verification is not supported yet
        LOG(WARNING) << "MD5 calculation not implemented yet for file: " << file_path;
        return "";

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to calculate MD5 for file " << file_path << ": " << e.what();
        return "";
    }
}

std::shared_ptr<io::S3ObjStorageClient> S3PluginDownloader::create_s3_client(
        const S3Config& config) {
    try {
        // Create S3 configuration
        S3Conf s3_conf;
        s3_conf.bucket = config.bucket;
        s3_conf.client_conf.endpoint = config.endpoint;
        s3_conf.client_conf.region = config.region;
        s3_conf.client_conf.ak = config.access_key;
        s3_conf.client_conf.sk = config.secret_key;
        s3_conf.client_conf.provider = io::ObjStorageType::AWS; // Default to AWS compatible

        // Use existing factory to create S3 client
        auto obj_storage_client = S3ClientFactory::instance().create(s3_conf.client_conf);
        if (!obj_storage_client) {
            LOG(WARNING) << "Failed to create S3 client";
            return nullptr;
        }

        // Convert to S3ObjStorageClient
        auto s3_client = std::dynamic_pointer_cast<io::S3ObjStorageClient>(obj_storage_client);
        if (!s3_client) {
            LOG(WARNING) << "Failed to cast to S3ObjStorageClient";
            return nullptr;
        }

        return s3_client;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception creating S3 client: " << e.what();
        return nullptr;
    }
}

S3PluginDownloader::S3PathInfo S3PluginDownloader::parse_s3_path(const std::string& s3_path) {
    S3PathInfo info;

    if (s3_path.starts_with("s3://")) {
        std::string path = s3_path.substr(5); // Remove "s3://"
        size_t slash_pos = path.find('/');
        if (slash_pos != std::string::npos) {
            info.bucket = path.substr(0, slash_pos);
            info.key = path.substr(slash_pos + 1);
        } else {
            info.bucket = path;
            info.key = "";
        }
    } else {
        // Assume it's directly a key, use bucket from configuration
        info.bucket = config_.bucket;
        info.key = s3_path;
    }

    return info;
}

Status S3PluginDownloader::list_remote_files(const std::string& remote_prefix,
                                             std::vector<io::FileInfo>* files) {
    // Prepare list parameters
    io::ObjectStoragePathOptions opts;
    opts.bucket = config_.bucket;
    opts.prefix = remote_prefix;

    // List objects
    auto response = s3_client_->list_objects(opts, files);
    if (response.status.code != 0) {
        return Status::IOError("Failed to list remote files: {}", response.status.msg);
    }

    return Status::OK();
}

} // namespace doris
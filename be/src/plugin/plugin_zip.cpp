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

#include "plugin/plugin_zip.h"

#include <string.h>

#include <boost/algorithm/string/predicate.hpp>

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "http/http_client.h"
#include "util/file_utils.h"
#include "util/md5.h"
#include "util/slice.h"
#include "util/time.h"
#include "util/zip_util.h"

namespace doris {

using namespace strings;

bool is_local_source(const std::string& source) {
    if (HasPrefixString(source, "http") || HasPrefixString(source, "https")) {
        return false;
    }

    return true;
}

PluginZip::~PluginZip() {
    for (auto& p : _clean_paths) {
        WARN_IF_ERROR(FileUtils::remove_all(p), "clean plugin_zip temp path failed: " + p);
    }
}

Status PluginZip::extract(const std::string& target_dir, const std::string& plugin_name) {
    // check plugin install path
    std::string plugin_install_path = strings::Substitute("$0/$1", target_dir, plugin_name);

    if (FileUtils::check_exist(plugin_install_path)) {
        return Status::AlreadyExist(strings::Substitute("plugin $0 already install!", plugin_name));
    }

    if (!FileUtils::check_exist(target_dir)) {
        RETURN_IF_ERROR(FileUtils::create_dir(target_dir));
    }

    std::string zip_path = _source;
    if (!is_local_source(_source)) {
        zip_path = strings::Substitute("$0/.temp_$1_$2.zip", target_dir, GetCurrentTimeMicros(),
                                       plugin_name);
        _clean_paths.push_back(zip_path);

        RETURN_IF_ERROR(download(zip_path));
    }

    // zip extract
    ZipFile zip_file(zip_path);
    RETURN_IF_ERROR(zip_file.extract(target_dir, plugin_name));

    return Status::OK();
}

Status PluginZip::download(const std::string& zip_path) {
    // download .zip
    Status status;
    HttpClient client;
    Md5Digest digest;

    std::unique_ptr<WritableFile> file;

    RETURN_IF_ERROR(Env::Default()->new_writable_file(zip_path, &file));
    RETURN_IF_ERROR(client.init(_source));

    auto download_cb = [&status, &digest, &file](const void* data, size_t length) {
        digest.update(data, length);

        Slice slice((const char*)data, length);
        status = file->append(slice);
        if (!status.ok()) {
            LOG(WARNING) << "fail to download data, file: " << file->filename()
                         << ", error: " << status.to_string();
            return false;
        }

        return true;
    };

    RETURN_IF_ERROR(client.execute(download_cb));
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(file->flush(WritableFile::FLUSH_ASYNC));
    RETURN_IF_ERROR(file->sync());
    RETURN_IF_ERROR(file->close());

    // md5 check
    HttpClient md5_client;
    RETURN_IF_ERROR(md5_client.init(_source + ".md5"));

    std::string expect;
    auto download_md5_cb = [&status, &expect](const void* data, size_t length) {
        expect = std::string((const char*)data, length);
        return true;
    };

    RETURN_IF_ERROR(md5_client.execute(download_md5_cb));

    digest.digest();
    if (0 != strncmp(digest.hex().c_str(), expect.c_str(), 32)) {
        return Status::InternalError(strings::Substitute(
                "plugin install checksum failed. expect: $0, actual:$1", expect, digest.hex()));
    }

    return Status::OK();
}

} // namespace doris
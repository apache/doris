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

#include <string>
#include <vector>

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
// TODO(ryan19929): Optimize the design of the Configuration class, remove duplicate configurations (like mode and lowercase)
class Configuration {
private:
    bool use_smart_;
    // TODO(ryan19929): delete config_->lower_case_, because it is always true(java version is same)
    bool enable_lowercase_;
    std::string dict_path_;

    struct DictFiles {
        std::string main {"main.dic"};
        std::string quantifier {"quantifier.dic"};
        std::string stopwords {"stopword.dic"};
    } dict_files_;

    std::vector<std::string> ext_dict_files_;
    std::vector<std::string> ext_stop_word_dict_files_;

public:
    Configuration(bool use_smart = true, bool enable_lowercase_ = true)
            : use_smart_(use_smart), enable_lowercase_(enable_lowercase_) {
        ext_dict_files_ = {"extra_main.dic", "extra_single_word.dic", "extra_single_word_full.dic",
                           "extra_single_word_low_freq.dic"};

        ext_stop_word_dict_files_ = {"extra_stopword.dic"};
    }

    bool isUseSmart() const { return use_smart_; }
    Configuration& setUseSmart(bool smart) {
        use_smart_ = smart;
        return *this;
    }

    bool isEnableLowercase() const { return enable_lowercase_; }
    Configuration& setEnableLowercase(bool enable) {
        enable_lowercase_ = enable;
        return *this;
    }

    std::string getDictPath() const { return dict_path_; }
    Configuration& setDictPath(const std::string& path) {
        dict_path_ = path;
        return *this;
    }

    void setMainDictFile(const std::string& file) { dict_files_.main = file; }
    void setQuantifierDictFile(const std::string& file) { dict_files_.quantifier = file; }
    void setStopWordDictFile(const std::string& file) { dict_files_.stopwords = file; }

    const std::string& getMainDictFile() const { return dict_files_.main; }
    const std::string& getQuantifierDictFile() const { return dict_files_.quantifier; }
    const std::string& getStopWordDictFile() const { return dict_files_.stopwords; }

    void addExtDictFile(const std::string& filePath) { ext_dict_files_.push_back(filePath); }
    void addExtStopWordDictFile(const std::string& filePath) {
        ext_stop_word_dict_files_.push_back(filePath);
    }

    const std::vector<std::string>& getExtDictFiles() const { return ext_dict_files_; }
    const std::vector<std::string>& getExtStopWordDictFiles() const {
        return ext_stop_word_dict_files_;
    }
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2

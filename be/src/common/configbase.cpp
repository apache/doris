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

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <map>

#define __IN_CONFIGBASE_CPP__
#include "common/config.h"
#undef __IN_CONFIGBASE_CPP__

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "util/filesystem_util.h"

namespace doris {
namespace config {

std::map<std::string, Register::Field>* Register::_s_field_map = nullptr;
std::map<std::string, std::function<bool()>>* RegisterConfValidator::_s_field_validator = nullptr;
std::map<std::string, std::string>* full_conf_map = nullptr;

std::mutex custom_conf_lock;

std::mutex mutable_string_config_lock;

// trim string
std::string& trim(std::string& s) {
    // rtrim
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char c) { return !std::isspace(c); })
                    .base(),
            s.end());
    // ltrim
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isspace(c); }));
    return s;
}

// split string by '='
void splitkv(const std::string& s, std::string& k, std::string& v) {
    const char sep = '=';
    int start = 0;
    int end = 0;
    if ((end = s.find(sep, start)) != std::string::npos) {
        k = s.substr(start, end - start);
        v = s.substr(end + 1);
    } else {
        k = s;
        v = "";
    }
}

// replace env variables
bool replaceenv(std::string& s) {
    std::size_t pos = 0;
    std::size_t start = 0;
    while ((start = s.find("${", pos)) != std::string::npos) {
        std::size_t end = s.find("}", start + 2);
        if (end == std::string::npos) {
            return false;
        }
        std::string envkey = s.substr(start + 2, end - start - 2);
        const char* envval = std::getenv(envkey.c_str());
        if (envval == nullptr) {
            return false;
        }
        s.erase(start, end - start + 1);
        s.insert(start, envval);
        pos = start + strlen(envval);
    }
    return true;
}

bool strtox(const std::string& valstr, bool& retval);
bool strtox(const std::string& valstr, int16_t& retval);
bool strtox(const std::string& valstr, int32_t& retval);
bool strtox(const std::string& valstr, int64_t& retval);
bool strtox(const std::string& valstr, double& retval);
bool strtox(const std::string& valstr, std::string& retval);

template <typename T>
bool strtox(const std::string& valstr, std::vector<T>& retval) {
    std::stringstream ss(valstr);
    std::string item;
    T t;
    while (std::getline(ss, item, ',')) {
        if (!strtox(trim(item), t)) {
            return false;
        }
        retval.push_back(t);
    }
    return true;
}

bool strtox(const std::string& valstr, bool& retval) {
    if (valstr.compare("true") == 0) {
        retval = true;
    } else if (valstr.compare("false") == 0) {
        retval = false;
    } else {
        return false;
    }
    return true;
}

template <typename T>
bool strtointeger(const std::string& valstr, T& retval) {
    if (valstr.length() == 0) {
        return false; // empty-string is only allowed for string type.
    }
    char* end;
    errno = 0;
    const char* valcstr = valstr.c_str();
    int64_t ret64 = strtoll(valcstr, &end, 10);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false; // bad parse
    }
    T tmp = retval;
    retval = static_cast<T>(ret64);
    if (retval != ret64) {
        retval = tmp;
        return false;
    }
    return true;
}

bool strtox(const std::string& valstr, int16_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, int32_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, int64_t& retval) {
    return strtointeger(valstr, retval);
}

bool strtox(const std::string& valstr, double& retval) {
    if (valstr.length() == 0) {
        return false; // empty-string is only allowed for string type.
    }
    char* end = nullptr;
    errno = 0;
    const char* valcstr = valstr.c_str();
    retval = strtod(valcstr, &end);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false; // bad parse
    }
    return true;
}

bool strtox(const std::string& valstr, std::string& retval) {
    retval = valstr;
    return true;
}

template <typename T>
bool convert(const std::string& value, T& retval) {
    std::string valstr(value);
    trim(valstr);
    if (!replaceenv(valstr)) {
        return false;
    }
    return strtox(valstr, retval);
}

// load conf file
bool Properties::load(const char* conf_file, bool must_exist) {
    // if conf_file is null, use the empty props
    if (conf_file == nullptr) {
        return true;
    }

    // open the conf file
    std::ifstream input(conf_file);
    if (!input.is_open()) {
        if (must_exist) {
            std::cerr << "config::load() failed to open the file:" << conf_file << std::endl;
            return false;
        }
        return true;
    }

    // load properties
    std::string line;
    std::string key;
    std::string value;
    line.reserve(512);
    while (input) {
        // read one line at a time
        std::getline(input, line);

        // remove left and right spaces
        trim(line);

        // ignore comments
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // read key and value
        splitkv(line, key, value);
        trim(key);
        trim(value);

        // insert into file_conf_map
        file_conf_map[key] = value;
    }

    // close the conf file
    input.close();

    return true;
}

template <typename T>
bool Properties::get_or_default(const char* key, const char* defstr, T& retval,
                                bool* is_retval_set) const {
    const auto& it = file_conf_map.find(std::string(key));
    std::string valstr;
    if (it == file_conf_map.end()) {
        if (defstr == nullptr) {
            // Not found in conf map, and no default value need to be set, just return
            *is_retval_set = false;
            return true;
        } else {
            valstr = std::string(defstr);
        }
    } else {
        valstr = it->second;
    }
    *is_retval_set = true;
    return convert(valstr, retval);
}

void Properties::set(const std::string& key, const std::string& val) {
    file_conf_map.emplace(key, val);
}

void Properties::set_force(const std::string& key, const std::string& val) {
    file_conf_map[key] = val;
}

bool Properties::dump(const std::string& conffile) {
    std::vector<std::string> files = {conffile};
    Status st = FileSystemUtil::remove_paths(files);
    if (!st.ok()) {
        return false;
    }
    st = FileSystemUtil::create_file(conffile);
    if (!st.ok()) {
        return false;
    }

    std::ofstream out(conffile);
    out << "# THIS IS AN AUTO GENERATED CONFIG FILE.\n";
    out << "# You can modify this file manually, and the configurations in this file\n";
    out << "# will overwrite the configurations in be.conf\n";
    out << "\n";

    for (auto const& iter : file_conf_map) {
        out << iter.first << " = " << iter.second << "\n";
    }
    out.close();
    return true;
}

template <typename T>
std::ostream& operator<<(std::ostream& out, const std::vector<T>& v) {
    size_t last = v.size() - 1;
    for (size_t i = 0; i < v.size(); ++i) {
        out << v[i];
        if (i != last) {
            out << ", ";
        }
    }
    return out;
}

#define SET_FIELD(FIELD, TYPE, FILL_CONF_MAP, SET_TO_DEFAULT)                                  \
    if (strcmp((FIELD).type, #TYPE) == 0) {                                                    \
        TYPE new_value = TYPE();                                                               \
        bool is_newval_set = false;                                                            \
        if (!props.get_or_default((FIELD).name, ((SET_TO_DEFAULT) ? (FIELD).defval : nullptr), \
                                  new_value, &is_newval_set)) {                                \
            std::cerr << "config field error: " << (FIELD).name << std::endl;                  \
            return false;                                                                      \
        }                                                                                      \
        if (!is_newval_set) {                                                                  \
            continue;                                                                          \
        }                                                                                      \
        TYPE& ref_conf_value = *reinterpret_cast<TYPE*>((FIELD).storage);                      \
        TYPE old_value = ref_conf_value;                                                       \
        ref_conf_value = new_value;                                                            \
        if (RegisterConfValidator::_s_field_validator != nullptr) {                            \
            auto validator = RegisterConfValidator::_s_field_validator->find((FIELD).name);    \
            if (validator != RegisterConfValidator::_s_field_validator->end() &&               \
                !(validator->second)()) {                                                      \
                ref_conf_value = old_value;                                                    \
                std::cerr << "validate " << (FIELD).name << "=" << new_value << " failed"      \
                          << std::endl;                                                        \
                return false;                                                                  \
            }                                                                                  \
        }                                                                                      \
        if (FILL_CONF_MAP) {                                                                   \
            std::ostringstream oss;                                                            \
            oss << ref_conf_value;                                                             \
            (*full_conf_map)[(FIELD).name] = oss.str();                                        \
        }                                                                                      \
        continue;                                                                              \
    }

// init conf fields
bool init(const char* conf_file, bool fill_conf_map, bool must_exist, bool set_to_default) {
    Properties props;
    // load properties file
    if (!props.load(conf_file, must_exist)) {
        return false;
    }
    // fill full_conf_map ?
    if (fill_conf_map && full_conf_map == nullptr) {
        full_conf_map = new std::map<std::string, std::string>();
    }

    // set conf fields
    for (const auto& it : *Register::_s_field_map) {
        SET_FIELD(it.second, bool, fill_conf_map, set_to_default);
        SET_FIELD(it.second, int16_t, fill_conf_map, set_to_default);
        SET_FIELD(it.second, int32_t, fill_conf_map, set_to_default);
        SET_FIELD(it.second, int64_t, fill_conf_map, set_to_default);
        SET_FIELD(it.second, double, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::string, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<bool>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<int16_t>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<int32_t>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<int64_t>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<double>, fill_conf_map, set_to_default);
        SET_FIELD(it.second, std::vector<std::string>, fill_conf_map, set_to_default);
    }

    return true;
}

#define UPDATE_FIELD(FIELD, VALUE, TYPE, PERSIST)                                                 \
    if (strcmp((FIELD).type, #TYPE) == 0) {                                                       \
        TYPE new_value;                                                                           \
        if (!convert((VALUE), new_value)) {                                                       \
            return Status::InvalidArgument("convert '{}' as {} failed", VALUE, #TYPE);            \
        }                                                                                         \
        TYPE& ref_conf_value = *reinterpret_cast<TYPE*>((FIELD).storage);                         \
        TYPE old_value = ref_conf_value;                                                          \
        if (RegisterConfValidator::_s_field_validator != nullptr) {                               \
            auto validator = RegisterConfValidator::_s_field_validator->find((FIELD).name);       \
            if (validator != RegisterConfValidator::_s_field_validator->end() &&                  \
                !(validator->second)()) {                                                         \
                ref_conf_value = old_value;                                                       \
                return Status::InvalidArgument("validate {}={} failed", (FIELD).name, new_value); \
            }                                                                                     \
        }                                                                                         \
        ref_conf_value = new_value;                                                               \
        if (full_conf_map != nullptr) {                                                           \
            std::ostringstream oss;                                                               \
            oss << new_value;                                                                     \
            (*full_conf_map)[(FIELD).name] = oss.str();                                           \
        }                                                                                         \
        if (PERSIST) {                                                                            \
            persist_config(std::string((FIELD).name), VALUE);                                     \
        }                                                                                         \
        return Status::OK();                                                                      \
    }

// write config to be_custom.conf
// the caller need to make sure that the given config is valid
bool persist_config(const std::string& field, const std::string& value) {
    // lock to make sure only one thread can modify the be_custom.conf
    std::lock_guard<std::mutex> l(custom_conf_lock);

    static const string conffile = string(getenv("DORIS_HOME")) + "/conf/be_custom.conf";

    Properties tmp_props;
    if (!tmp_props.load(conffile.c_str(), false)) {
        LOG(WARNING) << "failed to load " << conffile;
        return false;
    }

    tmp_props.set_force(field, value);
    return tmp_props.dump(conffile);
}

Status set_config(const std::string& field, const std::string& value, bool need_persist,
                  bool force) {
    auto it = Register::_s_field_map->find(field);
    if (it == Register::_s_field_map->end()) {
        return Status::NotFound("'{}' is not found", field);
    }

    if (!force && !it->second.valmutable) {
        return Status::NotSupported("'{}' is not support to modify", field);
    }

    UPDATE_FIELD(it->second, value, bool, need_persist);
    UPDATE_FIELD(it->second, value, int16_t, need_persist);
    UPDATE_FIELD(it->second, value, int32_t, need_persist);
    UPDATE_FIELD(it->second, value, int64_t, need_persist);
    UPDATE_FIELD(it->second, value, double, need_persist);
    {
        // add lock to ensure thread safe
        std::lock_guard<std::mutex> lock(mutable_string_config_lock);
        UPDATE_FIELD(it->second, value, std::string, need_persist);
    }

    // The other types are not thread safe to change dynamically.
    return Status::NotSupported("'{}' is type of '{}' which is not support to modify", field,
                                it->second.type);
}

Status set_fuzzy_config(const std::string& field, const std::string& value) {
    return set_config(field, value, false, true);
}

void set_fuzzy_configs() {
    // random value true or false
    Status s =
            set_fuzzy_config("disable_storage_page_cache", ((rand() % 2) == 0) ? "true" : "false");
    LOG(INFO) << s.to_string();
    // random value from 8 to 48
    // s = set_fuzzy_config("doris_scanner_thread_pool_thread_num", std::to_string((rand() % 41) + 8));
    // LOG(INFO) << s.to_string();
}

std::mutex* get_mutable_string_config_lock() {
    return &mutable_string_config_lock;
}

std::vector<std::vector<std::string>> get_config_info() {
    std::vector<std::vector<std::string>> configs;
    std::lock_guard<std::mutex> lock(mutable_string_config_lock);
    for (const auto& it : *full_conf_map) {
        auto field_it = Register::_s_field_map->find(it.first);
        if (field_it == Register::_s_field_map->end()) {
            continue;
        }

        std::vector<std::string> _config;
        _config.push_back(it.first);

        _config.push_back(field_it->second.type);
        _config.push_back(it.second);
        _config.push_back(field_it->second.valmutable ? "true" : "false");

        configs.push_back(_config);
    }
    return configs;
}

} // namespace config
} // namespace doris

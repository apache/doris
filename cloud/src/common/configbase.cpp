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

#include <fmt/core.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <utility>

#define __IN_CONFIGBASE_CPP__
#include "common/config.h"
#undef __IN_CONFIGBASE_CPP__

namespace doris::cloud::config {

std::map<std::string, Register::Field>* Register::_s_field_map = nullptr;
std::map<std::string, std::function<bool()>>* RegisterConfValidator::_s_field_validator = nullptr;
std::map<std::string, std::string>* full_conf_map = nullptr;
std::shared_mutex mutable_string_config_lock;
std::mutex conf_persist_lock;

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
bool replaceenv(std::string key, std::string& s) {
    size_t pos = 0;
    bool modified = false;

    while (pos < s.size()) {
        size_t start = s.find('$', pos);
        if (start == std::string::npos) break;

        if (start > 0 && s[start - 1] == '$') {
            s.erase(start - 1, 1);
            pos = start;
            modified = true;
            continue;
        }

        bool has_brace = false;
        size_t var_start = start + 1;
        size_t end = std::string::npos;

        // format: ${VAR}
        if (var_start < s.size() && s[var_start] == '{') {
            has_brace = true;
            var_start++;
            end = s.find('}', var_start);
            if (end == std::string::npos) return false;
        } else {
            // format: $VAR
            if (var_start >= s.size()) {
                pos = var_start;
                continue;
            }
            char first_char = s[var_start];
            if (!std::isalnum(first_char) && first_char != '_') {
                pos = var_start;
                continue;
            }
            end = var_start;
            while (end < s.size() && (std::isalnum(s[end]) || s[end] == '_')) {
                end++;
            }
        }

        std::string envkey = s.substr(var_start, end - var_start);
        const char* envval = std::getenv(envkey.c_str());
        if (envval == nullptr) return false;
        // replace env
        size_t replace_start = start;
        size_t replace_length = has_brace ? (end - start + 1) : (end - start);
        s.erase(replace_start, replace_length);
        s.insert(replace_start, envval);
        // reset
        pos = replace_start + strlen(envval);
        modified = true;
    }

    if (modified) {
        std::cout << "replace env conf " << key << "=" << s << std::endl;
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
bool convert(const std::string key, const std::string& value, T& retval) {
    std::string valstr(value);
    trim(valstr);
    if (!replaceenv(key, valstr)) {
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

bool Properties::dump(const std::string& conffile) {
    std::string conffile_tmp = conffile + ".tmp";

    if (std::filesystem::exists(conffile)) {
        // copy for modify
        std::ifstream in(conffile, std::ios::binary);
        std::ofstream out(conffile_tmp, std::ios::binary);
        out << in.rdbuf();
        in.close();
        out.close();
    }

    std::ofstream file_writer;

    file_writer.open(conffile_tmp, std::ios::out | std::ios::app);

    file_writer << std::endl;

    for (auto const& iter : file_conf_map) {
        file_writer << iter.first << " = " << iter.second << std::endl;
    }

    file_writer.close();
    if (!file_writer.good()) {
        return false;
    }

    std::filesystem::rename(conffile_tmp, conffile);
    return std::filesystem::exists(conffile);
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
    return convert(std::string(key), valstr, retval);
}

void Properties::set(const std::string& key, const std::string& val) {
    file_conf_map.emplace(key, val);
}

void Properties::set_force(const std::string& key, const std::string& val) {
    file_conf_map[key] = val;
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

#define UPDATE_FIELD(FIELD, VALUE, TYPE, PERSIST)                                           \
    if (strcmp((FIELD).type, #TYPE) == 0) {                                                 \
        TYPE new_value;                                                                     \
        if (!convert((FIELD).name, (VALUE), new_value)) {                                   \
            std::cerr << "convert " << VALUE << "as" << #TYPE << "failed";                  \
            return false;                                                                   \
        }                                                                                   \
        TYPE& ref_conf_value = *reinterpret_cast<TYPE*>((FIELD).storage);                   \
        TYPE old_value = ref_conf_value;                                                    \
        if (RegisterConfValidator::_s_field_validator != nullptr) {                         \
            auto validator = RegisterConfValidator::_s_field_validator->find((FIELD).name); \
            if (validator != RegisterConfValidator::_s_field_validator->end() &&            \
                !(validator->second)()) {                                                   \
                ref_conf_value = old_value;                                                 \
                std::cerr << "validate " << (FIELD).name << "=" << new_value << " failed"   \
                          << std::endl;                                                     \
                return false;                                                               \
            }                                                                               \
        }                                                                                   \
        ref_conf_value = new_value;                                                         \
        if (full_conf_map != nullptr) {                                                     \
            std::ostringstream oss;                                                         \
            oss << new_value;                                                               \
            (*full_conf_map)[(FIELD).name] = oss.str();                                     \
        }                                                                                   \
        if (PERSIST) {                                                                      \
            props.set_force(std::string((FIELD).name), VALUE);                              \
        }                                                                                   \
        return true;                                                                        \
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

bool do_set_config(const Register::Field& feild, const std::string& value, bool need_persist,
                   Properties& props) {
    UPDATE_FIELD(feild, value, bool, need_persist);
    UPDATE_FIELD(feild, value, int16_t, need_persist);
    UPDATE_FIELD(feild, value, int32_t, need_persist);
    UPDATE_FIELD(feild, value, int64_t, need_persist);
    UPDATE_FIELD(feild, value, double, need_persist);
    {
        // add lock to ensure thread safe
        std::unique_lock<std::shared_mutex> lock(mutable_string_config_lock);
        UPDATE_FIELD(feild, value, std::string, need_persist);
    }
    return false;
}

std::pair<bool, std::string> set_config(const std::string& field, const std::string& value,
                                        bool need_persist, Properties& props) {
    auto it = Register::_s_field_map->find(field);
    if (it == Register::_s_field_map->end()) {
        return {false, fmt::format("config field={} not exists", field)};
    }
    if (!it->second.valmutable) {
        return {false, fmt::format("config field={} is immutable", field)};
    }

    if (!do_set_config(it->second, value, need_persist, props)) {
        return {false, fmt::format("not supported to modify field={}, value={}", field, value)};
    }
    return {true, {}};
}

std::pair<bool, std::string> set_config(std::unordered_map<std::string, std::string> field_map,
                                        bool need_persist, const std::string& custom_conf_path) {
    Properties props;
    auto set_conf_closure = [&]() -> std::pair<bool, std::string> {
        for (const auto& [field, value] : field_map) {
            if (auto [succ, cause] = set_config(field, value, need_persist, props); !succ) {
                return {false, std::move(cause)};
            }
        }
        return {true, {}};
    };

    if (!need_persist) {
        return set_conf_closure();
    }

    // lock to make sure only one thread can modify the conf file
    std::lock_guard<std::mutex> l(conf_persist_lock);
    auto [succ, cause] = set_conf_closure();
    if (!succ) {
        return {succ, std::move(cause)};
    }
    if (props.dump(custom_conf_path)) {
        return {true, {}};
    }
    return {false, fmt::format("dump config modification to custom_conf_path={} "
                               "failed, plz check config::custom_conf_path and io status",
                               custom_conf_path)};
}

std::shared_mutex* get_mutable_string_config_lock() {
    return &mutable_string_config_lock;
}
} // namespace doris::cloud::config

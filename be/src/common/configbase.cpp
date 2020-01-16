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
#include <sstream>

#define __IN_CONFIGBASE_CPP__ 
#include "common/config.h"
#undef  __IN_CONFIGBASE_CPP__

namespace doris {
namespace config {

std::list<Register::Field>* Register::_s_fieldlist = NULL;
std::map<std::string, std::string>* confmap = NULL;

Properties props;

// load conf file
bool Properties::load(const char* filename) {
    // if filename is null, use the empty props
    if (filename == 0) {
        return true;
    }

    // open the conf file
    std::ifstream input(filename);
    if (!input.is_open()) {
        std::cerr <<  "config::load() failed to open the file:" << filename << std::endl;
        return false;
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
        
        // insert into propmap
        propmap[key] = value;
    }

    // close the conf file
    input.close();

    return true;
}

template <typename T>
bool Properties::get(const char* key, const char* defstr, T& retval) const {
    std::map<std::string, std::string>::const_iterator it = propmap.find(std::string(key));
    std::string valstr = it != propmap.end() ? it->second: std::string(defstr);
    trim(valstr);
    if (!replaceenv(valstr)) { 
        return false;
    }
    return strtox(valstr, retval);
}

template <typename T>
bool Properties::strtox(const std::string& valstr, std::vector<T>& retval) {
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

const std::map<std::string, std::string>& Properties::getmap() const {
    return propmap;
}

// trim string
std::string& Properties::trim(std::string& s)  {
    // rtrim
    s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
    // ltrim
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
}

// split string by '='
void Properties::splitkv(const std::string& s, std::string& k, std::string& v) {
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
bool Properties::replaceenv(std::string& s) {
    std::size_t pos = 0;
    std::size_t start = 0;
    while ((start = s.find("${", pos)) != std::string::npos) {
        std::size_t end = s.find("}", start + 2);
        if (end == std::string::npos) { 
            return false;
        }
        std::string envkey = s.substr(start + 2, end - start - 2);
        const char* envval = std::getenv(envkey.c_str());
        if (envval == NULL) {
            return false;
        }
        s.erase(start, end - start + 1);
        s.insert(start, envval);
        pos = start + strlen(envval);
    }
    return true;
}

bool Properties::strtox(const std::string& valstr, bool& retval) {
    if (valstr.compare("true") == 0) {
        retval = true;
    } else if (valstr.compare("false") == 0) {
        retval = false;
    } else {
        return false;
    }
    return true;
}

template<typename T>
bool Properties::strtointeger(const std::string& valstr, T& retval) {
    if (valstr.length() == 0)  { 
        return false; // empty-string is only allowed for string type.
    }
    char* end;
    errno = 0;
    const char* valcstr = valstr.c_str();
    int64_t ret64 = strtoll(valcstr, &end, 10);
    if (errno || end != valcstr + strlen(valcstr)) {
        return false;  // bad parse
    }
    retval = static_cast<T>(ret64);
    if (retval != ret64) {
        return false;
    }
    return true;
}

bool Properties::strtox(const std::string& valstr, int16_t& retval) {
    return strtointeger(valstr, retval);
}

bool Properties::strtox(const std::string& valstr, int32_t& retval) {
    return strtointeger(valstr, retval);
}

bool Properties::strtox(const std::string& valstr, int64_t& retval) {
    return strtointeger(valstr, retval);
}

bool Properties::strtox(const std::string& valstr, double& retval) {
    if (valstr.length() == 0)  {
        return false; // empty-string is only allowed for string type.
    }
    char* end = NULL;
    errno = 0;
    const char* valcstr = valstr.c_str();
    retval = strtod(valcstr, &end);
    if (errno || end != valcstr + strlen(valcstr))  {
        return false;  // bad parse
    }
    return true;
}

bool Properties::strtox(const std::string& valstr, std::string& retval) {
    retval = valstr;
    return true;
}

template<typename T>
std::ostream& operator<< (std::ostream& out, const std::vector<T>& v) {
    size_t last = v.size() - 1;
    for (size_t i = 0; i < v.size(); ++i) {
        out << v[i];
        if (i != last) {
            out << ", ";
        }
    }
    return out;
}

#define SET_FIELD(FIELD, TYPE, FILL_CONFMAP)\
        if (strcmp((FIELD).type, #TYPE) == 0) {\
            if (!props.get((FIELD).name, (FIELD).defval, *reinterpret_cast<TYPE*>((FIELD).storage))) {\
                std::cerr << "config field error: " << (FIELD).name << std::endl;\
                return false;\
            }\
            if (FILL_CONFMAP) {\
                std::ostringstream oss;\
                oss << (*reinterpret_cast<TYPE*>((FIELD).storage));\
                (*confmap)[(FIELD).name] = oss.str();\
            }\
            continue;\
        }

// init conf fields
bool init(const char* filename, bool fillconfmap) {
    // load properties file
    if (!props.load(filename)) { 
        return false;
    }
    // fill confmap ?
    if (fillconfmap && confmap == NULL) {
        confmap = new std::map<std::string, std::string>();
    }

    // set conf fields
    for (std::list<Register::Field>::iterator it = Register::_s_fieldlist->begin(); 
        it != Register::_s_fieldlist->end(); ++it) {
        SET_FIELD(*it, bool, fillconfmap);
        SET_FIELD(*it, int16_t, fillconfmap);
        SET_FIELD(*it, int32_t, fillconfmap);
        SET_FIELD(*it, int64_t, fillconfmap);
        SET_FIELD(*it, double, fillconfmap);
        SET_FIELD(*it, std::string, fillconfmap);
        SET_FIELD(*it, std::vector<bool>, fillconfmap);
        SET_FIELD(*it, std::vector<int16_t>, fillconfmap);
        SET_FIELD(*it, std::vector<int32_t>, fillconfmap);
        SET_FIELD(*it, std::vector<int64_t>, fillconfmap);
        SET_FIELD(*it, std::vector<double>, fillconfmap);
        SET_FIELD(*it, std::vector<std::string>, fillconfmap);
    }

    return true;
}

} // namespace config
} // namespace doris

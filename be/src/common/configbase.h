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

#ifndef DORIS_BE_SRC_COMMON_CONFIGBASE_H
#define DORIS_BE_SRC_COMMON_CONFIGBASE_H

#include <stdint.h>
#include <list>
#include <map>
#include <vector>

namespace doris {
namespace config {

class Register {
public:
    struct Field {
        const char* type;
        const char* name;
        void* storage;
        const char* defval;
        Field(const char* ftype, const char* fname, void* fstorage, const char* fdefval) :
            type(ftype), 
            name(fname),
            storage(fstorage),
            defval(fdefval) {}
    };

public:
    static std::list<Field>* _s_fieldlist;

public:
    Register(const char* ftype, const char* fname, void* fstorage, const char* fdefval) {
        if (_s_fieldlist == NULL) {
            _s_fieldlist = new std::list<Field>();
        }
        Field field(ftype, fname, fstorage, fdefval);
        _s_fieldlist->push_back(field);
    }
}; 

#define DEFINE_FIELD(FIELD_TYPE, FIELD_NAME, FIELD_DEFAULT)\
    FIELD_TYPE FIELD_NAME;\
    static Register reg_##FIELD_NAME(#FIELD_TYPE, #FIELD_NAME, &FIELD_NAME, FIELD_DEFAULT);

#define DECLARE_FIELD(FIELD_TYPE, FIELD_NAME)   extern FIELD_TYPE FIELD_NAME;

#ifdef __IN_CONFIGBASE_CPP__ 
#define CONF_Bool(name, defaultstr)         DEFINE_FIELD(bool, name, defaultstr)
#define CONF_Int16(name, defaultstr)        DEFINE_FIELD(int16_t, name, defaultstr)
#define CONF_Int32(name, defaultstr)        DEFINE_FIELD(int32_t, name, defaultstr)
#define CONF_Int64(name, defaultstr)        DEFINE_FIELD(int64_t, name, defaultstr)
#define CONF_Double(name, defaultstr)       DEFINE_FIELD(double, name, defaultstr)
#define CONF_String(name, defaultstr)       DEFINE_FIELD(std::string, name, defaultstr)
#define CONF_Bools(name, defaultstr)        DEFINE_FIELD(std::vector<bool>, name, defaultstr)
#define CONF_Int16s(name, defaultstr)       DEFINE_FIELD(std::vector<int16_t>, name, defaultstr)
#define CONF_Int32s(name, defaultstr)       DEFINE_FIELD(std::vector<int32_t>, name, defaultstr)
#define CONF_Int64s(name, defaultstr)       DEFINE_FIELD(std::vector<int64_t>, name, defaultstr)
#define CONF_Doubles(name, defaultstr)      DEFINE_FIELD(std::vector<double>, name, defaultstr)
#define CONF_Strings(name, defaultstr)      DEFINE_FIELD(std::vector<std::string>, name, defaultstr)
#else
#define CONF_Bool(name, defaultstr)         DECLARE_FIELD(bool, name)
#define CONF_Int16(name, defaultstr)        DECLARE_FIELD(int16_t, name)
#define CONF_Int32(name, defaultstr)        DECLARE_FIELD(int32_t, name)
#define CONF_Int64(name, defaultstr)        DECLARE_FIELD(int64_t, name)
#define CONF_Double(name, defaultstr)       DECLARE_FIELD(double, name)
#define CONF_String(name, defaultstr)       DECLARE_FIELD(std::string, name)
#define CONF_Bools(name, defaultstr)        DECLARE_FIELD(std::vector<bool>, name)
#define CONF_Int16s(name, defaultstr)       DECLARE_FIELD(std::vector<int16_t>, name)
#define CONF_Int32s(name, defaultstr)       DECLARE_FIELD(std::vector<int32_t>, name)
#define CONF_Int64s(name, defaultstr)       DECLARE_FIELD(std::vector<int64_t>, name)
#define CONF_Doubles(name, defaultstr)      DECLARE_FIELD(std::vector<double>, name)
#define CONF_Strings(name, defaultstr)      DECLARE_FIELD(std::vector<std::string>, name)
#endif

class Properties {
public:
    bool load(const char* filename);
    template<typename T> 
    bool get(const char* key, const char* defstr, T& retval) const;
    const std::map<std::string, std::string>& getmap() const;

private:
    template <typename T> 
    static bool strtox(const std::string& valstr, std::vector<T>& retval);
    template<typename T> 
    static bool strtointeger(const std::string& valstr, T& retval);
    static bool strtox(const std::string& valstr, bool& retval);
    static bool strtox(const std::string& valstr, int16_t& retval);
    static bool strtox(const std::string& valstr, int32_t& retval);
    static bool strtox(const std::string& valstr, int64_t& retval);
    static bool strtox(const std::string& valstr, double& retval);
    static bool strtox(const std::string& valstr, std::string& retval);
    static std::string& trim(std::string& s);
    static void splitkv(const std::string& s, std::string& k, std::string& v);
    static bool replaceenv(std::string& s);

private:
    std::map<std::string, std::string> propmap;
};

extern Properties props;

extern std::map<std::string, std::string>* confmap;

bool init(const char* filename, bool fillconfmap = false);

} // namespace config
} // namespace doris

#endif // DORIS_BE_SRC_COMMON_CONFIGBASE_H

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

#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

static_assert(__cplusplus >= 201701, "compiler need to support c++17 or newer");

namespace doris::cloud {

/**
 * This class implements arguments parser, it accepts both short '-arg' and
 * long '--argument' arguments
 *
 * simple::ArgParser parser(
 *   { // define args that will be used in program
 *     //                              arg_name  default_value  comment
 *     ArgParser::new_arg<bool>       ("verion"  , false   ,    "print version or not")            ,
 *     ArgParser::new_arg<long>       ("duration", 10086   ,    "duration for the test in seconds"),
 *     ArgParser::new_arg<std::string>("flags"   , "abcdef",    "flags for testing")               ,
 *     ArgParser::new_arg<double>     ("pi"      , 3.1415  ,    "pi")                              ,
 *   },
 *   true // skip unknown args if there is any
 * );
 *
 * // parse/update from args
 * parser.parse(argc, argv);
 * // to use args, types must be specified when access args
 * if (parser.get<bool>(version)) print_version();
 * ...
 *
 */
class ArgParser {
public:
    using arg_map_t =
            std::map<std::string,
                     std::tuple<std::variant<std::string, long, double, bool>, std::string>>;
    using arg_t = arg_map_t::value_type;

    /**
   * Create a key-value pair of argument, used in initialization
   * TODO: template arg pack for extension
   *
   * @param name argument name
   * @val argument value, for default
   * @comment comment for the argument
   * @return an arg_t, internal element of arg_map_t
   */
    template <typename T>
    static arg_t new_arg(const std::string& name, const T& val, const std::string& comment = "") {
        compile_time_check_type<T>();
        return arg_t {name, {T {val}, comment}};
    }

    /**
   * Print given args
   */
    static void print(const arg_map_t& args) {
        std::cout << "args: ";
        for (const auto& i : args) {
            std::cout << "{" << i.first << "=";
            if (std::get_if<std::string>(&std::get<0>(i.second))) {
                std::cout << std::get<std::string>(std::get<0>(i.second));
            } else if (std::get_if<bool>(&std::get<0>(i.second))) {
                std::cout << (std::get<bool>(std::get<0>(i.second)) ? "true" : "false");
            } else if (std::get_if<double>(&std::get<0>(i.second))) {
                std::cout << std::get<double>(std::get<0>(i.second));
            } else if (std::get_if<long>(&std::get<0>(i.second))) {
                std::cout << std::get<long>(std::get<0>(i.second));
            } else { // not defined type
                std::cout << "unknown type of " << i.first;
            }
            std::cout << "; " << std::get<1>(i.second) << "}, ";
        }
        std::cout << std::endl;
    }

    template <typename T>
    constexpr static void compile_time_check_type() {
        static_assert(std::is_same_v<std::string, T> || std::is_same_v<long, T> ||
                              std::is_same_v<double, T> || std::is_same_v<bool, T>,
                      "only std::string, long, double, and bool are allowed");
    }

    /**
   * Initialize and declare arguments
   *
   * @param args arguments list that predefined
   * @param skip_unknown_arg skip the unknown arguments when parse
   */
    ArgParser(const arg_map_t& args = {}, bool skip_unknown_arg = false)
            : args_(args), skip_unknown_(skip_unknown_arg) {}

    /**
   * Parse arguments
   *
   * @param argc number of arguments to parse
   * @param argv arguments to parse,
   *             e.g. ["--argument=abc", "-v", "-has_xxx=false"]
   * @out parse result, if nullptr, internal container will be used
   *      if out == nullptr and internal container is not initialized
   *      call to this function does not make sense
   * @return error msg that encountered
   */
    std::string parse(int argc, char const* const* argv, arg_map_t* out = nullptr) {
        arg_map_t ret;
        // copy
        if (out != nullptr)
            ret = *out;
        else
            ret = args_;

        std::string msg;
        std::vector<std::string> args;
        args.reserve(argc);
        for (int i = 0; i < argc; ++i) {
            args.emplace_back(argv[i]);
            auto& arg = args.back();
            auto eq = arg.find('=');
            // process boolean flags first
            if (eq == std::string::npos) { // "--version" || "-version" || "--help"
                auto k = arg.substr(1);
                auto it = ret.find(k) == ret.end() ? ret.find(k.substr(1)) : ret.find(k);
                if (it != ret.end() && std::get_if<bool>(&std::get<0>(it->second))) {
                    std::get<0>(it->second) = true;
                    continue;
                }
            }
            if (arg.size() < 4 || arg[0] != '-' || arg[2] == '-' || eq == std::string::npos) {
                // std::cerr << "invalid arg: " << arg << std::endl;
                msg += "invalid arg: " + arg + "; ";
                if (skip_unknown_)
                    continue;
                else
                    return msg;
            }
            std::string k = arg.substr(1 + (arg[1] == '-'), eq - 1 - (arg[1] == '-'));
            auto p = ret.find(k);

            if (p == ret.end()) { // no such an arg
                msg += "arg not supported: " + k + "; ";
                if (skip_unknown_)
                    continue;
                else
                    return msg;
            }

            std::string v = arg.substr(eq + 1);
            // std::cout << arg << ", k: " << k << ", v: " << v << std::endl;
            if (std::get_if<std::string>(&std::get<0>(p->second))) {
                std::get<0>(p->second) = v;
            } else if (std::get_if<bool>(&std::get<0>(p->second))) {
                if (v[0] >= '0' && v[0] <= '9') {
                    std::get<0>(p->second) = !!(std::stol(v));
                } else if (v.find("true") != std::string::npos && v.size() == 4) {
                    std::get<0>(p->second) = true;
                } else if (v.find("false") != std::string::npos && v.size() == 5) {
                    std::get<0>(p->second) = false;
                } else {
                    msg += "invalid arg " + arg + ", it should true or false. ";
                    if (skip_unknown_)
                        continue;
                    else
                        return msg;
                }
            } else { // number
                std::get<0>(p->second) = std::stol(v);
            }
        }

        if (out != nullptr)
            *out = std::move(ret);
        else
            args_ = std::move(ret);

        return msg;
    }

    auto& args() { return args_; }

    void print() { print(args_); }

    // std::remove_cvref_t is available since c++2a
    template <typename T>
    auto get(const std::string& name,
             const std::remove_reference_t<std::remove_cv_t<T>>& default_ = {}) {
        typedef std::remove_reference_t<std::remove_cv_t<T>> U;
        compile_time_check_type<U>();
        auto it = args_.find(name);
        // avoid invalid cast
        if (it == args_.end() || !std::get_if<U>(&std::get<0>(it->second))) {
            return default_;
        }
        return std::get<U>(std::get<0>(it->second));
    }

private:
    arg_map_t args_;
    bool skip_unknown_;
};

} // namespace doris::cloud

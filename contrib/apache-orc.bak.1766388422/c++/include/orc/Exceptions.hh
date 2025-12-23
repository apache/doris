/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_EXCEPTIONS_HH
#define ORC_EXCEPTIONS_HH

#include "orc/orc-config.hh"

#include <stdexcept>
#include <string>

namespace orc {

  class NotImplementedYet : public std::logic_error {
   public:
    explicit NotImplementedYet(const std::string& what_arg);
    explicit NotImplementedYet(const char* what_arg);
    ~NotImplementedYet() noexcept override;
    NotImplementedYet(const NotImplementedYet&);

   private:
    NotImplementedYet& operator=(const NotImplementedYet&);
  };

  class ParseError : public std::runtime_error {
   public:
    explicit ParseError(const std::string& what_arg);
    explicit ParseError(const char* what_arg);
    ~ParseError() noexcept override;
    ParseError(const ParseError&);

   private:
    ParseError& operator=(const ParseError&);
  };

  class InvalidArgument : public std::runtime_error {
   public:
    explicit InvalidArgument(const std::string& what_arg);
    explicit InvalidArgument(const char* what_arg);
    ~InvalidArgument() noexcept override;
    InvalidArgument(const InvalidArgument&);

   private:
    InvalidArgument& operator=(const InvalidArgument&);
  };

  class SchemaEvolutionError : public std::logic_error {
   public:
    explicit SchemaEvolutionError(const std::string& what_arg);
    explicit SchemaEvolutionError(const char* what_arg);
    virtual ~SchemaEvolutionError() noexcept override;
    SchemaEvolutionError(const SchemaEvolutionError&);
    SchemaEvolutionError& operator=(const SchemaEvolutionError&) = delete;
  };
}  // namespace orc

#endif

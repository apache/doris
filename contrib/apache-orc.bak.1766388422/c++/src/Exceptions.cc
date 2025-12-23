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

#include "orc/Exceptions.hh"

namespace orc {

  NotImplementedYet::NotImplementedYet(const std::string& what_arg) : logic_error(what_arg) {
    // PASS
  }

  NotImplementedYet::NotImplementedYet(const char* what_arg) : logic_error(what_arg) {
    // PASS
  }

  NotImplementedYet::NotImplementedYet(const NotImplementedYet& error) : logic_error(error) {
    // PASS
  }

  NotImplementedYet::~NotImplementedYet() noexcept {
    // PASS
  }

  ParseError::ParseError(const std::string& what_arg) : runtime_error(what_arg) {
    // PASS
  }

  ParseError::ParseError(const char* what_arg) : runtime_error(what_arg) {
    // PASS
  }

  ParseError::ParseError(const ParseError& error) : runtime_error(error) {
    // PASS
  }

  ParseError::~ParseError() noexcept {
    // PASS
  }

  InvalidArgument::InvalidArgument(const std::string& what_arg) : runtime_error(what_arg) {
    // PASS
  }

  InvalidArgument::InvalidArgument(const char* what_arg) : runtime_error(what_arg) {
    // PASS
  }

  InvalidArgument::InvalidArgument(const InvalidArgument& error) : runtime_error(error) {
    // PASS
  }

  InvalidArgument::~InvalidArgument() noexcept {
    // PASS
  }

  SchemaEvolutionError::SchemaEvolutionError(const std::string& what_arg) : logic_error(what_arg) {
    // PASS
  }

  SchemaEvolutionError::SchemaEvolutionError(const char* what_arg) : logic_error(what_arg) {
    // PASS
  }

  SchemaEvolutionError::SchemaEvolutionError(const SchemaEvolutionError& error)
      : logic_error(error) {
    // PASS
  }

  SchemaEvolutionError::~SchemaEvolutionError() noexcept {
    // PASS
  }
}  // namespace orc

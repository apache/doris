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

#include "orc/sargs/TruthValue.hh"

#include <stdexcept>

namespace orc {

  TruthValue operator||(TruthValue left, TruthValue right) {
    if (right == TruthValue::YES || left == TruthValue::YES) {
      return TruthValue::YES;
    }
    if (right == TruthValue::YES_NULL || left == TruthValue::YES_NULL) {
      return TruthValue::YES_NULL;
    }
    if (right == TruthValue::NO) {
      return left;
    }
    if (left == TruthValue::NO) {
      return right;
    }
    if (left == TruthValue::IS_NULL) {
      if (right == TruthValue::NO_NULL || right == TruthValue::IS_NULL) {
        return TruthValue::IS_NULL;
      } else {
        return TruthValue::YES_NULL;
      }
    }
    if (right == TruthValue::IS_NULL) {
      if (left == TruthValue::NO_NULL) {
        return TruthValue::IS_NULL;
      } else {
        return TruthValue::YES_NULL;
      }
    }
    if (left == TruthValue::NO_NULL && right == TruthValue::NO_NULL) {
      return TruthValue::NO_NULL;
    }
    return TruthValue::YES_NO_NULL;
  }

  TruthValue operator&&(TruthValue left, TruthValue right) {
    if (right == TruthValue::NO || left == TruthValue::NO) {
      return TruthValue::NO;
    }
    if (right == TruthValue::NO_NULL || left == TruthValue::NO_NULL) {
      return TruthValue::NO_NULL;
    }
    if (right == TruthValue::YES) {
      return left;
    }
    if (left == TruthValue::YES) {
      return right;
    }
    if (left == TruthValue::IS_NULL) {
      if (right == TruthValue::YES_NULL || right == TruthValue::IS_NULL) {
        return TruthValue::IS_NULL;
      } else {
        return TruthValue::NO_NULL;
      }
    }
    if (right == TruthValue::IS_NULL) {
      if (left == TruthValue::YES_NULL) {
        return TruthValue::IS_NULL;
      } else {
        return TruthValue::NO_NULL;
      }
    }
    if (left == TruthValue::YES_NULL && right == TruthValue::YES_NULL) {
      return TruthValue::YES_NULL;
    }
    return TruthValue::YES_NO_NULL;
  }

  TruthValue operator!(TruthValue val) {
    switch (val) {
      case TruthValue::NO:
        return TruthValue::YES;
      case TruthValue::YES:
        return TruthValue::NO;
      case TruthValue::IS_NULL:
      case TruthValue::YES_NO:
      case TruthValue::YES_NO_NULL:
        return val;
      case TruthValue::NO_NULL:
        return TruthValue::YES_NULL;
      case TruthValue::YES_NULL:
        return TruthValue::NO_NULL;
      default:
        throw std::invalid_argument("Unknown TruthValue");
    }
  }

  bool isNeeded(TruthValue val) {
    switch (val) {
      case TruthValue::NO:
      case TruthValue::IS_NULL:
      case TruthValue::NO_NULL:
        return false;
      case TruthValue::YES:
      case TruthValue::YES_NO:
      case TruthValue::YES_NULL:
      case TruthValue::YES_NO_NULL:
      default:
        return true;
    }
  }

}  // namespace orc

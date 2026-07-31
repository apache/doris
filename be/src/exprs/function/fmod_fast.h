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

#include <stddef.h>
#include <stdint.h>

namespace doris::fmod_fast {

bool is_x87_fast_path_enabled();

double scalar(double a, double b);
float scalar(float a, float b);

void vector_vector(const double* lhs, const double* rhs, double* result, uint8_t* null_map,
                   size_t size);
void vector_vector(const float* lhs, const float* rhs, float* result, uint8_t* null_map,
                   size_t size);

void vector_constant(const double* lhs, double rhs, double* result, uint8_t* null_map, size_t size);
void vector_constant(const float* lhs, float rhs, float* result, uint8_t* null_map, size_t size);

void constant_vector(double lhs, const double* rhs, double* result, uint8_t* null_map, size_t size);
void constant_vector(float lhs, const float* rhs, float* result, uint8_t* null_map, size_t size);

} // namespace doris::fmod_fast

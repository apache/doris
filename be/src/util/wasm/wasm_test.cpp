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

#include <fstream>
#include <iostream>
#include <sstream>
#include <wasmtime.hh>

std::string readFile(const char* name) {
    std::ifstream watFile;
    watFile.open(name);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

wasmtime::Span<uint8_t> readWasmFile(const char* filename) {
    FILE* file;
    file = fopen(filename, "rb");
    if (!file) {
        printf("> Error loading module!\n");
        fclose(file);
    }
    fseek(file, 0L, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0L, SEEK_SET);
    printf("File was read...\n");
    wasm_byte_vec_t wasm_bytes;
    wasm_byte_vec_new_uninitialized(&wasm_bytes, file_size);
    if (fread(wasm_bytes.data, file_size, 1, file) != 1) {
        printf("> Error loading module!\n");
    }
    fclose(file);
    std::vector<uint8_t> vec;
    wasmtime::Span<uint8_t> raw(reinterpret_cast<uint8_t*>(wasm_bytes.data), wasm_bytes.size);
    vec.assign(raw.begin(), raw.end());
    wasm_byte_vec_delete(&wasm_bytes);
    return vec;
}

int main() {
    wasmtime::Engine engine;
    wasmtime::Store store(engine);
    auto WatStr =
            "(module\n"
            "  (func $gcd (param i32 i32) (result i32)\n"
            "    (local i32)\n"
            "    block  ;; label = @1\n"
            "      block  ;; label = @2\n"
            "        local.get 0\n"
            "        br_if 0 (;@2;)\n"
            "        local.get 1\n"
            "        local.set 2\n"
            "        br 1 (;@1;)\n"
            "      end\n"
            "      loop  ;; label = @2\n"
            "        local.get 1\n"
            "        local.get 0\n"
            "        local.tee 2\n"
            "        i32.rem_u\n"
            "        local.set 0\n"
            "        local.get 2\n"
            "        local.set 1\n"
            "        local.get 0\n"
            "        br_if 0 (;@2;)\n"
            "      end\n"
            "    end\n"
            "    local.get 2\n"
            "  )\n"
            "  (export \"gcd\" (func $gcd))\n"
            ")";
    auto module = wasmtime::Module::compile(engine, WatStr).unwrap();
    auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

    // Invoke `gcd` export
    auto gcd = std::get<wasmtime::Func>(*instance.get(store, "gcd"));
    auto results = gcd.call(store, {15, 24}).unwrap();

    std::cout << results[0].i32() << "\n";
}
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

namespace doris {

enum class NullState {
    UNKNOWN = 0, 
    IS_NULL = 1,
    NOT_NULL = 2
};
struct RowCursorCell {
    
    RowCursorCell(void* ptr) : _ptr(ptr), _null_state(NullState::UNKNOWN) {}
    RowCursorCell(const void* ptr) : _ptr((void*)ptr), _null_state(NullState::UNKNOWN) {}
    RowCursorCell(void* ptr, NullState null_state) : _ptr((void*)ptr), _null_state(null_state) {}
    RowCursorCell(const void* ptr, NullState null_state) : _ptr((void*)ptr), _null_state(null_state) {}
    bool is_null() const { 
        return _null_state == NullState::UNKNOWN ? *reinterpret_cast<bool*>(_ptr) : _null_state == NullState::IS_NULL; 
    }
    void set_is_null(bool is_null) { 
        if (_null_state == NullState::UNKNOWN)
            *reinterpret_cast<bool*>(_ptr) = is_null;
        else{
            _null_state = (is_null ? NullState::IS_NULL : NullState::NOT_NULL);
        } 
    }
    void set_null(){ 
        if (_null_state == NullState::UNKNOWN){
            *reinterpret_cast<bool*>(_ptr) = true;
        }else{ 
            _null_state = NullState::IS_NULL; 
        }
    }
    void set_not_null(){ 
        if (_null_state == NullState::UNKNOWN){
            *reinterpret_cast<bool*>(_ptr) = false;
        }else{ 
            _null_state = NullState::IS_NULL; 
        }
    }
    const void* cell_ptr() const { 
        if (_null_state == NullState::UNKNOWN){ 
            return (char*)_ptr + 1; 
        }else{ 
            return (char*)_ptr; 
        }
    }
    void* mutable_cell_ptr() const { 
        if (_null_state == NullState::UNKNOWN){ 
            return (char*)_ptr + 1; 
        }else{ 
            return (char*)_ptr; 
        }
    }
private:
    void* _ptr;
    /**
     * @brief if _null_state is UNKNOWN, the null flag is the first char of ptr
     * 
     */
    NullState _null_state; 
};

} // namespace doris

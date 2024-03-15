/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 *  this file is copied from 
 *  https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonUpdater.h
 *  and modified by Doris
 */

#ifndef JSONB_UPDATER_H
#define JSONB_UPDATER_H
#include <deque>
#include <iostream>
#include <limits>
#include <type_traits>

#include "jsonb_document.h"
#include "jsonb_error.h"
#include "jsonb_writer.h"

namespace doris {
struct NodeInfo {
    /*
    There are two types of value with different structure: one is the
    basic data type and array, and the other one is the Object. We
    need to differentiate it here, because the actual value is stored
    differently. For example, for basic data type and array, the
    stored value is JsonbValue, but for Object data type, the stored
    value is JsonbKeyValue.
    The pointer addr points to the beginning of the data value, which
    means that for basic data type, it points ot JsonbValue and for
    the elements in the Object, it points to JsonbKeyValue.
    The pointer, jsonb_value, points to the actually value part, which
    means that for basic data type, it's the same with addr and for
    the JsonbKeyValue, it points to the payload of it.
   */
    char* addr;              // The offset of the address from the beginning
    JsonbValue* jsonb_value; // The address of the value of current node
};
/*
 * This class is used to update the given document. The given document
 * is stored in a buffer. When created, the the updater points to the
 * root value in the document, and pushPathKey and popPathKey can be
 * used to go to different key paths in the document.
 */
class JsonbUpdater {
public:
    // buffer_size is the maximum number of bytes can be packed in document
    JsonbUpdater(JsonbDocument* doc, uint buffer_size, double shrink_ratio = 0.5)
            : document_(doc), buffer_size_(buffer_size), str_shrink_ratio_(shrink_ratio) {
        root_ = document_->getValue();
        clearPath();
    }
    JsonbValue* getRoot() { return root_; }
    JsonbDocument* getDocument() { return document_; }
    JsonbValue* getCurrent() {
        return 0 == path_node_.size() ? nullptr : path_node_.back().jsonb_value;
    }
    // Remove the current node. It can't be root
    JsonbErrType remove() {
        // Remove root is not allowed
        if (path_node_.size() == 1) {
            return JsonbErrType::E_INVALID_OPER;
        }

        // Get the current node and because we want to delete it, pop it from stack
        NodeInfo curr_node = path_node_.back();
        int pack_size = (int)((char*)curr_node.jsonb_value - curr_node.addr +
                              curr_node.jsonb_value->numPackedBytes());

        // Move the data after the current node to new place
        if (!moveTo(curr_node.addr + pack_size, curr_node.addr)) {
            return JsonbErrType::E_OUTOFMEMORY;
        }
        path_node_.pop_back();
        return JsonbErrType::E_NONE;
    }

    JsonbErrType popPathKey() {
        /* It's root, it means that it's the first elements in the
     * document, which means that it points to the payload of the
     * document. It doesn't make sense if we pop the key.
     */
        if (path_node_.size() <= 1) return JsonbErrType::E_INVALID_OPER;
        path_node_.pop_back();
        return JsonbErrType::E_NONE;
    }

    // For ArrayVal
    JsonbErrType pushPathKey(int idx) {
        // Check whether it's an array
        if (nullptr == path_node_.back().jsonb_value || !path_node_.back().jsonb_value->isArray()) {
            return JsonbErrType::E_NOTARRAY;
        }

        // Get the element in given idx
        ArrayVal::iterator ite;
        JsonbErrType ret = getElemInArray(idx, path_node_.back().jsonb_value, ite);
        if (JsonbErrType::E_NONE != ret) return ret;

        if (ite == ((ArrayVal*)path_node_.back().jsonb_value)->end())
            return JsonbErrType::E_OUTOFBOUNDARY;

        path_node_.push_back({(char*)(ArrayVal::pointer)ite, (ArrayVal::pointer)ite});
        return JsonbErrType::E_NONE;
    }

    // For ObjectVal
    JsonbErrType pushPathKey(const char* key, // Should be a
                                              // null-terminated string
                             hDictFind handler = nullptr) {
        if (nullptr == key) return JsonbErrType::E_INVALID_KEY_STRING;
        return pushPathKey(key, (unsigned int)strlen(key), handler);
    }
    JsonbErrType pushPathKey(const char* key, unsigned int klen, hDictFind handler = nullptr) {
        // Check node validity
        if (nullptr == key) {
            return JsonbErrType::E_INVALID_KEY_STRING;
        }
        // Check whether it's an object
        if (nullptr == path_node_.back().jsonb_value ||
            !path_node_.back().jsonb_value->isObject()) {
            return JsonbErrType::E_NOTOBJ;
        }
        // Get current node and search the one we want
        ObjectVal* current = static_cast<ObjectVal*>(path_node_.back().jsonb_value);
        ObjectVal::iterator kv = current->search(key, klen, handler);
        if (kv == current->end()) {
            return JsonbErrType::E_KEYNOTEXIST;
        } else {
            JsonbKeyValue* fb_kv = static_cast<ObjectVal::iterator::pointer>(kv);
            path_node_.push_back({(char*)fb_kv, fb_kv->value()});
        }
        return JsonbErrType::E_NONE;
    }

    // Update a field
    JsonbErrType updateValue(const JsonbValue* value) { return updateValueInternal(value); }

    // Append a value to an array
    JsonbErrType appendValue(const JsonbValue* value) {
        // Check whether it's an array
        if (nullptr == path_node_.back().jsonb_value || !path_node_.back().jsonb_value->isArray()) {
            return JsonbErrType::E_NOTARRAY;
        }
        return insertArrayInternal(((ArrayVal*)path_node_.back().jsonb_value)->end(),
                                   (const char*)value,
                                   (const char*)value + value->numPackedBytes());
    }

    // Insert a value to an array
    JsonbErrType insertValue(int idx, const JsonbValue* value) {
        ArrayVal::iterator ite;
        JsonbErrType ret = getElemInArray(idx, path_node_.back().jsonb_value, ite);
        if (JsonbErrType::E_NONE != ret) return ret;
        return insertArrayInternal(ite, (const char*)value,
                                   (const char*)value + value->numPackedBytes());
    }

    // Insert a range of value to an array
    JsonbErrType insertValue(int idx, ArrayVal::const_iterator beg, ArrayVal::const_iterator end) {
        ArrayVal::iterator ite;
        JsonbErrType ret = getElemInArray(idx, path_node_.back().jsonb_value, ite);
        if (JsonbErrType::E_NONE != ret) return ret;

        const char* beg_addr = (const char*)ArrayVal::const_iterator::pointer(beg);
        const char* end_addr = (const char*)ArrayVal::const_iterator::pointer(end);
        return insertArrayInternal(ite, beg_addr, end_addr);
    }

    // For adding element to dict
    JsonbErrType insertValue(JsonbKeyValue* kvalue) {
        if (nullptr == kvalue) {
            return JsonbErrType::E_INVALID_JSONB_OBJ;
        }
        // Check whether it's an object
        if (nullptr == path_node_.back().jsonb_value ||
            !path_node_.back().jsonb_value->isObject()) {
            return JsonbErrType::E_NOTOBJ;
        }
        return insertValue(ObjectVal::const_iterator(kvalue), ++ObjectVal::const_iterator(kvalue));
    }

    // Insert a range of elements to a dictionary
    JsonbErrType insertValue(ObjectVal::const_iterator beg, ObjectVal::const_iterator end) {
        // Check whether it's an object
        if (nullptr == path_node_.back().jsonb_value ||
            !path_node_.back().jsonb_value->isObject()) {
            return JsonbErrType::E_NOTOBJ;
        }

        ObjectVal::const_iterator obj_end = ((ObjectVal*)path_node_.back().jsonb_value)->end();
        char* curr_addr = (char*)(ObjectVal::const_iterator::pointer)(obj_end);

        // Calculate the bytes needed
        int need_bytes = (int)((char*)(ObjectVal::const_iterator::pointer)end -
                               (char*)(ObjectVal::const_iterator::pointer)beg);
        char* new_addr = curr_addr + need_bytes;
        if (!moveTo(curr_addr, new_addr)) {
            return JsonbErrType::E_OUTOFMEMORY;
        }
        memcpy(curr_addr, (char*)(ObjectVal::const_iterator::pointer)beg, need_bytes);
        return JsonbErrType::E_NONE;
    }

    // Go back to the root node
    void clearPath() {
        path_node_.clear();
        path_node_.push_back({(char*)root_, root_});
    }

private:
    JsonbErrType updateValueInternal(const JsonbValue* value) {
        // If we want to update root, it must be an array or object
        if (path_node_.size() == 1 && !(value->isObject() || value->isArray())) {
            return JsonbErrType::E_INVALID_OPER;
        }
        // For the updating that we don't need to move data
        JsonbValue* curr = path_node_.back().jsonb_value;

        if (value->isInt() && curr->isInt()) {
            // Both are ints and optimization can be done here
            // setVal may fail because the new value can't fit into the current one.
            if (((JsonbIntVal*)curr)->setVal(((const JsonbIntVal*)value)->val())) {
                return JsonbErrType::E_NONE;
            }
        }

        // If both are strings and the new string is shorter than the
        // allocated space, then we can update in place.
        if (value->isString() && curr->isString()) {
            // When both are strings, optimization is possible
            const char* str = value->getValuePtr();
            int str_len = value->size();
            if ('\0' == str[str_len - 1]) {
                // There are tailing NULLs in the string
                str_len = (int)strlen(str);
            }

            // If the new length of the string range in
            // [old_len * (1 - str_shrink_ratio), old_len],
            // then we update it in place. Which means that
            // when the length of the new string is less than
            // old_len * str_shrink_ratio, we will do shrink
            if (str_len <= (int)curr->size() &&
                str_len >= ((int)curr->size() * (1.0 - str_shrink_ratio_))) {
                // We can update string in place
                if (!((JsonbStringVal*)curr)->setVal(str, str_len)) {
                    return JsonbErrType::E_INVALID_OPER;
                }
                return JsonbErrType::E_NONE;
            }
        }

        // We can't update in place, extand it.
        if (curr->numPackedBytes() != value->numPackedBytes()) {
            char* curr_addr = (char*)curr;
            char* next_addr = (char*)curr_addr + curr->numPackedBytes();
            char* new_next_addr = curr_addr + value->numPackedBytes();
            // Expand it or shrink it.
            if (!moveTo(next_addr, new_next_addr)) {
                // If move failed, then we do restore
                return JsonbErrType::E_OUTOFMEMORY;
            }
        }
        // Copy the new data to replace the old one
        memcpy(curr, value, value->numPackedBytes());
        return JsonbErrType::E_NONE;
    }

    /* Insert a node. Insert can only be applied for array. The node
     will be inserted at the given iterator. If the array is not long
     enough, it will be expanded automatically. Null will be the value
     for the expaneded pard. */
    JsonbErrType insertArrayInternal(ArrayVal::iterator ite, const char* val_beg,
                                     const char* val_end) {
        int bytes_needed = (int)(val_end - val_beg);
        char* idx_addr = (char*)ArrayVal::iterator::pointer(ite);
        if (!moveTo(idx_addr, idx_addr + bytes_needed)) {
            return JsonbErrType::E_OUTOFMEMORY;
        }

        // Copy the inserted value
        memcpy(idx_addr, val_beg, bytes_needed);
        return JsonbErrType::E_NONE;
    }

private:
    JsonbErrType getElemInArray(int idx, JsonbValue* array, ArrayVal::iterator& ite) {
        if (nullptr == array || !array->isArray()) return JsonbErrType::E_NOTARRAY;

        if (idx < 0) {
            return JsonbErrType::E_OUTOFBOUNDARY;
        }

        // Now we want to get the node at the given index.
        ArrayVal* arr_val = (ArrayVal*)path_node_.back().jsonb_value;
        int count = 0;
        ite = arr_val->begin();
        while (ite != arr_val->end() && count != idx) {
            ++count;
            ++ite;
        }

        if (ite == arr_val->end() && count != idx) {
            return JsonbErrType::E_OUTOFBOUNDARY;
        }

        return JsonbErrType::E_NONE;
    }

    /*
    Whenever the size of a sub node in the stack is updated, the
    parents should also be updated. inc_size can be positive
    (expanding) or negative (shrinking)
  */
    void updatePackageSize(int inc_size) {
        if (inc_size == 0) return;
        /*
      There are two cases for the top elements in the stack:
       when we do inserting, the first one must be a container,
       and we need to expand the size of it. But when for assignment,
       the first one can be basic type or container and we don't want
       to update it. But now, if we see it's the basic type, we will
       not update it and when it's the container, we still update it.
       It's fine, because, it will be rewritten outside.
    */
        for (auto ite = path_node_.begin(); ite != path_node_.end(); ++ite) {
            JsonbValue* value = ite->jsonb_value;
            if (value->type() >= JsonbType::T_Null && value->type() <= JsonbType::T_Double) {
                // For the last elements in the stack, it can be a scalar and
                // there is no size field stored in them. Just skip to update
                // it.
                assert(path_node_.end() == ite + 1);
            } else {
                uint32_t* size_pointer = (uint32_t*)((char*)value + sizeof(JsonbTypeUnder));
                *size_pointer += inc_size;
            }
        }
    }

    // Move the data from "from" to the end of the document to the new
    // address "to".
    bool moveTo(char* from, char* to) {
        size_t remaining = root_->numPackedBytes() - ((char*)from - (char*)root_);

        // Check whether it exceed the buffer
        if (to + remaining > (char*)document_ + buffer_size_) return false;
        updatePackageSize((int)(to - from));
        memmove(to, from, remaining);
        return true;
    }

    // Helper function to get the beginning of an array
    const char* getArrayBeg(const ArrayVal* arr) {
        return (const char*)(ArrayVal::const_iterator::pointer)arr->begin();
    }

    // Helper function to get the end of an array
    const char* getArrayEnd(const ArrayVal* arr) {
        return (const char*)(ArrayVal::const_iterator::pointer)arr->end();
    }

private:
    JsonbDocument* document_ = nullptr;
    JsonbValue* root_ = nullptr;
    uint buffer_size_;
    // This stack store all the key path in the document. It's deserve
    // noticing that the root node is always in this stack.
    std::deque<NodeInfo> path_node_;
    JsonbWriter writer_;
    double str_shrink_ratio_;
};

} // namespace doris
#endif

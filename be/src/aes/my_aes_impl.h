// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_EXPRS_MY_AES_IMPL_H
#define BDG_PALO_BE_EXPRS_MY_AES_IMPL_H

/** Maximum supported key kength */
const int MAX_AES_KEY_LENGTH = 256;

/* TODO: remove in a future version */
/* Guard against using an old export control restriction #define */
#ifdef AES_USE_KEY_BITS
#error AES_USE_KEY_BITS not supported
#endif
typedef uint32_t uint;
typedef uint8_t uint8;

namespace palo {

extern uint *my_aes_opmode_key_sizes;
void my_aes_create_key(const unsigned char *key, uint key_length,
                       uint8 *rkey, enum my_aes_opmode opmode);
}

#endif

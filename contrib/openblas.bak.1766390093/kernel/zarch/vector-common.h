/*
 * Copyright (c) IBM Corporation 2020.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *    3. Neither the name of the OpenBLAS project nor the names of
 *       its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <vecintrin.h>

#define VLEN_BYTES 16
#define VLEN_FLOATS (VLEN_BYTES / sizeof(FLOAT))

typedef FLOAT vector_float __attribute__ ((vector_size (VLEN_BYTES)));

/**
 * Load a vector into register, and hint on 8-byte alignment to improve
 * performance. gcc-9 and newer will create these hints by itself. For older
 * compiler versions, use inline assembly to explicitly express the hint.
 * Provide explicit hex encoding to cater for binutils versions that do not know
 * about vector-load with alignment hints yet.
 *
 * Note that, for block sizes where we apply vectorization, vectors in A will
 * always be 8-byte aligned.
 */
static inline vector_float vec_load_hinted(FLOAT const *restrict a) {
	vector_float const *restrict addr = (vector_float const *restrict)a;
	vector_float y;

#if __GNUC__ < 9 && !defined(__clang__)
	// hex-encode vl %[out],%[addr],3
	asm(".insn vrx,0xe70000003006,%[out],%[addr],3"
	    : [ out ] "=v"(y)
	    : [ addr ] "R"(*addr));
#else
	y = *addr;
#endif

	return y;
}

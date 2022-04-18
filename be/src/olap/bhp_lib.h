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

namespace doris {

inline int memcmp_sse(const void* buf1, const void* buf2, unsigned int count) {
    int result;

    __asm__ __volatile__(
            "cmpl $16, %%edx;"
            "jb 9f;"
            "16:" /*  len >= 16 */
            "movdqu  (%%rdi), %%xmm0;"
            "movdqu  (%%rsi), %%xmm1;"
            "pcmpeqb %%xmm1, %%xmm0;"
            "pmovmskb %%xmm0,%%rcx;"
            "xorl $0xffff, %%ecx;"
            "jz 15f;"
            "bsf %%ecx, %%ecx;" /* diff */
            "movzb (%%rsi, %%rcx), %%edx;"
            "movzb (%%rdi, %%rcx), %%eax;"
            "subl %%edx, %%eax;"
            "jmp 0f;"
            "15:" /* same */
            "subl $16, %%edx;"
            "jbe 1f;"
            "movq $16, %%rcx;"
            "cmpl $16, %%edx;"
            "jae 14f;"
            "movl %%edx, %%ecx;"
            "14:"
            //"addq %%rcx, %%rdi;"
            "lea (%%rdi,%%rcx), %%rdi;"
            "addq %%rcx, %%rsi;"
            "jmp 16b;"

            "9:" /* 8 =< len < 15 */
            "cmpl $8, %%edx;"
            "jb 5f;"
            "8:"
            "movq (%%rdi), %%xmm0;"
            "movq (%%rsi), %%xmm1;"
            "pcmpeqb %%xmm1, %%xmm0;"
            "pmovmskb %%xmm0, %%rcx;"
            "and $0xff, %%ecx;"
            "xorl $0xff, %%ecx;"
            "je 7f;"
            "bsf  %%ecx, %%ecx;" /* diff */
            "movzb (%%rsi, %%rcx), %%edx;"
            "movzb (%%rdi, %%rcx), %%eax;"
            "subl %%edx, %%eax;"
            "jmp 0f;"

            "7:"
            "subl $8, %%edx;"
            "jz 1f;"
            "movl %%edx, %%ecx;"
            "movq (%%rdi, %%rcx), %%xmm0;"
            "movq (%%rsi, %%rcx), %%xmm1;"
            "pcmpeqb %%xmm1, %%xmm0;"
            "pmovmskb %%xmm0, %%rcx;"
            "and $0xff, %%ecx;"
            "xorl $0xff, %%ecx;"
            "je 1f;"
            "bsf  %%ecx, %%ecx;"
            "addl %%edx, %%ecx;"
            "movzb (%%rsi, %%rcx), %%edx;"
            "movzb (%%rdi, %%rcx), %%eax;"
            "subl %%edx, %%eax;"
            "jmp 0f;"

            "5:"
            "cmpl $4, %%edx;"
            "jb 13f;"
            "4:"
            "subl $4, %%edx;"
            "movl (%%rdi), %%eax;"
            "movl (%%rsi), %%ecx;"
            "cmpl  %%ecx, %%eax;"
            "je 3f;"
            "bswap %%eax;"
            "bswap %%ecx;"
            "cmpl %%ecx, %%eax;"
            "ja 17f;"
            "mov $-1, %%eax;"
            "jmp 0f;"
            "17:"
            "mov $1, %%eax;"
            "jmp 0f;"
            "3:"
            "addq $4, %%rdi;"
            "lea 4(%%rsi), %%rsi;"
            "13:"
            "cmpl $0, %%edx;"
            "je 1f;"
            "2:"

            "movzbl (%%rdi), %%eax;"
            "movzbl (%%rsi), %%ecx;"
            "subl %%ecx, %%eax;"
            "jne 0f;"
            "subl $1, %%edx;"
            "jz 1f;"
            "movzbl 1(%%rdi), %%eax;"
            "movzbl 1(%%rsi), %%ecx;"
            "subl %%ecx, %%eax;"
            "jne 0f;"
            "subl $1, %%edx;"
            "jz 1f;"
            "movzbl 2(%%rdi), %%eax;"
            "movzbl 2(%%rsi), %%ecx;"
            "subl %%ecx, %%eax;"
            "jmp 0f;"

            "1:"
            "xorl %%eax, %%eax;"
            "0:"
            : "=a"(result), "=D"(buf1), "=S"(buf2), "=d"(count)
            : "D"(buf1), "S"(buf2), "d"(count)
            : "%rcx", "%xmm1", "%xmm0", "memory");
    return result;
}

//count must be between 0 and 2GB
/*__attribute__((always_inline))*/ inline int memcmp_sse32(const void* buf1, const void* buf2,
                                                           int count)

{
    int result;
    __asm__ __volatile__(
            //".align 8;"
            "cmp $1, %%edx;"
            "jbe 6f;"

            "addl  $16,  %%edx  ;"
            "movl  %%edx, %%eax ;"
            "xor  %%rcx, %%rcx ;"

            "2: "
            "movdqu  (%%rdi), %%xmm1;"
            "movdqu  (%%rsi), %%xmm2;"
            "subl     $16,    %%edx  ;"
            "subl     $16,    %%eax  ;"

            // " pcmpestri $0x18, %%xmm2, %%xmm1  ;"
            ".byte 0x66, 0x0f, 0x3a, 0x61, 0xca, 0x18;"
            " lea     16(%%rsi),    %%rsi  ;"
            " lea     16(%%rdi),    %%rdi  ;"
            //zflag=0 and cflag=0;no diff and no end, so continue the loop
            " ja  2b                     ;"
            // if cflag=1, jmp; no end but diff
            " jc  1f                   ;"

            "xorl %%eax, %%eax;"
            "jmp 0f;"

            "6:"
            "xor %%eax, %%eax;"
            "test %%edx, %%edx ;"
            "jz 0f ;"
            "movzbl (%%rdi), %%eax;"
            "movzbl (%%rsi), %%edx;"
            "subl %%edx, %%eax;"
            "jmp 0f;"

            "1:"
            "movzbl  -16(%%rsi, %%rcx), %%edx   ;"
            "movzbl  -16(%%rdi, %%rcx), %%eax   ;"
            "subl    %%edx, %%eax               ;"

            "0:"
            //"mov %%eax, %0;"

            : "=a"(result), "=D"(buf1), "=S"(buf2), "=d"(count)
            : "D"(buf1), "S"(buf2), "d"(count)
            : "%rcx", "memory", "xmm1", "xmm2");
    return result;
}

/*__attribute__((always_inline))*/ inline int memcmp_sse64(const void* buf1, const void* buf2,
                                                           size_t count) {
    int result;
    __asm__ __volatile__(
            "cmp $1, %%rdx;"
            "jbe 6f;"

            "addq $16, %%rdx;"
            "movq %%rdx,%%rax;"
            //"xor  %%rcx, %%rcx ;"

            "2: "
            "movdqu (%%rdi), %%xmm1;"
            "movdqu (%%rsi), %%xmm2;"

            "subq $16, %%rax;"
            "subq $16, %%rdx;"

            //"addq $16, %%rsi;"
            //"addq $16, %%rdi;"
            //  " pcmpestri $0x18, %%xmm2, %%xmm1  ;"
            ".byte 0x66, 0x0f, 0x3a, 0x61, 0xca, 0x18;"
            "lea 16(%%rsi), %%rsi;"
            "lea 16(%%rdi), %%rdi;"
            "ja 2b;" //no diff and no end, so continue the loop
            "jc 1f;" // no end but diff

            "xorl %%eax, %%eax;"
            "jmp 0f;"

            "6:"
            "xor %%eax, %%eax;"
            "test %%edx, %%edx ;"
            "jz 0f ;"
            "movzbl (%%rdi), %%eax;"
            "movzbl (%%rsi), %%edx;"
            "subl %%edx, %%eax;"
            "jmp 0f;"

            "1:"
            "movzbl  -16(%%rsi, %%rcx), %%edx   ;"
            "movzbl  -16(%%rdi, %%rcx), %%eax   ;"
            "subl    %%edx, %%eax               ;"

            "0:"
            //"mov %%eax, %0;"

            : "=a"(result), "=D"(buf1), "=S"(buf2), "=d"(count)
            : "D"(buf1), "S"(buf2), "d"(count)
            : "%rcx", "memory", "xmm1", "xmm2");
    return result;
}

/*__attribute__((always_inline))*/ inline int find_chr_from_mem(const char* s, int c, int len) {
    //len : edx; c: esi; s:rdi
    int index;
    __asm__ __volatile__(
            "and $0xff, %%esi;" //clear upper bytes
            "movd %%esi, %%xmm1;"

            "mov $1, %%eax;"
            "add $16, %%edx;"
            "mov %%rdi ,%%r8;"

            "1:"
            "movdqu (%%rdi), %%xmm2;"
            "sub $16, %%edx;"
            "addq $16, %%rdi;"
            //"pcmpestri $0x0, %%xmm2,%%xmm1;"
            ".byte 0x66 ,0x0f ,0x3a ,0x61 ,0xca ,0x00;"
            //"lea 16(%%rdi), %%rdi;"
            "ja 1b;" //Res2==0:no match and zflag==0: s is not end
            "jc 3f;" //Res2==1: match and s is not end

            "mov $0xffffffff, %%eax;" //no match
            "jmp 0f;"

            "3:"
            "sub %%r8, %%rdi;"
            "lea -16(%%edi,%%ecx),%%eax;"

            "0:"
            //        "mov %%eax, %0;"
            : "=a"(index), "=D"(s), "=S"(c), "=d"(len)
            : "D"(s), "S"(c), "d"(len)
            : "rcx", "r8", "memory", "xmm1", "xmm2");
    return index;
}

/*__attribute__((always_inline))*/ inline int find_chr_from_str(const char* s, int c, int len) {
    //s:rdi; c:rsi; len:rdx
    int index;
    __asm__ __volatile__(
            "and $0xff, %%esi;" //clear upper bytes
            "movd %%esi, %%xmm1;"
            "xor %%r8d,%%r8d;"

            "1:"
            "movdqu (%%rdi), %%xmm2;"
            "add $16, %%r8d;"
            "addq $16, %%rdi;"
            //        "pcmpistri $0x0, %%xmm2,%%xmm1;"
            ".byte 0x66 ,0x0f ,0x3a ,0x63 ,0xca ,0x00;"
            //"lea 16(%%rdi), %%rdi;"
            "ja 4f;"  // not null and no match, so clarify whether over the end
            "jc 2f;"  //match
            "jmp 3f;" //null and no match

            "4:"
            "cmp %%r8d,%%edx;"
            "ja 1b;"

            "3:"
            "mov $0xffffffff, %%eax;" // the end and no match
            "jmp 0f;"

            "2:"

            "lea -16(%%r8d, %%ecx), %%eax;"
            "cmp %%edx, %%eax;"
            "jae 3b;"

            "0:"
            // "mov %%eax, %0;"

            : "=a"(index), "=D"(s), "=S"(c), "=d"(len)
            : "D"(s), "S"(c), "d"(len)
            : "rcx", "r8", "memory", "xmm1", "xmm2");
    return index;
}

/*__attribute__((always_inline))*/ inline char* strchr_sse(const char* s, int c) {
    //s:rdi; c:rsi
    char* ret;
    __asm__ __volatile__(
            "and $0xff, %%esi;" //clear upper bytes
            //c==0
            "test %%esi, %%esi;"
            "jnz 0f ;"
            "movq %%rdi, %%rax;"
            "pxor %%xmm1, %%xmm1;"
            "3:"
            "movdqu (%%rdi), %%xmm2;"

            "addq $16, %%rdi;"
            // "pcmpistri $0x8, %%xmm2,%%xmm1;"
            ".byte 0x66, 0x0f, 0x3a, 0x63, 0xca, 0x08;"
            "jnz 3b;"

            "leaq -16(%%rdi,%%rcx), %%rax;"
            "jmp 2f;"

            "0:"
            "movd %%esi, %%xmm1;"
            //"xor %%rcx, %%rcx;"
            "xor %%rax, %%rax;"

            "1:"
            "movdqu (%%rdi), %%xmm2;"

            "addq $16, %%rdi;"
            //        "pcmpistri $0x0, %%xmm2,%%xmm1;"
            ".byte 0x66 ,0x0f ,0x3a ,0x63 ,0xca ,0x00;"
            "ja 1b;"
            "jnc 2f;"
            "lea -16(%%rdi, %%rcx), %%rax;"
            "2:"

            : "=a"(ret), "=D"(s), "=S"(c)
            : "D"(s), "S"(c)
            : "rcx", "memory", "xmm1", "xmm2");
    return ret;
}

/*__attribute__((always_inline))*/ inline char* strrchr_sse(const char* s, int c) {
    //s:rdi; c:rsi
    char* ret;
    __asm__ __volatile__(
            "and $0xff, %%esi;" //clear upper bytes
            //c==0
            "test %%esi, %%esi;"
            "jnz 0f ;"

            "movq %%rdi, %%rax;"
            "pxor %%xmm1, %%xmm1;"
            "3:"
            "movdqu (%%rdi), %%xmm2;"

            "addq $16, %%rdi;"
            // "pcmpistri $0x8, %%xmm2,%%xmm1;"
            ".byte 0x66, 0x0f, 0x3a, 0x63, 0xca, 0x08;"
            "jnz 3b;"

            "leaq -16(%%rdi,%%rcx), %%rax;"
            "jmp 3f;"

            "0:"
            "movd %%esi, %%xmm1;"
            //"xor %%rcx, %%rcx;"
            "xor %%rax, %%rax;"

            "1:"
            "movdqu (%%rdi), %%xmm2;"

            "addq $16, %%rdi;"
            //        "pcmpistri $0x40, %%xmm2,%%xmm1;"
            ".byte 0x66 ,0x0f ,0x3a ,0x63 ,0xca ,0x40;"
            "ja 1b;" //zflag =0 and cflag =0, it means no end and no match

            "jz 2f;"                        //zflag =1, the end of string
            "lea -16(%%rdi, %%rcx), %%rax;" //cflag =1
            "jmp 1b;"

            "2:"
            "jnc 3f;"
            "lea -16(%%rdi, %%rcx), %%rax;"
            "3:"
            //"mov %%rax, %0;"
            : "=a"(ret), "=D"(s), "=S"(c)
            : "D"(s), "S"(c)
            : "rcx", "memory", "xmm1", "xmm2");
    return ret;
}

inline char* strrchr_end_sse(char const* b, char const* e, char c) {
    //b:rdi; e:rsi; c:rdx
    char* ret;

    __asm__ __volatile__(

            //       "movzbq %5, %%rdx;"
            //  "mov %%rdx, %%r8;"
            "movzbq %5, %%r8;"

            "cmp $0, %%rdi;"
            "jbe 1f;"

            //calculate rdx, decide where to go
            "mov %%rsi, %%rdx;"
            "subq %%rdi, %%rdx;"
            "jbe 1f;" // if begin >= end, return
            "cmp $7, %%rdx;"
            "jna 2f;"

            // rdx >= 8
            "movd %%r8, %%xmm1;"
            "mov $1, %%rax;"
            "cmp $16, %%rdx;"
            "ja 3f;" //  if rdx > 16, jmp to 3f

            "5:"
            // 8 <= rdx <= 16
            "subq %%rdx, %%rsi;"
            "movdqu (%%rsi), %%xmm2;"
            // "pcmpestri $0x40, %%xmm2, %%xmm1;"
            ".byte 0x66, 0x0f, 0x3a, 0x61, 0xca, 0x40;"
            "jnc 1f; "                   // if cflag=0, not match, jmp to 1f
            "lea (%%rsi, %%rcx), %%rax;" // matched
            "jmp 0f;"

            // after 16-bytes compare
            "4:"
            "subq $16, %%rdx;"
            "cmp $7, %%rdx;"
            "jna 2f;" // if rdx < 8, jmp to 2f
            "cmp $16, %%rdx;"
            "jna 5b;"

            "3:"
            "subq $16, %%rsi;"
            "movdqu (%%rsi), %%xmm2;"
            // "pcmpestri $0x40, %%xmm2, %%xmm1;"
            ".byte 0x66, 0x0f, 0x3a, 0x61, 0xca, 0x40;"
            "ja 4b;"                     // cflag = 0:not match && zflag = 0:not end >>> loopback
            "lea (%%rsi, %%rcx), %%rax;" // rdx > 16, zflag always = 0, match
            "jmp 0f;"

            "2:"
            // 0 < rdx < 8
            "mov %%r8, %%rax;"

            // switch rdx;
            "cmpb -1(%%rsi), %%al;"
            "jne 11f;"
            "lea -1(%%rsi), %%rax;"
            "jmp 0f;"
            "11:"
            "cmp $1, %%rdx;"
            "je 1f;"

            "cmpb -2(%%rsi), %%al;"
            "jne 12f;"
            "lea -2(%%rsi), %%rax;"
            "jmp 0f;"
            "12:"
            "cmp $2, %%rdx;"
            "je 1f;"

            "cmpb -3(%%rsi), %%al;"
            "jne 13f;"
            "lea -3(%%rsi), %%rax;"
            "jmp 0f;"
            "13:"
            "cmp $3, %%rdx;"
            "je 1f;"

            "cmpb -4(%%rsi), %%al;"
            "jne 14f;"
            "lea -4(%%rsi), %%rax;"
            "jmp 0f;"
            "14:"
            "cmp $4, %%rdx;"
            "je 1f;"

            "cmpb -5(%%rsi), %%al;"
            "jne 15f;"
            "lea -5(%%rsi), %%rax;"
            "jmp 0f;"
            "15:"
            "cmp $5, %%rdx;"
            "je 1f;"

            "cmpb -6(%%rsi), %%al;"
            "jne 16f;"
            "lea -6(%%rsi), %%rax;"
            "jmp 0f;"
            "16:"
            "cmp $6, %%rdx;"
            "je 1f;"

            "cmpb -7(%%rsi), %%al;"
            "jne 1f;"
            "lea -7(%%rsi), %%rax;"
            "jmp 0f;"

            // failed return
            "1:"
            "xor %%rax, %%rax;" // return null

            // success return
            "0:"

            : "=a"(ret), "=D"(b), "=S"(e) //,"=d"(c)
            : "D"(b), "S"(e), "r"(c)
            : "r8", "rcx", "memory", "xmm1", "xmm2", "rdx");
    return ret;
}

/*__attribute__((always_inline))*/ inline void* memchr_sse(const void* s, int c, size_t n) {
    //s:rdi; c:rsi; n:rdx
    void* ret;
    __asm__ __volatile__(
            "and $0xff, %%esi;" //clear upper bytes
            "movd %%esi, %%xmm1;"

            "mov $1, %%rax;"
            "add $16, %%rdx;"

            "1:"
            "movdqu (%%rdi), %%xmm2;"
            "sub $16, %%rdx;"
            "addq $16, %%rdi;"
            //"pcmpestri $0x0, %%xmm2,%%xmm1;"
            ".byte 0x66 ,0x0f ,0x3a ,0x61 ,0xca ,0x00;"
            //"lea 16(%%rdi), %%rdi;"
            "ja 1b;" //Res2==0:no match and zflag==0: s is not end
            "jc 3f;" //Res2==1: match and s is not end

            "mov $0x0, %%rax;" //no match
            "jmp 0f;"

            "3:"

            "lea -16(%%rdi,%%rcx),%%rax;"

            "0:"
            //"mov %%rax, %0;"
            : "=a"(ret), "=D"(s), "=S"(c), "=d"(n)
            : "D"(s), "S"(c), "d"(n)
            : "rcx", "memory", "xmm1", "xmm2");
    return ret;
}

/*__attribute__((always_inline))*/ inline size_t strlen_sse(const char* s) {
    //s:rdi
    size_t ret;
    __asm__ __volatile__(
            "movq $-16, %%rax;"
            //"xor %%rcx, %%rcx;"
            "pxor %%xmm0, %%xmm0;"

            "1:"
            "movdqu (%%rdi), %%xmm1;"
            "addq $16, %%rax;"
            "addq $16, %%rdi;"
            //"pcmpistri $0x8, %%xmm1,%%xmm0;"
            ".byte 0x66, 0x0f, 0x3a, 0x63, 0xc1, 0x08;"
            //"lea     16(%%rdi),    %%rdi  ;"
            //"lea     16(%%rax),    %%rax  ;"
            "jnz 1b;"

            "addq %%rcx, %%rax;"
            //"mov %%rax, %0;"
            : "=a"(ret), "=D"(s)
            : "D"(s)
            : "rcx", "memory", "xmm0", "xmm1");
    return ret;
}

/*__attribute__((always_inline))*/ inline int strcmp_sse(const char* s1, const char* s2)

{
    //s1:rdi; s2:rsi
    int result;
    __asm__ __volatile__(
            "xor %%rax, %%rax ;"
            //"xor %%rcx, %%rcx ;"

            "1:"
            "movdqu  (%%rdi), %%xmm1;"
            "movdqu  (%%rsi), %%xmm2;"
            "addq $16, %%rsi;"
            "addq $16, %%rdi;"
            //       " pcmpistri $0x18, %%xmm2, %%xmm1  ;"
            ".byte 0x66 ,0x0f ,0x3a ,0x63 ,0xca ,0x18;"
            " ja  1b                     ;"

            "jnc  0f;"
            "movzbq  -16(%%rsi, %%rcx), %%rdx   ;"
            "movzbq  -16(%%rdi, %%rcx), %%rax   ;"
            //      "sub     %%rdx, %%rax               ;"
            "movl $1, %%ecx;"
            "movl $-1, %%edi;"
            "cmp  %%rdx, %%rax;"
            "cmova %%ecx, %%eax;"
            "cmovb %%edi, %%eax;"

            "0:"
            //"mov %%eax, %0;"

            : "=a"(result), "=D"(s1), "=S"(s2)
            : "D"(s1), "S"(s2)
            : "rcx", "rdx", "memory", "xmm1", "xmm2");
    return result;
}

/*__attribute__((always_inline))*/ inline int strncmp_sse(const char* s1, const char* s2, size_t n)

{
    //s1:rdi; s2:rsi; n:rdx
    int result;
    __asm__ __volatile__(
            "cmp $1, %%rdx;"
            "jbe 3f;"

            "xor %%rax, %%rax ;"

            "1:"
            "movdqu  (%%rdi), %%xmm1;"
            "movdqu  (%%rsi), %%xmm2;"
            "addq $16, %%rdi;"
            "addq $16, %%rsi;"
            // " pcmpistri $0x18, %%xmm2, %%xmm1  ;"
            ".byte 0x66 ,0x0f ,0x3a ,0x63 ,0xca ,0x18;"
            //  "lea 16(%%rsi), %%rsi;"
            //  "lea 16(%%rdi), %%rdi;"
            "ja  2f                     ;" //both 16Byte data elements are valid and identical
            "jnc  0f;"                     //Both 16byte data elements have EOS and identical

            //the following situation is  Both 16byte data elements differ at offset X (ecx).

            "cmp %%rdx, %%rcx;"
            "jae 0f;" // X is out of n

            "movzbq  -16(%%rsi, %%rcx), %%rdx   ;" // X is in the range of n
            "movzbq  -16(%%rdi, %%rcx), %%rax   ;"
            "subq     %%rdx, %%rax               ;"
            "jmp 0f;"

            "2:"
            "subq $16, %%rdx;"
            "jbe 0f;"
            "ja 1b;"

            "3:"
            "xor %%eax, %%eax;"
            "test %%rdx, %%rdx ;"
            "jz 0f ;"
            "movzbl (%%rdi), %%eax;"
            "movzbl (%%rsi), %%edx;"
            "subl %%edx, %%eax;"

            "0:"
            //  "mov %%eax, %0;"

            : "=a"(result), "=D"(s1), "=S"(s2), "=d"(n)
            : "D"(s1), "S"(s2), "d"(n)
            : "rcx", "memory", "xmm1", "xmm2");
    return result;
}

/*__attribute__((always_inline))*/ inline int baidu_crc32_byte(char const* src, int crc,
                                                               int length) {
    int crc_out;
    __asm__ __volatile__(
            "1:"
            "movzbl (%%rdi), %%ecx;"
            //"crc32b %%cl, %%esi;"
            ".byte 0xf2, 0xf, 0x38, 0xf0, 0xf1;"

            "add $1, %%rdi;"
            "sub $1, %%edx;"
            "jnz 1b;"
            "movl %%esi,%%eax;"
            : "=a"(crc_out), "=D"(src), "=S"(crc), "=d"(length)
            : "D"(src), "S"(crc), "d"(length)
            : "memory", "ecx");

    return crc_out;
}

inline int crc32c_qw(char const* src, int crc, unsigned int qwlen) {
    int crc_out;
    __asm__ __volatile__(
            "1:"
            //      "crc32q (%%rdi), %%rsi;"
            ".byte 0xf2 ,0x48 ,0x0f ,0x38 ,0xf1, 0x37;"

            "addq $8, %%rdi;"
            "subl $1, %%edx;"
            "jnz 1b;"
            "mov %%esi,%%eax;"
            : "=a"(crc_out), "=D"(src), "=S"(crc), "=d"(qwlen)
            : "D"(src), "S"(crc), "d"(qwlen)
            : "memory");
    return crc_out;
}

inline int baidu_crc32_qw(char const* src, int crc, unsigned int length) {
    unsigned int iquotient = length >> 3;
    unsigned int iremainder = length & 0x7;
    char const* p;

    if (iquotient) {
        crc = crc32c_qw(src, crc, iquotient);
    }

    if (iremainder) {
        p = src + (length - iremainder);
        crc = baidu_crc32_byte(p, crc, iremainder);
    }

    return crc;
}

} // namespace doris

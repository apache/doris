/***************************************************************************
Copyright (c) 2024, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#ifndef SBGEMV_COMMON_MMA_C
#define SBGEMV_COMMON_MMA_C
#include "sbgemv_common.c"

#if defined(_AIX) || defined(__clang__)
#define USE_MERGE_MMA
#endif

FORCEINLINE void vec_load_pair2(vec_bf16 *in0, vec_bf16 *in)
{
  vec_load_pair((vec_f32 *)(in0 + 0), (vec_f32 *)(in + 0));
  vec_load_pair((vec_f32 *)(in0 + 2), (vec_f32 *)(in + 2));
}

FORCEINLINE void vec_load_mult_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 inp)
{
  vec_bf16 in0 = (vec_bf16)vec_load_vec(in);

  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)in0, (vec_uc8)inp);
}

FORCEINLINE void vec_load_mult12a_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 inp)
{
  vec_bf16 in11 = (vec_bf16)vec_load_vec(in1);

  vec_load_mult_mma(out, in0, inp);

  __builtin_mma_xvbf16ger2pp(out + 1, (vec_uc8)in11, (vec_uc8)inp);
}

FORCEINLINE void vec_load_mult14_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *in2, vec_bf16 *in3, vec_bf16 inp)
{
  vec_bf16 in21 = (vec_bf16)vec_load_vec(in2);
  vec_bf16 in31 = (vec_bf16)vec_load_vec(in3);

  vec_load_mult12a_mma(out, in0, in1, inp);

  __builtin_mma_xvbf16ger2pp(out + 2, (vec_uc8)in21, (vec_uc8)inp);
  __builtin_mma_xvbf16ger2pp(out + 3, (vec_uc8)in31, (vec_uc8)inp);
}

FORCEINLINE void vec_load_mult2_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 *inp)
{
  vec_bf16 in0[2];

  vec_load_pair((vec_f32 *)in0, (vec_f32 *)in);

  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)in0[0], (vec_uc8)inp[0]);
  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)in0[1], (vec_uc8)inp[1]);
}

FORCEINLINE void vec_mult2d_mma(__vector_quad *out, vec_bf16 *in01, vec_bf16 *in11, vec_bf16 *inp)
{
  __builtin_mma_xvbf16ger2pp(out + 0, (vec_uc8)in01[0], (vec_uc8)inp[0]);
  __builtin_mma_xvbf16ger2pp(out + 1, (vec_uc8)in11[0], (vec_uc8)inp[0]);
}

FORCEINLINE void vec_load_mult22_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *inp)
{
  vec_bf16 in01[2], in11[2];

  vec_load_pair((vec_f32 *)in01, (vec_f32 *)in0);
  vec_load_pair((vec_f32 *)in11, (vec_f32 *)in1);

  vec_mult2d_mma(out, in01 + 0, in11 + 0, inp + 0);
  vec_mult2d_mma(out, in01 + 1, in11 + 1, inp + 1);
}

FORCEINLINE void vec_load_mult24_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *in2, vec_bf16 *in3, vec_bf16 *inp)
{
  vec_bf16 in01[2], in11[2], in21[2], in31[2];

  vec_load_pair((vec_f32 *)in01, (vec_f32 *)in0);
  vec_load_pair((vec_f32 *)in11, (vec_f32 *)in1);
  vec_load_pair((vec_f32 *)in21, (vec_f32 *)in2);
  vec_load_pair((vec_f32 *)in31, (vec_f32 *)in3);

  vec_mult2d_mma(out + 0, in01 + 0, in11 + 0, inp + 0);
  vec_mult2d_mma(out + 2, in21 + 0, in31 + 0, inp + 0);
  vec_mult2d_mma(out + 0, in01 + 1, in11 + 1, inp + 1);
  vec_mult2d_mma(out + 2, in21 + 1, in31 + 1, inp + 1);
}

FORCEINLINE void vec_load_mult4_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 *inp)
{
  vec_bf16 in0[2];

  vec_load_pair((vec_f32 *)(in0 + 0), (vec_f32 *)(in + 2));

  vec_load_mult2_mma(out, in + 0, inp + 0);

  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)in0[0], (vec_uc8)inp[2]);
  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)in0[1], (vec_uc8)inp[3]);
}

FORCEINLINE void vec_load_mult42_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *inp)
{
  vec_bf16 in01[4], in11[4];

  vec_load_pair2(in01, in0);
  vec_load_pair2(in11, in1);

  vec_mult2d_mma(out, in01 + 0, in11 + 0, inp + 0);
  vec_mult2d_mma(out, in01 + 1, in11 + 1, inp + 1);
  vec_mult2d_mma(out, in01 + 2, in11 + 2, inp + 2);
  vec_mult2d_mma(out, in01 + 3, in11 + 3, inp + 3);
}

FORCEINLINE void vec_mult4d_mma(__vector_quad *out, vec_bf16 *in01, vec_bf16 *in11, vec_bf16 *in21, vec_bf16 *in31, vec_bf16 *inp)
{
  vec_mult2d_mma(out + 0, in01, in11, inp);
  vec_mult2d_mma(out + 2, in21, in31, inp);
}

FORCEINLINE void vec_load_mult44_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *in2, vec_bf16 *in3, vec_bf16 *inp)
{
  vec_bf16 in01[4], in11[4], in21[4], in31[4];

  vec_load_pair2(in01, in0);
  vec_load_pair2(in11, in1);
  vec_load_pair2(in21, in2);
  vec_load_pair2(in31, in3);

  vec_mult4d_mma(out, in01 + 0, in11 + 0, in21 + 0, in31 + 0, inp + 0);
  vec_mult4d_mma(out, in01 + 1, in11 + 1, in21 + 1, in31 + 1, inp + 1);
  vec_mult4d_mma(out, in01 + 2, in11 + 2, in21 + 2, in31 + 2, inp + 2);
  vec_mult4d_mma(out, in01 + 3, in11 + 3, in21 + 3, in31 + 3, inp + 3);
}

FORCEINLINE void vec_loadN_mult_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(in, n);

  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)in0, (vec_uc8)inp);
}

FORCEINLINE void vec_loadN_mult12a_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in11 = (vec_bf16)vec_loadN(in1, n);

  vec_loadN_mult_mma(out, in0, inp, n);

  __builtin_mma_xvbf16ger2pp(out + 1, (vec_uc8)in11, (vec_uc8)inp);
}

FORCEINLINE void vec_loadN_mult14_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *in2, vec_bf16 *in3, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in21 = (vec_bf16)vec_loadN(in2, n);
  vec_bf16 in31 = (vec_bf16)vec_loadN(in3, n);

  vec_loadN_mult12a_mma(out, in0, in1, inp, n);

  __builtin_mma_xvbf16ger2pp(out + 2, (vec_uc8)in21, (vec_uc8)inp);
  __builtin_mma_xvbf16ger2pp(out + 3, (vec_uc8)in31, (vec_uc8)inp);
}

FORCEINLINE void vec_mult1_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 inp)
{
  vec_bf16 in00 = vec_mergeh(in0, in0);

  __builtin_mma_xvbf16ger2(out, (vec_uc8)inp, (vec_uc8)in00);
}

FORCEINLINE void vec_mult2_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 inp)
{
  vec_bf16 in01 = vec_mergel(in0, in0);

  vec_mult1_mma(&out[0], in0, inp);

  __builtin_mma_xvbf16ger2(&out[1], (vec_uc8)inp, (vec_uc8)in01);
}

#ifndef USE_MERGE_MMA
FORCEINLINE void vec_mult4_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 inp)
{
  vec_mult2_mma(out + 0, in0[0], inp);
  vec_mult2_mma(out + 2, in0[1], inp);
}
#endif

FORCEINLINE void vec_loadN_mult11_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(in, n);

  vec_mult1_mma(out, in0, inp);
}

FORCEINLINE void vec_loadN_mult12_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(in, n);

  vec_mult2_mma(out, in0, inp);
}

FORCEINLINE void vec_load_mult12_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 inp)
{
  vec_bf16 in0 = (vec_bf16)vec_load_vec(in);

  vec_mult2_mma(out, in0, inp);
}

#ifndef USE_MERGE_MMA
FORCEINLINE void vec_load_mult18_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 inp)
{
  vec_bf16 in0[4];

  vec_load_pair((vec_f32 *)(in0 + 0), (vec_f32 *)(in + 0));
  vec_load_pair((vec_f32 *)(in0 + 2), (vec_f32 *)(in + 2));

  vec_mult4_mma(&out[0], in0 + 0, inp);
  vec_mult4_mma(&out[4], in0 + 2, inp);
}
#endif

FORCEINLINE void vec_reduce1_mma(__vector_quad *out, vec_f32 *temp, vec_f32 v_alpha, vec_f32 *vy0)
{
  __builtin_mma_disassemble_acc((void*)temp, &out[0]);

  vy0[0] += (temp[0] * v_alpha);
}

FORCEINLINE void vec_reduce2_mma(__vector_quad *out, vec_f32 *temp, vec_f32 v_alpha, vec_f32 *vy0)
{
  vec_reduce1_mma(&out[0], &temp[0], v_alpha, &vy0[0]);
  vec_reduce1_mma(&out[1], &temp[4], v_alpha, &vy0[1]);
}

#ifndef USE_MERGE_MMA
FORCEINLINE void vec_reduce8_mma(__vector_quad *out, vec_f32 *temp, vec_f32 v_alpha, vec_f32 *vy0)
{
  vec_reduce2_mma(&out[0], &temp[0],  v_alpha, vy0 + 0);
  vec_reduce2_mma(&out[2], &temp[8],  v_alpha, vy0 + 2);
  vec_reduce2_mma(&out[4], &temp[16], v_alpha, vy0 + 4);
  vec_reduce2_mma(&out[6], &temp[24], v_alpha, vy0 + 6);
}
#else
FORCEINLINE void vec_reduce44_mma(__vector_quad *out, vec_f32 *temp, vec_f32 v_alpha, vec_f32 *vy0)
{
  __builtin_mma_disassemble_acc((void*)temp, &out[0]);

  vy0[0] += (temp[0] * v_alpha);
  vy0[2] += (temp[1] * v_alpha);
  vy0[4] += (temp[2] * v_alpha);
  vy0[6] += (temp[3] * v_alpha);
}

FORCEINLINE void vec_reduce84_mma(__vector_quad *out, vec_f32 *temp, vec_f32 v_alpha, vec_f32 *vy0)
{
  vec_reduce44_mma(&out[0], &temp[0], v_alpha, vy0 + 0);
  vec_reduce44_mma(&out[1], &temp[4], v_alpha, vy0 + 1);
}

FORCEINLINE void vec_reduce88_mma(__vector_quad *out, vec_f32 *temp, vec_f32 v_alpha, vec_f32 *vy0)
{
  vec_reduce44_mma(&out[0], &temp[ 0], v_alpha, vy0 + 0);
  vec_reduce44_mma(&out[1], &temp[ 4], v_alpha, vy0 + 1);
  vec_reduce44_mma(&out[2], &temp[ 8], v_alpha, vy0 + 8);
  vec_reduce44_mma(&out[3], &temp[12], v_alpha, vy0 + 9);
}
#endif

FORCEINLINE void vec_mult11a_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 in1, vec_bf16 inp)
{
  vec_bf16 in00 = vec_mergeh(in0, in1);

  __builtin_mma_xvbf16ger2(out, (vec_uc8)inp, (vec_uc8)in00);
}

FORCEINLINE void vec_mult2a_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 in1, vec_bf16 inp)
{
  vec_bf16 in01 = vec_mergel(in0, in1);

  vec_mult11a_mma(&out[0], in0, in1, inp);

  __builtin_mma_xvbf16ger2(&out[1], (vec_uc8)inp, (vec_uc8)in01);
}

FORCEINLINE void vec_mult4a_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 inp)
{
  vec_mult2a_mma(out + 0, in0[0], in1[0], inp);
  vec_mult2a_mma(out + 2, in0[1], in1[1], inp);
}

FORCEINLINE void vec_loadN_mult11a_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(ina, n);
  vec_bf16 in1 = vec_loadN(inb, n);

  vec_mult11a_mma(out, in0, in1, inp);
}

FORCEINLINE void vec_load_mult22a_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp)
{
  vec_bf16 in0 = (vec_bf16)vec_load_vec(ina);
  vec_bf16 in1 = (vec_bf16)vec_load_vec(inb);

  vec_mult2a_mma(out, in0, in1, inp);
}

FORCEINLINE void vec_load4_mma(vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *ina, vec_bf16 *inb)
{
  vec_load_pair((vec_f32 *)(in0 + 0), (vec_f32 *)(ina + 0));
  vec_load_pair((vec_f32 *)(in1 + 0), (vec_f32 *)(inb + 0));
  vec_load_pair((vec_f32 *)(in0 + 2), (vec_f32 *)(ina + 2));
  vec_load_pair((vec_f32 *)(in1 + 2), (vec_f32 *)(inb + 2));
}

#ifndef USE_MERGE_MMA
FORCEINLINE void vec_load_mult28a_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp)
{
  vec_bf16 in0[4], in1[4];

  vec_load4_mma(in0, in1, ina, inb);

  vec_mult4a_mma(&out[0], in0 + 0, in1 + 0, inp);
  vec_mult4a_mma(&out[4], in0 + 2, in1 + 2, inp);
}
#endif

FORCEINLINE void vec_loadN_mult22a_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(ina, n);
  vec_bf16 in1 = vec_loadN(inb, n);

  vec_mult2a_mma(out, in0, in1, inp);
}

FORCEINLINE void vec_mult11b_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 in1, vec_bf16 inp)
{
  vec_bf16 in00 = vec_mergeh(in0, in1);

  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)inp, (vec_uc8)in00);
}

FORCEINLINE void vec_mult2b_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 in1, vec_bf16 inp)
{
  vec_bf16 in01 = vec_mergel(in0, in1);

  vec_mult11b_mma(&out[0], in0, in1, inp);

  __builtin_mma_xvbf16ger2pp(&out[1], (vec_uc8)inp, (vec_uc8)in01);
}

FORCEINLINE void vec_mult4b_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 inp)
{
  vec_mult2b_mma(out + 0, in0[0], in1[0], inp);
  vec_mult2b_mma(out + 2, in0[1], in1[1], inp);
}

#ifdef USE_MERGE_MMA
FORCEINLINE void vec_mult1c_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 inp)
{
  vec_bf16 in00 = vec_mergeh(in0, in0);

  __builtin_mma_xvbf16ger2pp(out, (vec_uc8)inp, (vec_uc8)in00);
}

FORCEINLINE void vec_mult2c_mma(__vector_quad *out, vec_bf16 in0, vec_bf16 inp)
{
  vec_bf16 in01 = vec_mergel(in0, in0);

  vec_mult1c_mma(&out[0], in0, inp);

  __builtin_mma_xvbf16ger2pp(&out[1], (vec_uc8)inp, (vec_uc8)in01);
}

FORCEINLINE void vec_mult44_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 *inp)
{
  vec_mult2_mma(out, in[0], inp[0]);
  vec_mult2c_mma(out, in[1], inp[1]);
}

FORCEINLINE void vec_mult44c_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 *inp)
{
  vec_mult2c_mma(out, in[0], inp[0]);
  vec_mult2c_mma(out, in[1], inp[1]);
}

FORCEINLINE void vec_mult44a_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *inp)
{
  vec_mult2a_mma(out, in0[0], in1[0], inp[0]);
  vec_mult2b_mma(out, in0[1], in1[1], inp[1]);
}

FORCEINLINE void vec_mult44b_mma(__vector_quad *out, vec_bf16 *in0, vec_bf16 *in1, vec_bf16 *inp)
{
  vec_mult2b_mma(out, in0[0], in1[0], inp[0]);
  vec_mult2b_mma(out, in0[1], in1[1], inp[1]);
}
#endif

FORCEINLINE void vec_loadN_mult11b_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(ina, n);
  vec_bf16 in1 = vec_loadN(inb, n);

  vec_mult11b_mma(out, in0, in1, inp);
}

FORCEINLINE void vec_load_mult22b_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp)
{
  vec_bf16 in0 = (vec_bf16)vec_load_vec(ina);
  vec_bf16 in1 = (vec_bf16)vec_load_vec(inb);

  vec_mult2b_mma(out, in0, in1, inp);
}

#ifndef USE_MERGE_MMA
FORCEINLINE void vec_load_mult28b_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp)
{
  vec_bf16 in0[4], in1[4];

  vec_load4_mma(in0, in1, ina, inb);

  vec_mult4b_mma(&out[0], in0 + 0, in1 + 0, inp);
  vec_mult4b_mma(&out[4], in0 + 2, in1 + 2, inp);
}
#else
FORCEINLINE void vec_load_mult184_mma(__vector_quad *out, vec_bf16 *in, vec_bf16 *inp)
{
  vec_bf16 in0[4];

  vec_load_pair((vec_f32 *)(in0 + 0), (vec_f32 *)(in + 0));
  vec_load_pair((vec_f32 *)(in0 + 2), (vec_f32 *)(in + 2));

  vec_mult44_mma(out, in0 + 0, inp + 0);
  vec_mult44c_mma(out, in0 + 2, inp + 2);
}

FORCEINLINE void vec_load_mult284a_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 *inp)
{
  vec_bf16 in0[4], in1[4];

  vec_load4_mma(in0, in1, ina, inb);

  vec_mult44a_mma(out, in0 + 0, in1 + 0, inp + 0);
  vec_mult44b_mma(out, in0 + 2, in1 + 2, inp + 2);
}

FORCEINLINE void vec_load_mult284b_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 *inp)
{
  vec_bf16 in0[4], in1[4];

  vec_load4_mma(in0, in1, ina, inb);

  vec_mult44b_mma(out, in0 + 0, in1 + 0, inp + 0);
  vec_mult44b_mma(out, in0 + 2, in1 + 2, inp + 2);
}

FORCEINLINE void vec_load_mult288a_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 *inp)
{
  vec_bf16 in0[8], in1[8];

  vec_load4_mma(in0 + 0, in1 + 0, ina + 0, inb + 0);
  vec_load4_mma(in0 + 4, in1 + 4, ina + 4, inb + 4);

  vec_mult44a_mma(out + 0, in0 + 0, in1 + 0, inp + 0);
  vec_mult44a_mma(out + 2, in0 + 4, in1 + 4, inp + 0);
  vec_mult44b_mma(out + 0, in0 + 2, in1 + 2, inp + 2);
  vec_mult44b_mma(out + 2, in0 + 6, in1 + 6, inp + 2);
}

FORCEINLINE void vec_load_mult288b_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 *inp)
{
  vec_bf16 in0[8], in1[8];

  vec_load4_mma(in0 + 0, in1 + 0, ina + 0, inb + 0);
  vec_load4_mma(in0 + 4, in1 + 4, ina + 4, inb + 4);

  vec_mult44b_mma(out + 0, in0 + 0, in1 + 0, inp + 0);
  vec_mult44b_mma(out + 2, in0 + 4, in1 + 4, inp + 0);
  vec_mult44b_mma(out + 0, in0 + 2, in1 + 2, inp + 2);
  vec_mult44b_mma(out + 2, in0 + 6, in1 + 6, inp + 2);
}
#endif

FORCEINLINE void vec_loadN_mult22b_mma(__vector_quad *out, vec_bf16 *ina, vec_bf16 *inb, vec_bf16 inp, BLASLONG n)
{
  vec_bf16 in0 = vec_loadN(ina, n);
  vec_bf16 in1 = vec_loadN(inb, n);

  vec_mult2b_mma(out, in0, in1, inp);
}

FORCEINLINE void vec_load4_pair(vec_f32 *vy0, vec_f32 *v_y)
{
  vec_load_pair(vy0 + 0, v_y + 0);
  vec_load_pair(vy0 + 2, v_y + 2);
  vec_load_pair(vy0 + 4, v_y + 4);
  vec_load_pair(vy0 + 6, v_y + 6);
}

FORCEINLINE void vec_store4_pair(vec_f32 *v_y, vec_f32 *vy0)
{
  vec_store_pair(v_y + 0, vy0 + 0);
  vec_store_pair(v_y + 2, vy0 + 2);
  vec_store_pair(v_y + 4, vy0 + 4);
  vec_store_pair(v_y + 6, vy0 + 6);
}

FORCEINLINE void vec_setzero_2(__vector_quad *temp0)
{
  __builtin_mma_xxsetaccz(&temp0[0]);
  __builtin_mma_xxsetaccz(&temp0[1]);
}

FORCEINLINE void vec_setzero_4(__vector_quad *temp0)
{
  vec_setzero_2(temp0 + 0);
  vec_setzero_2(temp0 + 2);
}

FORCEINLINE void vec_setzero_8(__vector_quad *temp0)
{
  vec_setzero_4(temp0 + 0);
  vec_setzero_4(temp0 + 4);
}

FORCEINLINE void vec_reduce_2(vec_f32 *temp00, __vector_quad *temp0)
{
  __builtin_mma_disassemble_acc((void*)(temp00 +  0), &temp0[0]);
  __builtin_mma_disassemble_acc((void*)(temp00 +  4), &temp0[1]);
}

FORCEINLINE void vec_reduce_4(vec_f32 *temp00, __vector_quad *temp0)
{
  vec_reduce_2(temp00 +  0, temp0 + 0);
  vec_reduce_2(temp00 +  8, temp0 + 2);
}

FORCEINLINE void vec_reduce_8(vec_f32 *temp00, __vector_quad *temp0)
{
  vec_reduce_4(temp00 +  0, temp0 + 0);
  vec_reduce_4(temp00 + 16, temp0 + 4);
}

#ifdef USE_MERGE_MMA
FORCEINLINE void vec_load8_pair(vec_f32 *vy0, vec_f32 *v_y)
{
  vec_load4_pair(vy0 +  0, v_y +  0);
  vec_load4_pair(vy0 +  8, v_y +  8);
}

FORCEINLINE void vec_store8_pair(vec_f32 *v_y, vec_f32 *vy0)
{
  vec_store4_pair(v_y +  0, vy0 +  0);
  vec_store4_pair(v_y +  8, vy0 +  8);
}

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define VEC_SHIFT(data, shift)  vec_sldw(data, data, 4 - shift)

#define MASK_0  0xf000
#define MASK_1  0x0f00
#define MASK_2  0x00f0
#define MASK_3  0x000f
#else
#define VEC_SHIFT(data, shift)  vec_sldw(data, data, shift)

#define MASK_0  0x000f
#define MASK_1  0x00f0
#define MASK_2  0x0f00
#define MASK_3  0xf000
#endif

FORCEINLINE void vec_make_mult1(vec_bf16 *v_x0, const bool mask)
{
  if (mask) {
    v_x0[ 0] = vec_and(v_x0[0], (vec_bf16)vec_genbm(MASK_0));
  }

  v_x0[ 1] = VEC_SHIFT(v_x0[ 0], 1);
  v_x0[ 2] = VEC_SHIFT(v_x0[ 0], 2);
  v_x0[ 3] = VEC_SHIFT(v_x0[ 0], 3);
}

FORCEINLINE void vec_make_mult2(vec_bf16 *v_x0)
{
  v_x0[ 5] = vec_and(v_x0[0], (vec_bf16)vec_genbm(MASK_1));
  vec_make_mult1(v_x0, true);

  v_x0[ 4] = VEC_SHIFT(v_x0[ 5], 3);
  v_x0[ 6] = VEC_SHIFT(v_x0[ 5], 1);
  v_x0[ 7] = VEC_SHIFT(v_x0[ 5], 2);
}

FORCEINLINE void vec_make_mult4(vec_bf16 *v_x0)
{
  v_x0[10] = vec_and(v_x0[0], (vec_bf16)vec_genbm(MASK_2));
  v_x0[15] = vec_and(v_x0[0], (vec_bf16)vec_genbm(MASK_3));
  vec_make_mult2(v_x0);

  v_x0[ 8] = VEC_SHIFT(v_x0[10], 2);
  v_x0[ 9] = VEC_SHIFT(v_x0[10], 3);
  v_x0[11] = VEC_SHIFT(v_x0[10], 1);
  v_x0[12] = VEC_SHIFT(v_x0[15], 1);
  v_x0[13] = VEC_SHIFT(v_x0[15], 2);
  v_x0[14] = VEC_SHIFT(v_x0[15], 3);
}
#endif

#endif

/*****************************************************************************
Copyright (c) 2020, The OpenBLAS Project
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
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

#include "common.h"

void openblas_warning(int verbose, const char * msg);

#ifdef SMALL_MATRIX_OPT
static int inner_small_matrix_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, IFLOAT *sa, IFLOAT *sb, BLASLONG mypos){
  int routine_mode;
#ifndef COMPLEX
  int (*gemm_small_kernel)(BLASLONG, BLASLONG, BLASLONG, FLOAT *, BLASLONG, FLOAT ,FLOAT *, BLASLONG, FLOAT, FLOAT *, BLASLONG);
  int (*gemm_small_kernel_b0)(BLASLONG, BLASLONG, BLASLONG, FLOAT *, BLASLONG, FLOAT, FLOAT *, BLASLONG, FLOAT *, BLASLONG);
#else
  int (*zgemm_small_kernel)(BLASLONG, BLASLONG, BLASLONG, FLOAT *, BLASLONG, FLOAT , FLOAT, FLOAT *, BLASLONG, FLOAT , FLOAT, FLOAT *, BLASLONG);
  int (*zgemm_small_kernel_b0)(BLASLONG, BLASLONG, BLASLONG, FLOAT *, BLASLONG, FLOAT , FLOAT, FLOAT *, BLASLONG, FLOAT *, BLASLONG);
  FLOAT alpha[2], beta[2];
#endif
  routine_mode=args->routine_mode;
  if((routine_mode & BLAS_SMALL_B0_OPT) == BLAS_SMALL_B0_OPT){
#ifndef COMPLEX
    gemm_small_kernel_b0=args->routine;
    gemm_small_kernel_b0(args->m, args->n, args->k, args->a, args->lda, *(FLOAT *)(args->alpha), args->b, args->ldb, args->c, args->ldc);
#else
    zgemm_small_kernel_b0=args->routine;
    alpha[0] = *((FLOAT *)args -> alpha + 0);
    alpha[1] = *((FLOAT *)args -> alpha + 1);
    zgemm_small_kernel_b0(args->m, args->n, args->k, args->a, args->lda, alpha[0], alpha[1], args->b, args->ldb, args->c, args->ldc);
#endif
    return(0);
  }else if(routine_mode & BLAS_SMALL_OPT){
#ifndef COMPLEX
    gemm_small_kernel=args->routine;
    gemm_small_kernel(args->m, args->n, args->k, args->a, args->lda, *(FLOAT *)(args->alpha), args->b, args->ldb, *(FLOAT *)(args->beta), args->c, args->ldc);
#else
    zgemm_small_kernel=args->routine;
    alpha[0] = *((FLOAT *)args -> alpha + 0);
    alpha[1] = *((FLOAT *)args -> alpha + 1);
    beta[0] = *((FLOAT *)args -> beta + 0);
    beta[1] = *((FLOAT *)args -> beta + 1);
    zgemm_small_kernel(args->m, args->n, args->k, args->a, args->lda, alpha[0], alpha[1], args->b, args->ldb, beta[0], beta[1], args->c, args->ldc);
#endif    
    return(0);
  }
  return(1);
}
#endif

int CNAME(blas_arg_t * args_array, BLASLONG nums){
  XFLOAT *buffer;
  XFLOAT *sa, *sb;
  int nthreads=1;
  int (*routine)(blas_arg_t *, void *, void *, XFLOAT *, XFLOAT *, BLASLONG);
  int i=0, /*j,*/ current_nums;

#ifdef SMP
  blas_queue_t * queue=NULL;
#endif
  
  if(nums <=0 ) return 0;

  buffer = (XFLOAT *)blas_memory_alloc(0);
  sa = (XFLOAT *)((BLASLONG)buffer +GEMM_OFFSET_A);
  sb = (XFLOAT *)(((BLASLONG)sa + ((GEMM_P * GEMM_Q * COMPSIZE * SIZE + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
  
#ifdef SMP
  nthreads=num_cpu_avail(3);

  if(nthreads==1){

#endif
    //single thread
    for(i=0; i<nums; i++){
      routine=args_array[i].routine;
#ifdef SMALL_MATRIX_OPT
      if(args_array[i].routine_mode & BLAS_SMALL_OPT){
	inner_small_matrix_thread(&args_array[i], NULL, NULL, NULL, NULL, 0);
      }else{
#endif      
	routine(&args_array[i], NULL, NULL, sa, sb, 0);
#ifdef SMALL_MATRIX_OPT
      }
#endif
    }
#ifdef SMP
  } else {
    //multi thread

    queue=(blas_queue_t *)malloc((nums+1) * sizeof(blas_queue_t));
    if(queue == NULL){
      openblas_warning(0, "memory alloc failed!\n");
      return(1);
    }
    for(i=0; i<nums; i++){
      queue[i].args=&args_array[i];
      queue[i].range_m=NULL;
      queue[i].range_n=NULL;
      queue[i].sa=NULL;
      queue[i].sb=NULL;
      queue[i].next=&queue[i+1];

      queue[i].mode=args_array[i].routine_mode;
      queue[i].routine=args_array[i].routine;
      
#ifdef SMALL_MATRIX_OPT
      if((args_array[i].routine_mode & BLAS_SMALL_B0_OPT) || (args_array[i].routine_mode & BLAS_SMALL_OPT)){
	queue[i].routine=inner_small_matrix_thread;
      }
#endif
    }
    
    for(i=0; i<nums; i+=nthreads){
      current_nums=((nums-i)>nthreads)? nthreads: (nums-i);

      queue[i].sa=sa;
      queue[i].sb=sb;
      queue[i+current_nums-1].next=NULL;
      
      exec_blas(current_nums, &queue[i]);
    }
    free(queue);
  }
#endif
  blas_memory_free(buffer);
  return 0;
}

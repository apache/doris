/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
//#include <sys/mman.h>
#include "common.h"

#ifndef USE_OPENMP

#include "blas_server.c"

#else

#ifndef likely
#ifdef __GNUC__
#define likely(x) __builtin_expect(!!(x), 1)
#else
#define likely(x) (x)
#endif
#endif
#ifndef unlikely
#ifdef __GNUC__
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define unlikely(x) (x)
#endif
#endif

#ifndef OMP_SCHED
#define OMP_SCHED static
#endif

int blas_server_avail = 0;
int blas_omp_number_max = 0;
int blas_omp_threads_local = 1;

extern int openblas_omp_adaptive_env(void);

static void * blas_thread_buffer[MAX_PARALLEL_NUMBER][MAX_CPU_NUMBER];
#ifdef HAVE_C11
static atomic_bool blas_buffer_inuse[MAX_PARALLEL_NUMBER];
#else
static _Bool blas_buffer_inuse[MAX_PARALLEL_NUMBER];
#endif

static void adjust_thread_buffers(void) {

  int i=0, j=0;

  //adjust buffer for each thread
  for(i=0; i < MAX_PARALLEL_NUMBER; i++) {
    for(j=0; j < blas_cpu_number; j++){
      if(blas_thread_buffer[i][j] == NULL){
        blas_thread_buffer[i][j] = blas_memory_alloc(2);
      }
    }
    for(; j < MAX_CPU_NUMBER; j++){
      if(blas_thread_buffer[i][j] != NULL){
        blas_memory_free(blas_thread_buffer[i][j]);
        blas_thread_buffer[i][j] = NULL;
      }
    }
  }
}

void goto_set_num_threads(int num_threads) {

  if (num_threads < 1) num_threads = blas_num_threads;

  if (num_threads > MAX_CPU_NUMBER) num_threads = MAX_CPU_NUMBER;

  if (num_threads > blas_num_threads) {
    blas_num_threads = num_threads;
  }

  blas_cpu_number  = num_threads;

  adjust_thread_buffers();
#if defined(ARCH_MIPS64) || defined(ARCH_LOONGARCH64)
#ifndef DYNAMIC_ARCH
  //set parameters for different number of threads.
  blas_set_parameter();
#endif
#endif

}
void openblas_set_num_threads(int num_threads) {

	goto_set_num_threads(num_threads);
}

#ifdef OS_LINUX

int openblas_setaffinity(int thread_idx, size_t cpusetsize, cpu_set_t* cpu_set) {
  fprintf(stderr,"OpenBLAS: use OpenMP environment variables for setting cpu affinity\n");
  return -1;
}
int openblas_getaffinity(int thread_idx, size_t cpusetsize, cpu_set_t* cpu_set) {
  fprintf(stderr,"OpenBLAS: use OpenMP environment variables for querying cpu affinity\n");
  return -1;
}
#endif

int blas_thread_init(void){

#if defined(__FreeBSD__) && defined(__clang__)
extern int openblas_omp_num_threads_env(void);

   if(blas_omp_number_max <= 0)
	   blas_omp_number_max= openblas_omp_num_threads_env();
   if (blas_omp_number_max <= 0) 
	   blas_omp_number_max=MAX_CPU_NUMBER;
#else
    blas_omp_number_max = omp_get_max_threads();
#endif

  blas_get_cpu_number();

  adjust_thread_buffers();

  blas_server_avail = 1;

  return 0;
}

int BLASFUNC(blas_thread_shutdown)(void){
  int i=0, j=0;
  blas_server_avail = 0;

  for(i=0; i<MAX_PARALLEL_NUMBER; i++) {
    for(j=0; j<MAX_CPU_NUMBER; j++){
      if(blas_thread_buffer[i][j]!=NULL){
        blas_memory_free(blas_thread_buffer[i][j]);
        blas_thread_buffer[i][j]=NULL;
      }
    }
  }

  return 0;
}

static void legacy_exec(void *func, int mode, blas_arg_t *args, void *sb){

      if (!(mode & BLAS_COMPLEX)){
#ifdef EXPRECISION
	if ((mode & BLAS_PREC) == BLAS_XDOUBLE){
	  /* REAL / Extended Double */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, xdouble,
			xdouble *, BLASLONG, xdouble *, BLASLONG,
			xdouble *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((xdouble *)args -> alpha)[0],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	} else
#endif
	  if ((mode & BLAS_PREC) == BLAS_DOUBLE){
	    /* REAL / Double */
	    void (*afunc)(BLASLONG, BLASLONG, BLASLONG, double,
			  double *, BLASLONG, double *, BLASLONG,
			  double *, BLASLONG, void *) = func;

	    afunc(args -> m, args -> n, args -> k,
		  ((double *)args -> alpha)[0],
		  args -> a, args -> lda,
		  args -> b, args -> ldb,
		  args -> c, args -> ldc, sb);
	  } else if ((mode & BLAS_PREC) == BLAS_SINGLE){
	    /* REAL / Single */
	    void (*afunc)(BLASLONG, BLASLONG, BLASLONG, float,
			  float *, BLASLONG, float *, BLASLONG,
			  float *, BLASLONG, void *) = func;

	    afunc(args -> m, args -> n, args -> k,
		  ((float *)args -> alpha)[0],
		  args -> a, args -> lda,
		  args -> b, args -> ldb,
		  args -> c, args -> ldc, sb);
#ifdef BUILD_BFLOAT16
          } else if ((mode & BLAS_PREC) == BLAS_BFLOAT16){
            /* REAL / BFLOAT16 */
            void (*afunc)(BLASLONG, BLASLONG, BLASLONG, bfloat16,
                          bfloat16 *, BLASLONG, bfloat16 *, BLASLONG,
                          bfloat16 *, BLASLONG, void *) = func;

            afunc(args -> m, args -> n, args -> k,
                  ((bfloat16 *)args -> alpha)[0],
                  args -> a, args -> lda,
                  args -> b, args -> ldb,
                  args -> c, args -> ldc, sb);
          } else if ((mode & BLAS_PREC) == BLAS_STOBF16){
            /* REAL / BLAS_STOBF16 */
            void (*afunc)(BLASLONG, BLASLONG, BLASLONG, float,
                          float *, BLASLONG, bfloat16 *, BLASLONG,
                          float *, BLASLONG, void *) = func;

            afunc(args -> m, args -> n, args -> k,
                  ((float *)args -> alpha)[0],
                  args -> a, args -> lda,
                  args -> b, args -> ldb,
                  args -> c, args -> ldc, sb);
          } else if ((mode & BLAS_PREC) == BLAS_DTOBF16){
            /* REAL / BLAS_DTOBF16 */
            void (*afunc)(BLASLONG, BLASLONG, BLASLONG, double,
                          double *, BLASLONG, bfloat16 *, BLASLONG,
                          double *, BLASLONG, void *) = func;

            afunc(args -> m, args -> n, args -> k,
                  ((double *)args -> alpha)[0],
                  args -> a, args -> lda,
                  args -> b, args -> ldb,
                  args -> c, args -> ldc, sb);
#endif
          } else {
            /* REAL / Other types in future */
	  }
      } else {
#ifdef EXPRECISION
	if ((mode & BLAS_PREC) == BLAS_XDOUBLE){
	  /* COMPLEX / Extended Double */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble,
			xdouble *, BLASLONG, xdouble *, BLASLONG,
			xdouble *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((xdouble *)args -> alpha)[0],
		((xdouble *)args -> alpha)[1],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	} else
#endif
	  if ((mode & BLAS_PREC) == BLAS_DOUBLE){
	    /* COMPLEX / Double */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, double, double,
			double *, BLASLONG, double *, BLASLONG,
			double *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((double *)args -> alpha)[0],
		((double *)args -> alpha)[1],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	  } else if ((mode & BLAS_PREC) == BLAS_SINGLE){
	    /* COMPLEX / Single */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, float, float,
			float *, BLASLONG, float *, BLASLONG,
			float *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((float *)args -> alpha)[0],
		((float *)args -> alpha)[1],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
      } else {
            /* COMPLEX / Other types in future */
	 }
   }
}

static void exec_threads(int thread_num, blas_queue_t *queue, int buf_index){

  void *buffer, *sa, *sb;
  int pos=0, release_flag=0;

  buffer = NULL;
  sa = queue -> sa;
  sb = queue -> sb;

#ifdef CONSISTENT_FPCSR
#ifdef __aarch64__
  __asm__ __volatile__ ("msr fpcr, %0" : : "r" (queue -> sse_mode));
#else
  __asm__ __volatile__ ("ldmxcsr %0" : : "m" (queue -> sse_mode));
  __asm__ __volatile__ ("fldcw %0"   : : "m" (queue -> x87_mode));
#endif
#endif

  if ((sa == NULL) && (sb == NULL) && ((queue -> mode & BLAS_PTHREAD) == 0)) {

    pos= thread_num;
    buffer = blas_thread_buffer[buf_index][pos];

    //fallback
    if(buffer==NULL) {
      buffer = blas_memory_alloc(2);
      release_flag=1;
    }

    if (sa == NULL) {
      sa = (void *)((BLASLONG)buffer + GEMM_OFFSET_A);
      queue->sa=sa;
    }

    if (sb == NULL) {
      if (!(queue -> mode & BLAS_COMPLEX)){
#ifdef EXPRECISION
	if ((queue -> mode & BLAS_PREC) == BLAS_XDOUBLE){
	  sb = (void *)(((BLASLONG)sa + ((QGEMM_P * QGEMM_Q * sizeof(xdouble)
					  + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	} else
#endif
	  if ((queue -> mode & BLAS_PREC) == BLAS_DOUBLE){
#if defined ( BUILD_DOUBLE) || defined (BUILD_COMPLEX16)
	    sb = (void *)(((BLASLONG)sa + ((DGEMM_P * DGEMM_Q * sizeof(double)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
#endif
	  } else if ((queue -> mode & BLAS_PREC) == BLAS_SINGLE){
#if defined (BUILD_SINGLE) || defined (BUILD_COMPLEX)
	    sb = (void *)(((BLASLONG)sa + ((SGEMM_P * SGEMM_Q * sizeof(float)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
#endif
	  } else {
          /* Other types in future */
	  }
      } else {
#ifdef EXPRECISION
	if ((queue -> mode & BLAS_PREC) == BLAS_XDOUBLE){
	  sb = (void *)(((BLASLONG)sa + ((XGEMM_P * XGEMM_Q * 2 * sizeof(xdouble)
					  + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	} else
#endif
	  if ((queue -> mode & BLAS_PREC) == BLAS_DOUBLE){
#ifdef BUILD_COMPLEX16
	    sb = (void *)(((BLASLONG)sa + ((ZGEMM_P * ZGEMM_Q * 2 * sizeof(double)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
#else 
fprintf(stderr,"UNHANDLED COMPLEX16\n");
#endif
	  } else if ((queue -> mode & BLAS_PREC) == BLAS_SINGLE) {
#ifdef BUILD_COMPLEX
	    sb = (void *)(((BLASLONG)sa + ((CGEMM_P * CGEMM_Q * 2 * sizeof(float)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
#else 
fprintf(stderr,"UNHANDLED COMPLEX\n");
#endif
	  } else {
          /* Other types in future */
	  }
      }
      queue->sb=sb;
    }
  }

  if (queue -> mode & BLAS_LEGACY) {
    legacy_exec(queue -> routine, queue -> mode, queue -> args, sb);
  } else
    if (queue -> mode & BLAS_PTHREAD) {
      void (*pthreadcompat)(void *) = queue -> routine;
      (pthreadcompat)(queue -> args);

    } else {
      int (*routine)(blas_arg_t *, void *, void *, void *, void *, BLASLONG) = queue -> routine;

      (routine)(queue -> args, queue -> range_m, queue -> range_n, sa, sb, queue -> position);

    }

  if (release_flag) blas_memory_free(buffer);

}

int exec_blas(BLASLONG num, blas_queue_t *queue){

  // Handle lazy re-init of the thread-pool after a POSIX fork
  if (unlikely(blas_server_avail == 0)) blas_thread_init();

  BLASLONG i, buf_index;

  if ((num <= 0) || (queue == NULL)) return 0;

#ifdef CONSISTENT_FPCSR
  for (i = 0; i < num; i ++) {
#ifdef __aarch64__
    __asm__ __volatile__ ("mrs %0, fpcr" : "=r" (queue[i].sse_mode));
#else
    __asm__ __volatile__ ("fnstcw %0"  : "=m" (queue[i].x87_mode));
    __asm__ __volatile__ ("stmxcsr %0" : "=m" (queue[i].sse_mode));
#endif
  }
#endif

while (true) {
    for(i=0; i < MAX_PARALLEL_NUMBER; i++) {
#ifdef HAVE_C11
      _Bool inuse = false;
      if(atomic_compare_exchange_weak(&blas_buffer_inuse[i], &inuse, true)) {
#else
      if(blas_buffer_inuse[i] == false) {
        blas_buffer_inuse[i] = true;
#endif
        buf_index = i;
        break;
      }
    }
    if(i != MAX_PARALLEL_NUMBER)
      break;
  }
  /*For caller-managed threading, if caller has registered the callback, pass exec_thread as callback function*/
  if (openblas_threads_callback_) {
#ifndef USE_SIMPLE_THREADED_LEVEL3
    for (i = 0; i < num; i ++)
      queue[i].position = i;
#endif
    openblas_threads_callback_(1, (openblas_dojob_callback) exec_threads, num, sizeof(blas_queue_t), (void*) queue, buf_index);
  } else {

 if (openblas_omp_adaptive_env() != 0) {
 #pragma omp parallel for num_threads(num) schedule(OMP_SCHED)
  for (i = 0; i < num; i ++) {
#ifndef USE_SIMPLE_THREADED_LEVEL3
    queue[i].position = i;
#endif
  exec_threads(omp_get_thread_num(), &queue[i], buf_index);
  }
} else {
#pragma omp parallel for schedule(OMP_SCHED)
  for (i = 0; i < num; i ++) {

#ifndef USE_SIMPLE_THREADED_LEVEL3
    queue[i].position = i;
#endif

  exec_threads(omp_get_thread_num(), &queue[i], buf_index);
  }
}
}

#ifdef HAVE_C11
  atomic_store(&blas_buffer_inuse[buf_index], false);
#else
  blas_buffer_inuse[buf_index] = false;
#endif

  return 0;
}

#endif

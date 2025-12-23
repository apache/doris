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

#include <stdio.h>
#include "common.h"
#ifdef OS_LINUX
#include <sys/sysinfo.h>
#include <sched.h>
#include <errno.h>
#include <linux/unistd.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif

#ifdef OS_HAIKU
#include <unistd.h>
#endif

#if defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN)
#include <sys/sysctl.h>
#include <sys/resource.h>
#endif


#define FIXED_PAGESIZE 4096


void *sa = NULL;
void *sb = NULL;
static double static_buffer[BUFFER_SIZE/sizeof(double)];

void *blas_memory_alloc(int numproc){

  if (sa == NULL){
#if 0
    sa = (void *)qalloc(QFAST, BUFFER_SIZE);
#else
    sa = (void *)malloc(BUFFER_SIZE);
#endif
    sb = (void *)&static_buffer[0];
  }

  return sa;
}

void blas_memory_free(void *free_area){
  return;
}



extern void openblas_warning(int verbose, const char * msg);

#ifndef SMP

#define blas_cpu_number 1
#define blas_num_threads 1

/* Dummy Function */
int  goto_get_num_procs  (void) { return 1;};
void goto_set_num_threads(int num_threads) {};

#else

#if defined(OS_LINUX) || defined(OS_SUNOS)
#ifndef NO_AFFINITY
int get_num_procs(void);
#else
int get_num_procs(void) {

  static int nums = 0;
  cpu_set_t cpuset,*cpusetp;
  size_t size;
  int ret;

#if defined(__GLIBC_PREREQ)
#if !__GLIBC_PREREQ(2, 7)
  int i;
#if !__GLIBC_PREREQ(2, 6)
  int n;
#endif
#endif
#endif

  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
#if !defined(OS_LINUX)
  return nums;
#endif

/*
#if !defined(__GLIBC_PREREQ)
  return nums;
#else
 #if !__GLIBC_PREREQ(2, 3)
  return nums;
 #endif

 #if !__GLIBC_PREREQ(2, 7)
  ret = sched_getaffinity(0,sizeof(cpuset), &cpuset);
  if (ret!=0) return nums;
  n=0;
  #if !__GLIBC_PREREQ(2, 6)
  for (i=0;i<nums;i++)
     if (CPU_ISSET(i,&cpuset)) n++;
  nums=n;
  #else
  nums = CPU_COUNT(sizeof(cpuset),&cpuset);
  #endif
  return nums;
 #else
  if (nums >= CPU_SETSIZE) {
    cpusetp = CPU_ALLOC(nums);
      if (cpusetp == NULL) {
        return nums;
      }
    size = CPU_ALLOC_SIZE(nums);
    ret = sched_getaffinity(0,size,cpusetp);
    if (ret!=0) {
      CPU_FREE(cpusetp);
      return nums;
    }
    ret = CPU_COUNT_S(size,cpusetp);
    if (ret > 0 && ret < nums) nums = ret;	
    CPU_FREE(cpusetp);
    return nums;
  } else {
    ret = sched_getaffinity(0,sizeof(cpuset),&cpuset);
    if (ret!=0) {
      return nums;
    }
    ret = CPU_COUNT(&cpuset);
    if (ret > 0 && ret < nums) nums = ret;	
    return nums;
  }
 #endif
#endif
*/
   return 1;
}
#endif
#endif

#ifdef OS_ANDROID
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_HAIKU
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_AIX
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_WINDOWS

int get_num_procs(void) {

  static int nums = 0;

  if (nums == 0) {

    SYSTEM_INFO sysinfo;

    GetSystemInfo(&sysinfo);

    nums = sysinfo.dwNumberOfProcessors;
  }

  return nums;
}

#endif

#if defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLY)

int get_num_procs(void) {

  static int nums = 0;

  int m[2];
  size_t len;

  if (nums == 0) {
    m[0] = CTL_HW;
    m[1] = HW_NCPU;
    len = sizeof(int);
    sysctl(m, 2, &nums, &len, NULL, 0);
  }

  return nums;
}

#endif

#if defined(OS_DARWIN)
int get_num_procs(void) {
  static int nums = 0;
  size_t len;
  if (nums == 0){
    len = sizeof(int);
    sysctlbyname("hw.physicalcpu", &nums, &len, NULL, 0);
  }
  return nums;
}
/*
void set_stack_limit(int limitMB){
  int result=0;
  struct rlimit rl;
  rlim_t StackSize;

  StackSize=limitMB*1024*1024;
  result=getrlimit(RLIMIT_STACK, &rl);
  if(result==0){
    if(rl.rlim_cur < StackSize){
      rl.rlim_cur=StackSize;
      result=setrlimit(RLIMIT_STACK, &rl);
      if(result !=0){
        fprintf(stderr, "OpenBLAS: set stack limit error =%d\n", result);
      }
    }
  }
}
*/
#endif


/*
OpenBLAS uses the numbers of CPU cores in multithreading.
It can be set by openblas_set_num_threads(int num_threads);
*/
int blas_cpu_number  = 0;
/*
The numbers of threads in the thread pool.
This value is equal or large than blas_cpu_number. This means some threads are sleep.
*/
int blas_num_threads = 0;

int  goto_get_num_procs  (void) {
  return blas_cpu_number;
}

void openblas_fork_handler(void)
{
  // This handler shuts down the OpenBLAS-managed PTHREAD pool when OpenBLAS is
  // built with "make USE_OPENMP=0".
  // Hanging can still happen when OpenBLAS is built against the libgomp
  // implementation of OpenMP. The problem is tracked at:
  //   http://gcc.gnu.org/bugzilla/show_bug.cgi?id=60035
  // In the mean time build with USE_OPENMP=0 or link against another
  // implementation of OpenMP.
#if !((defined(OS_WINDOWS) && !defined(OS_CYGWIN_NT)) || defined(OS_ANDROID)) && defined(SMP_SERVER)
  int err;
  err = pthread_atfork ((void (*)(void)) BLASFUNC(blas_thread_shutdown), NULL, NULL);
  if(err != 0)
    openblas_warning(0, "OpenBLAS Warning ... cannot install fork handler. You may meet hang after fork.\n");
#endif
}

extern int openblas_num_threads_env(void);
extern int openblas_goto_num_threads_env(void);
extern int openblas_omp_num_threads_env(void);

int blas_get_cpu_number(void){
#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN) || defined(OS_ANDROID)
  int max_num;
#endif
  int blas_goto_num   = 0;
  int blas_omp_num    = 0;

  if (blas_num_threads) return blas_num_threads;

#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN) || defined(OS_ANDROID)
  max_num = get_num_procs();
#endif

  // blas_goto_num = 0;
#ifndef USE_OPENMP
  blas_goto_num=openblas_num_threads_env();
  if (blas_goto_num < 0) blas_goto_num = 0;

  if (blas_goto_num == 0) {
    blas_goto_num=openblas_goto_num_threads_env();
    if (blas_goto_num < 0) blas_goto_num = 0;
  }

#endif

  // blas_omp_num = 0;
  blas_omp_num=openblas_omp_num_threads_env();
  if (blas_omp_num < 0) blas_omp_num = 0;

  if (blas_goto_num > 0) blas_num_threads = blas_goto_num;
  else if (blas_omp_num > 0) blas_num_threads = blas_omp_num;
  else blas_num_threads = MAX_CPU_NUMBER;

#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN) || defined(OS_ANDROID)
  if (blas_num_threads > max_num) blas_num_threads = max_num;
#endif

  if (blas_num_threads > MAX_CPU_NUMBER) blas_num_threads = MAX_CPU_NUMBER;

#ifdef DEBUG
  printf( "Adjusted number of threads : %3d\n", blas_num_threads);
#endif

  blas_cpu_number = blas_num_threads;

  return blas_num_threads;
}
#endif


int openblas_get_num_procs(void) {
#ifndef SMP
  return 1;
#else
  return get_num_procs();
#endif
}

int openblas_get_num_threads(void) {
#ifndef SMP
  return 1;
#else
  // init blas_cpu_number if needed
  blas_get_cpu_number();
  return blas_cpu_number;
#endif
}

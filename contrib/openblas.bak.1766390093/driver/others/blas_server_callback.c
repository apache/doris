#include "common.h"

/* global variable to change threading backend from openblas-managed to caller-managed */
openblas_threads_callback openblas_threads_callback_ = 0;

/* non-threadsafe function should be called before any other
   openblas function to change how threads are managed */
   
void openblas_set_threads_callback_function(openblas_threads_callback callback)
{
  openblas_threads_callback_ = callback;
}
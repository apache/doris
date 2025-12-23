#include <stdio.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#ifndef CBLAS
float NAME(blasint *N, bfloat16 *x, blasint *INCX, bfloat16 *y, blasint *INCY){
   BLASLONG n    = *N;
   BLASLONG incx = *INCX;
   BLASLONG incy = *INCY;
   float ret;
   PRINT_DEBUG_NAME;

   if (n <= 0) return 0.;

   IDEBUG_START;
   FUNCTION_PROFILE_START();

   if (incx < 0) x -= (n - 1) * incx;
   if (incy < 0) y -= (n - 1) * incy;
   ret = BF16_DOT_K(n, x, incx, y, incy);

   FUNCTION_PROFILE_END(1, 2 * n, 2 * n);
   IDEBUG_END;

   return ret;
 }

#else

float CNAME(blasint n, bfloat16 *x, blasint incx, bfloat16 *y, blasint incy){

  float ret;
  PRINT_DEBUG_CNAME;

  if (n <= 0) return 0.;

  IDEBUG_START;
  FUNCTION_PROFILE_START();

  if (incx < 0) x -= (n - 1) * incx;
  if (incy < 0) y -= (n - 1) * incy;
  ret = BF16_DOT_K(n, x, incx, y, incy);

  FUNCTION_PROFILE_END(1, 2 * n, 2 * n);
  IDEBUG_END;

  return ret;
}

#endif

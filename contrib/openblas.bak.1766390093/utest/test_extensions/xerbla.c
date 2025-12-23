/*****************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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

static int link_xerbla=TRUE;
static int lerr, _info, ok;
static char *rout;

static void F77_xerbla(char *srname, void *vinfo)
{
   blasint info=*(blasint*)vinfo;

   if (link_xerbla)
   {
      link_xerbla = 0;
      return;
   }

   if (rout != NULL && strcmp(rout, srname) != 0){
      printf("***** XERBLA WAS CALLED WITH SRNAME = <%s> INSTEAD OF <%s> *******\n", srname, rout);
      ok = FALSE;
   }

   if (info != _info){
      printf("***** XERBLA WAS CALLED WITH INFO = %d INSTEAD OF %d in %s *******\n",info, _info, srname);
      lerr = TRUE;
      ok = FALSE;
   } else lerr = FALSE;
}

/**  
* error function redefinition 
*/
int BLASFUNC(xerbla)(char *name, blasint *info, blasint length)
{
  F77_xerbla(name, info);
  return 0;
}

int check_error(void) {
   if (lerr == TRUE ) {
       printf("***** ILLEGAL VALUE OF PARAMETER NUMBER %d NOT DETECTED BY %s *****\n", _info, rout);
      ok = FALSE;
   }
   lerr = TRUE;
   return ok;
}

void set_xerbla(char* current_rout, int expected_info){
   if (link_xerbla) /* call these first to link */
      F77_xerbla(rout, &_info);

   ok = TRUE;
   lerr = TRUE;
   _info = expected_info;
   rout = current_rout;
}

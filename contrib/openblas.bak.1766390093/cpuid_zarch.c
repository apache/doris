/**************************************************************************
  Copyright (c) 2016, The OpenBLAS Project
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

#include <string.h>

#include "cpuid_zarch.h"


void get_libname(void)
{
	int d = detect();
	printf("%s", cpuname_lower[d]);
}

char *get_corename(void)
{
	return cpuname[detect()];
}

void get_architecture(void)
{
	printf("ZARCH");
}

void get_subarchitecture(void)
{
	int d = detect();
	printf("%s", cpuname[d]);
}

void get_subdirname(void)
{
	printf("zarch");
}


void get_cpuconfig(void)
{

	int d = detect();
	switch (d){
	case CPU_GENERIC:
	  printf("#define ZARCH_GENERIC\n");
 	  printf("#define DTB_DEFAULT_ENTRIES 64\n");
	  break;
	case CPU_Z13:
	  printf("#define Z13\n");
	  printf("#define DTB_DEFAULT_ENTRIES 64\n");
	  break;
	case CPU_Z14:
	  printf("#define Z14\n");
	  printf("#define L1_DATA_SIZE 131072\n");
	  printf("#define L1_DATA_LINESIZE 256\n");
	  printf("#define L1_DATA_ASSOCIATIVE 8\n");
	  printf("#define L2_SIZE 4194304\n");
	  printf("#define L2_LINESIZE 256\n");
	  printf("#define L2_ASSOCIATIVE 8\n");
	  printf("#define DTB_DEFAULT_ENTRIES 64\n");
	  printf("#define DTB_SIZE 4096\n");
	  break;
	}
}

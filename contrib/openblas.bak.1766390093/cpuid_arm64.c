/**************************************************************************
  Copyright (c) 2013, The OpenBLAS Project
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

#include <stdlib.h>
#include <string.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
int32_t value;
size_t length=sizeof(value);
int64_t value64;
size_t length64=sizeof(value64);
#endif
#if (defined OS_LINUX || defined OS_ANDROID)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#ifndef HWCAP_CPUID
#define HWCAP_CPUID (1 << 11)
#endif
#ifndef HWCAP_SVE
#define HWCAP_SVE (1 << 22)
#endif
#if (defined OS_WINDOWS)
#include <winreg.h>
#endif

#define get_cpu_ftr(id, var) ({                                 \
                __asm__ __volatile__ ("mrs %0, "#id : "=r" (var));              \
        })
#endif

#define CPU_UNKNOWN     	0
#define CPU_ARMV8       	1
// Arm
#define CPU_CORTEXA53     2
#define CPU_CORTEXA55     14
#define CPU_CORTEXA57     3
#define CPU_CORTEXA72     4
#define CPU_CORTEXA73     5
#define CPU_CORTEXA76    23
#define CPU_NEOVERSEN1    11
#define CPU_NEOVERSEV1    16
#define CPU_NEOVERSEN2    17
#define CPU_NEOVERSEV2    24
#define CPU_CORTEXX1      18
#define CPU_CORTEXX2	  19
#define CPU_CORTEXA510	  20
#define CPU_CORTEXA710    21
// Qualcomm
#define CPU_FALKOR        6
// Cavium
#define CPU_THUNDERX      7
#define CPU_THUNDERX2T99  8
#define CPU_THUNDERX3T110 12
//Hisilicon
#define CPU_TSV110        9
// Ampere
#define CPU_EMAG8180	 10
// Apple
#define CPU_VORTEX       13
// Fujitsu
#define CPU_A64FX	 15
// Phytium
#define CPU_FT2000       22

static char *cpuname[] = {
  "UNKNOWN",
  "ARMV8" ,
  "CORTEXA53",
  "CORTEXA57",
  "CORTEXA72",
  "CORTEXA73",
  "FALKOR",
  "THUNDERX",
  "THUNDERX2T99",
  "TSV110",
  "EMAG8180",
  "NEOVERSEN1",
  "THUNDERX3T110",
  "VORTEX",
  "CORTEXA55",
  "A64FX",
  "NEOVERSEV1",
  "NEOVERSEN2",
  "CORTEXX1",
  "CORTEXX2",
  "CORTEXA510",
  "CORTEXA710",
  "FT2000",
  "CORTEXA76",
  "NEOVERSEV2"
};

static char *cpuname_lower[] = {
  "unknown",
  "armv8",
  "cortexa53",
  "cortexa57",
  "cortexa72",
  "cortexa73",
  "falkor",
  "thunderx",
  "thunderx2t99",
  "tsv110",
  "emag8180",
  "neoversen1",
  "thunderx3t110",
  "vortex",
  "cortexa55",
  "a64fx",
  "neoversev1",
  "neoversen2",
  "cortexx1",
  "cortexx2",
  "cortexa510",
  "cortexa710",
  "ft2000",
  "cortexa76",
  "neoversev2"
};

static int cpulowperf=0;
static int cpumidperf=0;
static int cpuhiperf=0;

int get_feature(char *search)
{

#if defined( __linux ) || defined( __NetBSD__ )
	FILE *infile;
  	char buffer[2048], *p,*t;
  	p = (char *) NULL ;

  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if (!strncmp("Features", buffer, 8))
		{
			p = strchr(buffer, ':') + 2;
			break;
		}
	}

	fclose(infile);


	if( p == NULL ) return 0;

	t = strtok(p," ");
	while( (t = strtok(NULL," ")))
	{
		if (!strcmp(t, search))   { return(1); }
	}

#endif
	return(0);
}
static int cpusort(const void *model1, const void *model2)
{
	return (*(int*)model2-*(int*)model1);
}

int detect(void)
{

#if defined( __linux ) || defined( __NetBSD__ )
	int n,i,ii;
	int midr_el1;
	int implementer;
	int cpucap[1024];
	int cpucores[1024];
	FILE *infile;
	char cpupart[6],cpuimpl[6];
	char *cpu_impl=NULL,*cpu_pt=NULL;
	char buffer[2048], *p, *cpu_part = NULL, *cpu_implementer = NULL;
	p = (char *) NULL ;
	cpulowperf=cpumidperf=cpuhiperf=0;
	for (i=0;i<1024;i++)cpucores[i]=0;
	n=0;
	infile = fopen("/sys/devices/system/cpu/possible", "r");
	if (!infile) {
		infile = fopen("/proc/cpuinfo", "r");
		while (fgets(buffer, sizeof(buffer), infile)) {
			if (!strncmp("processor", buffer, 9))
 			n++;
		}
	} else {
		fgets(buffer, sizeof(buffer), infile);
		sscanf(buffer,"0-%d",&n);
 		n++;
	}
        fclose(infile);

	cpu_implementer=NULL;
	for (i=0;i<n;i++){
		sprintf(buffer,"/sys/devices/system/cpu/cpu%d/regs/identification/midr_el1",i);
		infile= fopen(buffer,"r");
		if (!infile) {
			infile = fopen("/proc/cpuinfo", "r");
			for (ii=0;ii<n;ii++){
				cpu_part=NULL;cpu_implementer=NULL;
				while (fgets(buffer, sizeof(buffer), infile)) {
					if ((cpu_part != NULL) && (cpu_implementer != NULL)) {
						break;
					}

					if ((cpu_part == NULL) && !strncmp("CPU part", buffer, 8)) {
					cpu_pt = strchr(buffer, ':') + 2;
					cpu_part = strdup(cpu_pt);
					cpucores[i]=strtol(cpu_part,NULL,0);

					} else if ((cpu_implementer == NULL) && !strncmp("CPU implementer", buffer, 15)) {
					cpu_impl = strchr(buffer, ':') + 2;
					cpu_implementer = strdup(cpu_impl);
					}

				}
				if (strstr(cpu_implementer, "0x41")) {
					if (cpucores[ii] >= 0xd4b) cpuhiperf++;
				else
					if (cpucores[ii] >= 0xd07) cpumidperf++;
				else cpulowperf++;
				}
				else cpulowperf++;
			}
			fclose(infile);
			break;
		} else {
		        (void)fgets(buffer, sizeof(buffer), infile);
			midr_el1=strtoul(buffer,NULL,16);
			fclose(infile);
			implementer = (midr_el1 >> 24) & 0xFF;
			cpucores[i] = (midr_el1 >> 4)  & 0xFFF;
		sprintf(buffer,"/sys/devices/system/cpu/cpu%d/cpu_capacity",i);
		infile= fopen(buffer,"r");
		if (!infile) {
				if (implementer== 65) {
					if (cpucores[i] >= 0xd4b) cpuhiperf++;
				else
					if (cpucores[i] >= 0xd07) cpumidperf++;
				else cpulowperf++;
				}
				else cpulowperf++;
			} else {
		        (void)fgets(buffer, sizeof(buffer), infile);
				sscanf(buffer,"%d",&cpucap[i]);
					if (cpucap[i] >= 1000) cpuhiperf++;
				else
					if (cpucap[i] >= 500) cpumidperf++;
				else cpulowperf++;
        		fclose(infile);
			}
		}
		sprintf(cpuimpl,"0x%02x",implementer);
		cpu_implementer=strdup(cpuimpl);
	}
	qsort(cpucores,1024,sizeof(int),cpusort);
	sprintf(cpupart,"0x%03x",cpucores[0]);
	cpu_part=strdup(cpupart);
	if(cpu_part != NULL && cpu_implementer != NULL) {
    // Arm
    if (strstr(cpu_implementer, "0x41")) {
      if (strstr(cpu_part, "0xd03"))
        return CPU_CORTEXA53;
      else if (strstr(cpu_part, "0xd07"))
        return CPU_CORTEXA57;
      else if (strstr(cpu_part, "0xd08"))
        return CPU_CORTEXA72;
      else if (strstr(cpu_part, "0xd09"))
        return CPU_CORTEXA73;
      else if (strstr(cpu_part, "0xd0c"))
        return CPU_NEOVERSEN1;
      else if (strstr(cpu_part, "0xd40"))
        return CPU_NEOVERSEV1;
      else if (strstr(cpu_part, "0xd49"))
        return CPU_NEOVERSEN2;
      else if (strstr(cpu_part, "0xd05"))
	return CPU_CORTEXA55;
      else if (strstr(cpu_part, "0xd46"))
        return CPU_CORTEXA510;
      else if (strstr(cpu_part, "0xd47"))
	return CPU_CORTEXA710;
      else if (strstr(cpu_part, "0xd4d")) //A715
	return CPU_CORTEXA710;
      else if (strstr(cpu_part, "0xd44"))
        return CPU_CORTEXX1;
      else if (strstr(cpu_part, "0xd4c"))
	return CPU_CORTEXX2;
      else if (strstr(cpu_part, "0xd4e")) //X3
	return CPU_CORTEXX2;
      else if (strstr(cpu_part, "0xd4f")) //NVIDIA Grace et al.
        return CPU_NEOVERSEV2;
      else if (strstr(cpu_part, "0xd0b")) 
       return CPU_CORTEXA76;
    }
    // Qualcomm
    else if (strstr(cpu_implementer, "0x51") && strstr(cpu_part, "0xc00"))
      return CPU_FALKOR;
    // Cavium
    else if (strstr(cpu_implementer, "0x43") && strstr(cpu_part, "0x0a1"))
			return CPU_THUNDERX;
    else if (strstr(cpu_implementer, "0x43") && strstr(cpu_part, "0x0af"))
			return CPU_THUNDERX2T99;
    else if (strstr(cpu_implementer, "0x43") && strstr(cpu_part, "0x0b8"))
			return CPU_THUNDERX3T110;
    // HiSilicon
    else if (strstr(cpu_implementer, "0x48") && strstr(cpu_part, "0xd01"))
                        return CPU_TSV110;
    // Ampere
    else if (strstr(cpu_implementer, "0x50") && strstr(cpu_part, "0x000"))
                        return CPU_EMAG8180;
    // Fujitsu
    else if (strstr(cpu_implementer, "0x46") && strstr(cpu_part, "0x001"))
                        return CPU_A64FX;
    // Apple
    else if (strstr(cpu_implementer, "0x61") && strstr(cpu_part, "0x022"))
	    		return CPU_VORTEX;
   // Phytium
   else if (strstr(cpu_implementer, "0x70") && (strstr(cpu_part, "0x660") || strstr(cpu_part, "0x661") 
   			|| strstr(cpu_part, "0x662") || strstr(cpu_part, "0x663")))
	    		return CPU_FT2000;
	}

	p = (char *) NULL ;
	infile = fopen("/proc/cpuinfo", "r");
	while (fgets(buffer, sizeof(buffer), infile))
	{

		if ((!strncmp("model name", buffer, 10)) || (!strncmp("Processor", buffer, 9)) ||
		    (!strncmp("CPU architecture", buffer, 16)))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}

  	fclose(infile);

  	if(p != NULL)
	{

		if ((strstr(p, "AArch64")) || (strstr(p, "8")))
		{
			return CPU_ARMV8;

		}


	}
#else
#ifdef __APPLE__
	length64 = sizeof(value64);
	sysctlbyname("hw.ncpu",&value64,&length64,NULL,0);
	cpulowperf=value64;
	length64 = sizeof(value64);
	sysctlbyname("hw.nperflevels",&value64,&length64,NULL,0);
	if (value64 > 1) {
	length64 = sizeof(value64);
	sysctlbyname("hw.perflevel0.cpusperl2",&value64,&length64,NULL,0);
	cpuhiperf=value64;
	length64 = sizeof(value64);
	sysctlbyname("hw.perflevel1.cpusperl2",&value64,&length64,NULL,0);
	cpulowperf=value64;
	}
	length64 = sizeof(value64);
	sysctlbyname("hw.cpufamily",&value64,&length64,NULL,0);
	if (value64 ==131287967|| value64 == 458787763 ) return CPU_VORTEX; //A12/M1
	if (value64 == 3660830781) return CPU_VORTEX; //A15/M2
        if (value64 == 2271604202) return CPU_VORTEX; //A16/M3
        if (value64 == 1867590060) return CPU_VORTEX; //M4
#else
#ifdef OS_WINDOWS
	HKEY reghandle;
	HKEY hklm = HKEY_LOCAL_MACHINE;
	WCHAR valstring[512];
	PVOID pvalstring=valstring;
	DWORD size=sizeof (valstring);
	DWORD type=RRF_RT_ANY;
	DWORD flags=0;
	LPCWSTR subkey= L"HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0";
        LPCWSTR field=L"ProcessorNameString"; 
	LONG errcode=RegOpenKeyEx(HKEY_LOCAL_MACHINE,TEXT("Hardware\\Description\\System\\CentralProcessor\\0"), 0, KEY_READ, &reghandle);
	if (errcode != NO_ERROR) wprintf(L"Could not open registry key for proc0: %x\n",errcode);
        errcode=RegQueryValueEx(reghandle, "ProcessorNameString", NULL,NULL ,pvalstring,&size);
	if (errcode != ERROR_SUCCESS) wprintf(L"Error reading cpuname from registry:%x\n",errcode);
//wprintf(stderr,L"%s\n",(PWSTR)valstring);
	RegCloseKey(reghandle);
	if (strstr(valstring, "Snapdragon(R) X Elite")) return CPU_NEOVERSEN1;
	if (strstr(valstring, "Ampere(R) Altra")) return CPU_NEOVERSEN1;
	if (strstr(valstring, "Snapdragon (TM) 8cx Gen 3")) return CPU_CORTEXX1;
	if (strstr(valstring, "Snapdragon Compute Platform")) return CPU_CORTEXX1;
#endif
#endif
	return CPU_ARMV8;	
#endif

	return CPU_UNKNOWN;
}

char *get_corename(void)
{
	return cpuname[detect()];
}

void get_architecture(void)
{
	printf("ARM64");
}

void get_subarchitecture(void)
{
	int d = detect();
	printf("%s", cpuname[d]);
}

void get_subdirname(void)
{
	printf("arm64");
}

void get_cpucount(void)
{
int n=0;

#if defined( __linux ) || defined( __NetBSD__ )
	FILE *infile;
  	char buffer[2048], *p,*t;
  	p = (char *) NULL ;

  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if (!strncmp("processor", buffer, 9))
		n++;
  	}

  	fclose(infile);

	printf("#define NUM_CORES %d\n",n);
	if (cpulowperf >0)
	printf("#define NUM_CORES_LP %d\n",cpulowperf);
	if (cpumidperf >0)
	printf("#define NUM_CORES_MP %d\n",cpumidperf);
	if (cpuhiperf >0)
	printf("#define NUM_CORES_HP %d\n",cpuhiperf);
#endif
#ifdef __APPLE__
	length64 = sizeof(value64);
	sysctlbyname("hw.physicalcpu_max",&value,&length,NULL,0);
	printf("#define NUM_CORES %d\n",value);
	if (cpulowperf >0)
	printf("#define NUM_CORES_LP %d\n",cpulowperf);
	if (cpumidperf >0)
	printf("#define NUM_CORES_MP %d\n",cpumidperf);
	if (cpuhiperf >0)
	printf("#define NUM_CORES_HP %d\n",cpuhiperf);
#endif	
}



void get_cpuconfig(void)
{

  // All arches should define ARMv8
  printf("#define ARMV8\n");
  printf("#define HAVE_NEON\n"); // This shouldn't be necessary
  printf("#define HAVE_VFPV4\n"); // This shouldn't be necessary
	int d = detect();
	switch (d)
	{

	    case CPU_CORTEXA53:
	    case CPU_CORTEXA55:
	        printf("#define %s\n", cpuname[d]);
	      // Fall-through
	    case CPU_ARMV8:
	      // Minimum parameters for ARMv8 (based on A53)
	    	printf("#define L1_DATA_SIZE 32768\n");
	    	printf("#define L1_DATA_LINESIZE 64\n");
 	   	printf("#define L2_SIZE 262144\n");
	    	printf("#define L2_LINESIZE 64\n");
	    	printf("#define DTB_DEFAULT_ENTRIES 64\n");
 	   	printf("#define DTB_SIZE 4096\n");
 	   	printf("#define L2_ASSOCIATIVE 4\n");
			break;

	    case CPU_CORTEXA57:
	    case CPU_CORTEXA72:
	    case CPU_CORTEXA73:
      // Common minimum settings for these Arm cores
      // Can change a lot, but we need to be conservative
      // TODO: detect info from /sys if possible
      		printf("#define %s\n", cpuname[d]);
		printf("#define L1_CODE_SIZE 49152\n");
		printf("#define L1_CODE_LINESIZE 64\n");
		printf("#define L1_CODE_ASSOCIATIVE 3\n");
		printf("#define L1_DATA_SIZE 32768\n");
		printf("#define L1_DATA_LINESIZE 64\n");
		printf("#define L1_DATA_ASSOCIATIVE 2\n");
		printf("#define L2_SIZE 524288\n");
		printf("#define L2_LINESIZE 64\n");
		printf("#define L2_ASSOCIATIVE 16\n");
		printf("#define DTB_DEFAULT_ENTRIES 64\n");
		printf("#define DTB_SIZE 4096\n");
		break;
	    case CPU_NEOVERSEN1:
		printf("#define %s\n", cpuname[d]);
		printf("#define L1_CODE_SIZE 65536\n");
		printf("#define L1_CODE_LINESIZE 64\n");
		printf("#define L1_CODE_ASSOCIATIVE 4\n");
		printf("#define L1_DATA_SIZE 65536\n");
		printf("#define L1_DATA_LINESIZE 64\n");
		printf("#define L1_DATA_ASSOCIATIVE 4\n");
		printf("#define L2_SIZE 1048576\n");
		printf("#define L2_LINESIZE 64\n");
		printf("#define L2_ASSOCIATIVE 8\n");
		printf("#define DTB_DEFAULT_ENTRIES 48\n");
		printf("#define DTB_SIZE 4096\n");
		break;

	    case CPU_NEOVERSEV1:
                printf("#define HAVE_SVE 1\n");
            case CPU_CORTEXA76:
                printf("#define %s\n", cpuname[d]);
                printf("#define L1_CODE_SIZE 65536\n");
                printf("#define L1_CODE_LINESIZE 64\n");
                printf("#define L1_CODE_ASSOCIATIVE 4\n");
                printf("#define L1_DATA_SIZE 65536\n");
                printf("#define L1_DATA_LINESIZE 64\n");
                printf("#define L1_DATA_ASSOCIATIVE 4\n");
                printf("#define L2_SIZE 1048576\n");
                printf("#define L2_LINESIZE 64\n");
                printf("#define L2_ASSOCIATIVE 8\n");
                printf("#define DTB_DEFAULT_ENTRIES 48\n");
                printf("#define DTB_SIZE 4096\n");
                break;

	    case CPU_NEOVERSEN2:
                printf("#define %s\n", cpuname[d]);
                printf("#define L1_CODE_SIZE 65536\n");
                printf("#define L1_CODE_LINESIZE 64\n");
                printf("#define L1_CODE_ASSOCIATIVE 4\n");
                printf("#define L1_DATA_SIZE 65536\n");
                printf("#define L1_DATA_LINESIZE 64\n");
                printf("#define L1_DATA_ASSOCIATIVE 4\n");
                printf("#define L2_SIZE 1048576\n");
                printf("#define L2_LINESIZE 64\n");
                printf("#define L2_ASSOCIATIVE 8\n");
                printf("#define DTB_DEFAULT_ENTRIES 48\n");
                printf("#define DTB_SIZE 4096\n");
                printf("#define HAVE_SVE 1\n");
                break;
       case CPU_NEOVERSEV2:
                printf("#define ARMV9\n");
                printf("#define HAVE_SVE 1\n");
                 printf("#define %s\n", cpuname[d]);
                 printf("#define L1_CODE_SIZE 65536\n");
                 printf("#define L1_CODE_LINESIZE 64\n");
                 printf("#define L1_CODE_ASSOCIATIVE 4\n");
                 printf("#define L1_DATA_SIZE 65536\n");
                 printf("#define L1_DATA_LINESIZE 64\n");
                 printf("#define L1_DATA_ASSOCIATIVE 4\n");
                 printf("#define L2_SIZE 1048576\n");
                 printf("#define L2_LINESIZE 64\n");
                 printf("#define L2_ASSOCIATIVE 8\n");
                                                                // L1 Data TLB = 48 entries
                                                                // L2 Data TLB = 2048 entries
                 printf("#define DTB_DEFAULT_ENTRIES 48\n");
                 printf("#define DTB_SIZE 4096\n");  // Set to 4096 for symmetry with other configs.
		break;
	    case CPU_CORTEXA510:
	    case CPU_CORTEXA710:
	    case CPU_CORTEXX1:
	    case CPU_CORTEXX2:
		printf("#define ARMV9\n");
                printf("#define HAVE_SVE 1\n");
                printf("#define %s\n", cpuname[d]);
                printf("#define L1_CODE_SIZE 65536\n");
                printf("#define L1_CODE_LINESIZE 64\n");
                printf("#define L1_CODE_ASSOCIATIVE 4\n");
                printf("#define L1_DATA_SIZE 65536\n");
                printf("#define L1_DATA_LINESIZE 64\n");
                printf("#define L1_DATA_ASSOCIATIVE 4\n");
                printf("#define L2_SIZE 1048576\n");
                printf("#define L2_LINESIZE 64\n");
                printf("#define L2_ASSOCIATIVE 8\n");
		printf("#define DTB_DEFAULT_ENTRIES 64\n");
		printf("#define DTB_SIZE 4096\n");
		break;
	    case CPU_FALKOR:
	        printf("#define FALKOR\n");
	        printf("#define L1_CODE_SIZE 65536\n");
	        printf("#define L1_CODE_LINESIZE 64\n");
	        printf("#define L1_DATA_SIZE 32768\n");
	        printf("#define L1_DATA_LINESIZE 128\n");
	        printf("#define L2_SIZE 524288\n");
	        printf("#define L2_LINESIZE 64\n");
	        printf("#define DTB_DEFAULT_ENTRIES 64\n");
	        printf("#define DTB_SIZE 4096\n");
	        printf("#define L2_ASSOCIATIVE 16\n");
	        break;

	    case CPU_THUNDERX:
		printf("#define THUNDERX\n");
		printf("#define L1_DATA_SIZE 32768\n");
		printf("#define L1_DATA_LINESIZE 128\n");
		printf("#define L2_SIZE 16777216\n");
		printf("#define L2_LINESIZE 128\n");
		printf("#define DTB_DEFAULT_ENTRIES 64\n");
		printf("#define DTB_SIZE 4096\n");
		printf("#define L2_ASSOCIATIVE 16\n");
		break;

	    case CPU_THUNDERX2T99:
		printf("#define THUNDERX2T99                  \n");
		printf("#define L1_CODE_SIZE         32768    \n");
		printf("#define L1_CODE_LINESIZE     64       \n");
		printf("#define L1_CODE_ASSOCIATIVE  8        \n");
		printf("#define L1_DATA_SIZE         32768    \n");
		printf("#define L1_DATA_LINESIZE     64       \n");
		printf("#define L1_DATA_ASSOCIATIVE  8        \n");
		printf("#define L2_SIZE              262144   \n");
		printf("#define L2_LINESIZE          64       \n");
		printf("#define L2_ASSOCIATIVE       8        \n");
		printf("#define L3_SIZE              33554432 \n");
		printf("#define L3_LINESIZE          64       \n");
		printf("#define L3_ASSOCIATIVE       32       \n");
		printf("#define DTB_DEFAULT_ENTRIES  64       \n");
		printf("#define DTB_SIZE             4096     \n");
		break;
			
	    case CPU_TSV110:
		printf("#define TSV110                        \n");
		printf("#define L1_CODE_SIZE         65536    \n");
		printf("#define L1_CODE_LINESIZE     64       \n");
		printf("#define L1_CODE_ASSOCIATIVE  4        \n");
		printf("#define L1_DATA_SIZE         65536    \n");
		printf("#define L1_DATA_LINESIZE     64       \n");
		printf("#define L1_DATA_ASSOCIATIVE  4        \n");
		printf("#define L2_SIZE              524228   \n");
		printf("#define L2_LINESIZE          64       \n");
		printf("#define L2_ASSOCIATIVE       8        \n");
		printf("#define DTB_DEFAULT_ENTRIES  64       \n");
		printf("#define DTB_SIZE             4096     \n");
		break;	

	    case CPU_EMAG8180:
     		 // Minimum parameters for ARMv8 (based on A53)
		printf("#define EMAG8180\n");
    		printf("#define L1_CODE_SIZE 32768\n");
    		printf("#define L1_DATA_SIZE 32768\n");
    		printf("#define L1_DATA_LINESIZE 64\n");
    		printf("#define L2_SIZE 262144\n");
    		printf("#define L2_LINESIZE 64\n");
	    	printf("#define DTB_DEFAULT_ENTRIES 64\n");
	    	printf("#define DTB_SIZE 4096\n");
		break;

	    case CPU_THUNDERX3T110:
		printf("#define THUNDERX3T110                 \n");
		printf("#define L1_CODE_SIZE         65536    \n");
		printf("#define L1_CODE_LINESIZE     64       \n");
		printf("#define L1_CODE_ASSOCIATIVE  8        \n");
		printf("#define L1_DATA_SIZE         32768    \n");
		printf("#define L1_DATA_LINESIZE     64       \n");
		printf("#define L1_DATA_ASSOCIATIVE  8        \n");
		printf("#define L2_SIZE              524288   \n");
		printf("#define L2_LINESIZE          64       \n");
		printf("#define L2_ASSOCIATIVE       8        \n");
		printf("#define L3_SIZE              94371840 \n");
		printf("#define L3_LINESIZE          64       \n");
		printf("#define L3_ASSOCIATIVE       32       \n");
		printf("#define DTB_DEFAULT_ENTRIES  64       \n");
		printf("#define DTB_SIZE             4096     \n");
		break;
	    case CPU_VORTEX:
		printf("#define VORTEX			      \n");
#ifdef __APPLE__
		length64 = sizeof(value64);
		sysctlbyname("hw.l1icachesize",&value64,&length64,NULL,0);
		printf("#define L1_CODE_SIZE	     %lld       \n",value64);
		length64 = sizeof(value64);
		sysctlbyname("hw.cachelinesize",&value64,&length64,NULL,0);
		printf("#define L1_CODE_LINESIZE     %lld       \n",value64);
		printf("#define L1_DATA_LINESIZE     %lld       \n",value64);
		length64 = sizeof(value64);
		sysctlbyname("hw.l1dcachesize",&value64,&length64,NULL,0);
		printf("#define L1_DATA_SIZE	     %lld       \n",value64);
		length64 = sizeof(value64);
		sysctlbyname("hw.l2cachesize",&value64,&length64,NULL,0);
		printf("#define L2_SIZE	     %lld       \n",value64);
#endif	
		printf("#define DTB_DEFAULT_ENTRIES  64       \n");
		printf("#define DTB_SIZE             4096     \n");
		break;
	    case CPU_A64FX:
		printf("#define A64FX\n");
                printf("#define HAVE_SVE 1\n");
    		printf("#define L1_CODE_SIZE 65535\n");
    		printf("#define L1_DATA_SIZE 65535\n");
    		printf("#define L1_DATA_LINESIZE 256\n");
    		printf("#define L2_SIZE 8388608\n");
    		printf("#define L2_LINESIZE 256\n");
	    	printf("#define DTB_DEFAULT_ENTRIES 64\n");
	    	printf("#define DTB_SIZE 4096\n");
		break;
	    case CPU_FT2000:
		printf("#define FT2000\n");
    		printf("#define L1_CODE_SIZE 32768\n");
    		printf("#define L1_DATA_SIZE 32768\n");
    		printf("#define L1_DATA_LINESIZE 64\n");
    		printf("#define L2_SIZE 33554432\n");
    		printf("#define L2_LINESIZE 64\n");
	    	printf("#define DTB_DEFAULT_ENTRIES 64\n");
	    	printf("#define DTB_SIZE 4096\n");
		break;
	}
	get_cpucount();
}


void get_libname(void)
{
	int d = detect();
	printf("%s", cpuname_lower[d]);
}

void get_features(void)
{

#if defined( __linux ) || defined( __NetBSD__ )
	FILE *infile;
  	char buffer[2048], *p,*t;
  	p = (char *) NULL ;

  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if (!strncmp("Features", buffer, 8))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}

  	fclose(infile);


	if( p == NULL ) return;

	t = strtok(p," ");
	while( (t = strtok(NULL," ")))
	{
	}

#endif
	return;
}

/*****************************************************************************
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

#include <stdbool.h>

#include "common.h"

/*
 * OpenBLAS contains some kernels that are optimised for RVV 1.0.  Before we
 * can use these kernels we need to determine whether the device supports
 * RVV 1.0 and what the device's VLEN is.  Our strategy will be as follows.
 *
 * First we'll invoke the hwprobe syscall to detect RVV 1.0.  In an ideal world,
 * this is all we should need to do.  If the syscall is not implemented we
 * should be able to deduce that RVV 1.0 is not supported (as it was added to
 * Linux after hwprobe) and if the syscall is implemented we can use it to
 * determine whether RVV 1.0 is supported.  However, there are some riscv64
 * boards out there that implement RVV 1.0 but ship with a Linux kernel that
 * predates RVV vector support and hwprobe support.  These kernels contain
 * the backported RVV patches but not the hwprobe patches and so they
 * advertise support for RVV via hwcap.  To cater for these boards we need
 * to fall back to hwcap if hwprobe is not supported. Unfortunately, some
 * boards indicate support for RVV via hwcap even though they only support
 * RVV 0.7.1, which is incompatible with RVV 1.0.  So an additional check is
 * required to test if the devices advertising support for RVV via hwcap really
 * support RVV 1.0.  This test works by executing a vsetvli instruction that
 * sets the tail agnostic and mask agnostic bits in the vtype register.
 * These bits are not supported prior to RVV 0.9 so will cause the VIL bit to
 * be set on the VTYPE register in CPUs supporting 0.7.1.  If this bit is set
 * we can determine that RVV 1.0 is not supported.
 *
 * This approach is borrowed from
 * VideoLan dav1d:
 *   (https://code.videolan.org/videolan/dav1d/-/merge_requests/1629).
 *
 * We assume that if a kernel reports the presence of RVV via hwcap that
 * the device supports the vsetvli instruction.
 *
 * For now we're just going to invoke the hwprobe syscall directly, rather than
 * invoking it through glibc.  Support for hwprobe has been added to glibc but
 * at the time of writing this support has not yet been included in a glibc
 * release.  Once it has, it will be better to invoke hwprobe via glibc as doing
 * so should take advantage of the vdso entry and be more efficient.
 */

/*
 * This should work on Android as well but I have no way of testing.
 */

#if defined(OS_LINUX)
#include <unistd.h>
#include <sys/syscall.h>
#include <stdint.h>
#include <sys/auxv.h>

#define DETECT_RISCV64_HWCAP_ISA_V (1 << ('V' - 'A'))

struct riscv_hwprobe {
	int64_t key;
	uint64_t value;
};

/* The constants below are copied from
 * /usr/include/riscv64-linux-gnu/asm/hwprobe.h. We duplicate the
 *  constants as the header file from which they are copied will only
 *  be present if we're building on a device with Linux 6.5 or greater.
 */

#define RISCV_HWPROBE_KEY_IMA_EXT_0	4
#define		RISCV_HWPROBE_IMA_V		(1 << 2)

#ifndef NR_riscv_hwprobe
#ifndef NR_arch_specific_syscall
#define NR_arch_specific_syscall 244
#endif
#define NR_riscv_hwprobe (NR_arch_specific_syscall + 14)
#endif
#endif // defined(OS_LINUX)

unsigned detect_riscv64_get_vlenb(void);
uint64_t detect_riscv64_rvv100(void);

extern gotoblas_t gotoblas_RISCV64_GENERIC;
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL256B)
extern gotoblas_t gotoblas_RISCV64_ZVL256B;
#endif
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL128B)
extern gotoblas_t gotoblas_RISCV64_ZVL128B;
#endif

#define CPU_GENERIC         0
#define CPU_RISCV64_ZVL256B 1
#define CPU_RISCV64_ZVL128B 2

static char *cpuname[] = {
	"riscv64_generic",
	"riscv64_zvl256b",
	"riscv64_zvl128b"
};
#define NUM_CORETYPES (sizeof(cpuname)/sizeof(char*))

extern int openblas_verbose(void);
extern void openblas_warning(int verbose, const char* msg);

char* gotoblas_corename(void) {
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL256B)
	if (gotoblas == &gotoblas_RISCV64_ZVL256B)
		return cpuname[CPU_RISCV64_ZVL256B];
#endif
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL128B)
	if (gotoblas == &gotoblas_RISCV64_ZVL128B)
		return cpuname[CPU_RISCV64_ZVL128B];
#endif
	if (gotoblas == &gotoblas_RISCV64_GENERIC)
		return cpuname[CPU_GENERIC];

	return "unknown";
}

static gotoblas_t* get_coretype(void) {
	unsigned vlenb = 0;

#if !defined(OS_LINUX)
	return NULL;
#else

	/*
	 * See the hwprobe documentation
	 *
	 * ( https://docs.kernel.org/arch/riscv/hwprobe.html )
	 * for more details.
	 */

	struct riscv_hwprobe pairs[] = {
		{ .key = RISCV_HWPROBE_KEY_IMA_EXT_0, },
	};
	int ret = syscall(NR_riscv_hwprobe, pairs, 1, 0, NULL, 0);
	if (ret == 0) {
		if (!(pairs[0].value & RISCV_HWPROBE_IMA_V))
			return NULL;
	} else {
		if (!(getauxval(AT_HWCAP) & DETECT_RISCV64_HWCAP_ISA_V))
			return NULL;

		if (!detect_riscv64_rvv100())
			return NULL;
	}

	/*
	 * RVV 1.0 is supported.  We now just need to determine the coretype
	 * based on the VLEN.
	 */

	vlenb = detect_riscv64_get_vlenb();

	if (vlenb < 16)
		return NULL;
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL256B)
	if (vlenb >= 32)
		return &gotoblas_RISCV64_ZVL256B;
#endif

#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL128B)
	return &gotoblas_RISCV64_ZVL128B;
#else
	return NULL;
#endif

#endif  // !defined(OS_LINUX)
}

static gotoblas_t* force_coretype(char* coretype) {
	size_t i;
	char message[128];

	for (i = 0; i < NUM_CORETYPES && strcasecmp(coretype, cpuname[i]); i++);

	if (i == CPU_GENERIC)
		return &gotoblas_RISCV64_GENERIC;

	if (i == CPU_RISCV64_ZVL256B) {
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL256B)
		return &gotoblas_RISCV64_ZVL256B;
#else
		openblas_warning(1,
				 "riscv64_zvl256b support not compiled in\n");
		return NULL;
#endif
	}

	if (i == CPU_RISCV64_ZVL128B) {
#if !defined(DYNAMIC_LIST) || defined(DYN_RISCV64_ZVL128B)
		return &gotoblas_RISCV64_ZVL128B;
#else
		openblas_warning(1,
				 "riscv64_zvl128b support not compiled in\n");
		return NULL;
#endif
	}

	snprintf(message, sizeof(message), "Core not found: %s\n", coretype);
	openblas_warning(1, message);

	return NULL;
}

void gotoblas_dynamic_init(void) {

	char coremsg[128];
	char* p;

	if (gotoblas) return;

	p = getenv("OPENBLAS_CORETYPE");
	if (p)
		gotoblas = force_coretype(p);
	else
		gotoblas = get_coretype();

	if (!gotoblas) {
		snprintf(coremsg, sizeof(coremsg), "Falling back to generic riscv64 core\n");
		openblas_warning(1, coremsg);
		gotoblas = &gotoblas_RISCV64_GENERIC;
	}

	if (gotoblas->init) {
		snprintf(coremsg, sizeof(coremsg), "Core: %s\n",
			 gotoblas_corename());
		openblas_warning(2, coremsg);
		gotoblas->init();
		return;
	}

	openblas_warning(0, "OpenBLAS : Architecture Initialization failed. No initialization function found.\n");
	exit(1);
}

void gotoblas_dynamic_quit(void) {
	gotoblas = NULL;
}

#include <stdlib.h>

#define CPU_GENERIC     0
#define CPU_Z13         1
#define CPU_Z14         2
#define CPU_Z15         3

static char *cpuname[] = {
  "ZARCH_GENERIC",
  "Z13",
  "Z14",
  "Z15"
};

static char *cpuname_lower[] = {
  "zarch_generic",
  "z13",
  "z14",
  "z15"
};

// Guard the use of getauxval() on glibc version >= 2.16
#ifdef __GLIBC__
#include <features.h>
#if __GLIBC_PREREQ(2, 16)
#include <sys/auxv.h>
#define HAVE_GETAUXVAL 1

static unsigned long get_hwcap(void)
{
	unsigned long hwcap = getauxval(AT_HWCAP);
	char *maskenv;

	// honor requests for not using specific CPU features in LD_HWCAP_MASK
	maskenv = getenv("LD_HWCAP_MASK");
	if (maskenv)
		hwcap &= strtoul(maskenv, NULL, 0);

	return hwcap;
	// note that a missing auxval is interpreted as no capabilities
	// available, which is safe.
}

#else // __GLIBC_PREREQ(2, 16)
#warn "Cannot detect SIMD support in Z13 or newer architectures since glibc is older than 2.16"

static unsigned long get_hwcap(void) {
	// treat missing support for getauxval() as no capabilities available,
	// which is safe.
	return 0;
}
#endif // __GLIBC_PREREQ(2, 16)
#endif // __GLIBC

static int detect(void)
{
	unsigned long hwcap = get_hwcap();

	// Choose the architecture level for optimized kernels based on hardware
	// capability bits (just like glibc chooses optimized implementations).
	//
	// The hardware capability bits that are used here indicate both
	// hardware support for a particular ISA extension and the presence of
	// software support to enable its use. For example, when HWCAP_S390_VX
	// is set then both the CPU can execute SIMD instructions and the Linux
	// kernel can manage applications using the vector registers and SIMD
	// instructions.
	//
	// See glibc's sysdeps/s390/dl-procinfo.h for an overview (also in
	// sysdeps/unix/sysv/linux/s390/bits/hwcap.h) of the defined hardware
	// capability bits. They are derived from the information that the
	// "store facility list (extended)" instructions provide.
	// (https://sourceware.org/git/?p=glibc.git;a=blob_plain;f=sysdeps/s390/dl-procinfo.h;hb=HEAD)
	//
	// currently used:
	// HWCAP_S390_VX - vector facility for z/Architecture (introduced with
	//                 IBM z13), enables level CPU_Z13 (SIMD)
	// HWCAP_S390_VXE - vector enhancements facility 1 (introduced with IBM
	//                  z14), together with VX enables level CPU_Z14
	//                  (single-precision SIMD instructions)
	//
	// When you add optimized kernels that make use of other ISA extensions
	// (e.g., for exploiting the vector-enhancements facility 2 that was introduced
	// with IBM z15), then add a new architecture level (e.g., CPU_Z15) and gate
	// it on the hwcap that represents it here (e.g., HWCAP_S390_VXRS_EXT2
	// for the z15 vector enhancements).
	//
	// To learn the value of hwcaps on a given system, set the environment
	// variable LD_SHOW_AUXV and let ld.so dump it (e.g., by running
	// LD_SHOW_AUXV=1 /bin/true).
	// Also, the init function for dynamic arch support will print hwcaps
	// when OPENBLAS_VERBOSE is set to 2 or higher.
	if ((hwcap & HWCAP_S390_VX) && (hwcap & HWCAP_S390_VXE))
		return CPU_Z14;

	if (hwcap & HWCAP_S390_VX)
		return CPU_Z13;

	return CPU_GENERIC;
}


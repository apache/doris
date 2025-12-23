#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <complex.h>
#ifdef complex
#undef complex
#endif
#ifdef I
#undef I
#endif

#if defined(_WIN64)
typedef long long BLASLONG;
typedef unsigned long long BLASULONG;
#else
typedef long BLASLONG;
typedef unsigned long BLASULONG;
#endif

#ifdef LAPACK_ILP64
typedef BLASLONG blasint;
#if defined(_WIN64)
#define blasabs(x) llabs(x)
#else
#define blasabs(x) labs(x)
#endif
#else
typedef int blasint;
#define blasabs(x) abs(x)
#endif

typedef blasint integer;

typedef unsigned int uinteger;
typedef char *address;
typedef short int shortint;
typedef float real;
typedef double doublereal;
typedef struct { real r, i; } complex;
typedef struct { doublereal r, i; } doublecomplex;
#ifdef _MSC_VER
static inline _Fcomplex Cf(complex *z) {_Fcomplex zz={z->r , z->i}; return zz;}
static inline _Dcomplex Cd(doublecomplex *z) {_Dcomplex zz={z->r , z->i};return zz;}
static inline _Fcomplex * _pCf(complex *z) {return (_Fcomplex*)z;}
static inline _Dcomplex * _pCd(doublecomplex *z) {return (_Dcomplex*)z;}
#else
static inline _Complex float Cf(complex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double Cd(doublecomplex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex float * _pCf(complex *z) {return (_Complex float*)z;}
static inline _Complex double * _pCd(doublecomplex *z) {return (_Complex double*)z;}
#endif
#define pCf(z) (*_pCf(z))
#define pCd(z) (*_pCd(z))
typedef blasint logical;

typedef char logical1;
typedef char integer1;

#define TRUE_ (1)
#define FALSE_ (0)

/* Extern is for use with -E */
#ifndef Extern
#define Extern extern
#endif

/* I/O stuff */

typedef int flag;
typedef int ftnlen;
typedef int ftnint;

/*external read, write*/
typedef struct
{	flag cierr;
	ftnint ciunit;
	flag ciend;
	char *cifmt;
	ftnint cirec;
} cilist;

/*internal read, write*/
typedef struct
{	flag icierr;
	char *iciunit;
	flag iciend;
	char *icifmt;
	ftnint icirlen;
	ftnint icirnum;
} icilist;

/*open*/
typedef struct
{	flag oerr;
	ftnint ounit;
	char *ofnm;
	ftnlen ofnmlen;
	char *osta;
	char *oacc;
	char *ofm;
	ftnint orl;
	char *oblnk;
} olist;

/*close*/
typedef struct
{	flag cerr;
	ftnint cunit;
	char *csta;
} cllist;

/*rewind, backspace, endfile*/
typedef struct
{	flag aerr;
	ftnint aunit;
} alist;

/* inquire */
typedef struct
{	flag inerr;
	ftnint inunit;
	char *infile;
	ftnlen infilen;
	ftnint	*inex;	/*parameters in standard's order*/
	ftnint	*inopen;
	ftnint	*innum;
	ftnint	*innamed;
	char	*inname;
	ftnlen	innamlen;
	char	*inacc;
	ftnlen	inacclen;
	char	*inseq;
	ftnlen	inseqlen;
	char 	*indir;
	ftnlen	indirlen;
	char	*infmt;
	ftnlen	infmtlen;
	char	*inform;
	ftnint	informlen;
	char	*inunf;
	ftnlen	inunflen;
	ftnint	*inrecl;
	ftnint	*innrec;
	char	*inblank;
	ftnlen	inblanklen;
} inlist;

#define VOID void

union Multitype {	/* for multiple entry points */
	integer1 g;
	shortint h;
	integer i;
	/* longint j; */
	real r;
	doublereal d;
	complex c;
	doublecomplex z;
	};

typedef union Multitype Multitype;

struct Vardesc {	/* for Namelist */
	char *name;
	char *addr;
	ftnlen *dims;
	int  type;
	};
typedef struct Vardesc Vardesc;

struct Namelist {
	char *name;
	Vardesc **vars;
	int nvars;
	};
typedef struct Namelist Namelist;

#define abs(x) ((x) >= 0 ? (x) : -(x))
#define dabs(x) (fabs(x))
#define f2cmin(a,b) ((a) <= (b) ? (a) : (b))
#define f2cmax(a,b) ((a) >= (b) ? (a) : (b))
#define dmin(a,b) (f2cmin(a,b))
#define dmax(a,b) (f2cmax(a,b))
#define bit_test(a,b)	((a) >> (b) & 1)
#define bit_clear(a,b)	((a) & ~((uinteger)1 << (b)))
#define bit_set(a,b)	((a) |  ((uinteger)1 << (b)))

#define abort_() { sig_die("Fortran abort routine called", 1); }
#define c_abs(z) (cabsf(Cf(z)))
#define c_cos(R,Z) { pCf(R)=ccos(Cf(Z)); }
#ifdef _MSC_VER
#define c_div(c, a, b) {Cf(c)._Val[0] = (Cf(a)._Val[0]/Cf(b)._Val[0]); Cf(c)._Val[1]=(Cf(a)._Val[1]/Cf(b)._Val[1]);}
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/df(b)._Val[1]);}
#else
#define c_div(c, a, b) {pCf(c) = Cf(a)/Cf(b);}
#define z_div(c, a, b) {pCd(c) = Cd(a)/Cd(b);}
#endif
#define c_exp(R, Z) {pCf(R) = cexpf(Cf(Z));}
#define c_log(R, Z) {pCf(R) = clogf(Cf(Z));}
#define c_sin(R, Z) {pCf(R) = csinf(Cf(Z));}
//#define c_sqrt(R, Z) {*(R) = csqrtf(Cf(Z));}
#define c_sqrt(R, Z) {pCf(R) = csqrtf(Cf(Z));}
#define d_abs(x) (fabs(*(x)))
#define d_acos(x) (acos(*(x)))
#define d_asin(x) (asin(*(x)))
#define d_atan(x) (atan(*(x)))
#define d_atn2(x, y) (atan2(*(x),*(y)))
#define d_cnjg(R, Z) { pCd(R) = conj(Cd(Z)); }
#define r_cnjg(R, Z) { pCf(R) = conjf(Cf(Z)); }
#define d_cos(x) (cos(*(x)))
#define d_cosh(x) (cosh(*(x)))
#define d_dim(__a, __b) ( *(__a) > *(__b) ? *(__a) - *(__b) : 0.0 )
#define d_exp(x) (exp(*(x)))
#define d_imag(z) (cimag(Cd(z)))
#define r_imag(z) (cimagf(Cf(z)))
#define d_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define r_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define d_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define r_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define d_log(x) (log(*(x)))
#define d_mod(x, y) (fmod(*(x), *(y)))
#define u_nint(__x) ((__x)>=0 ? floor((__x) + .5) : -floor(.5 - (__x)))
#define d_nint(x) u_nint(*(x))
#define u_sign(__a,__b) ((__b) >= 0 ? ((__a) >= 0 ? (__a) : -(__a)) : -((__a) >= 0 ? (__a) : -(__a)))
#define d_sign(a,b) u_sign(*(a),*(b))
#define r_sign(a,b) u_sign(*(a),*(b))
#define d_sin(x) (sin(*(x)))
#define d_sinh(x) (sinh(*(x)))
#define d_sqrt(x) (sqrt(*(x)))
#define d_tan(x) (tan(*(x)))
#define d_tanh(x) (tanh(*(x)))
#define i_abs(x) abs(*(x))
#define i_dnnt(x) ((integer)u_nint(*(x)))
#define i_len(s, n) (n)
#define i_nint(x) ((integer)u_nint(*(x)))
#define i_sign(a,b) ((integer)u_sign((integer)*(a),(integer)*(b)))
#define pow_dd(ap, bp) ( pow(*(ap), *(bp)))
#define pow_si(B,E) spow_ui(*(B),*(E))
#define pow_ri(B,E) spow_ui(*(B),*(E))
#define pow_di(B,E) dpow_ui(*(B),*(E))
#define pow_zi(p, a, b) {pCd(p) = zpow_ui(Cd(a), *(b));}
#define pow_ci(p, a, b) {pCf(p) = cpow_ui(Cf(a), *(b));}
#define pow_zz(R,A,B) {pCd(R) = cpow(Cd(A),*(B));}
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/* procedure parameter types for -A and -C++ */


#ifdef __cplusplus
typedef logical (*L_fp)(...);
#else
typedef logical (*L_fp)();
#endif

static float spow_ui(float x, integer n) {
	float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static double dpow_ui(double x, integer n) {
	double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#ifdef _MSC_VER
static _Fcomplex cpow_ui(complex x, integer n) {
	complex pow={1.0,0.0}; unsigned long int u;
		if(n != 0) {
		if(n < 0) n = -n, x.r = 1/x.r, x.i=1/x.i;
		for(u = n; ; ) {
			if(u & 01) pow.r *= x.r, pow.i *= x.i;
			if(u >>= 1) x.r *= x.r, x.i *= x.i;
			else break;
		}
	}
	_Fcomplex p={pow.r, pow.i};
	return p;
}
#else
static _Complex float cpow_ui(_Complex float x, integer n) {
	_Complex float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
#ifdef _MSC_VER
static _Dcomplex zpow_ui(_Dcomplex x, integer n) {
	_Dcomplex pow={1.0,0.0}; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x._Val[0] = 1/x._Val[0], x._Val[1] =1/x._Val[1];
		for(u = n; ; ) {
			if(u & 01) pow._Val[0] *= x._Val[0], pow._Val[1] *= x._Val[1];
			if(u >>= 1) x._Val[0] *= x._Val[0], x._Val[1] *= x._Val[1];
			else break;
		}
	}
	_Dcomplex p = {pow._Val[0], pow._Val[1]};
	return p;
}
#else
static _Complex double zpow_ui(_Complex double x, integer n) {
	_Complex double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
static integer pow_ii(integer x, integer n) {
	integer pow; unsigned long int u;
	if (n <= 0) {
		if (n == 0 || x == 1) pow = 1;
		else if (x != -1) pow = x == 0 ? 1/x : 0;
		else n = -n;
	}
	if ((n > 0) || !(n == 0 || x == 1 || x != -1)) {
		u = n;
		for(pow = 1; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static integer dmaxloc_(double *w, integer s, integer e, integer *n)
{
	double m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static integer smaxloc_(float *w, integer s, integer e, integer *n)
{
	float m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static inline void cdotc_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i]))._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i]))._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i*incx]))._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i*incx]))._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i])) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i*incx])) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotc_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i]))._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i]))._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i*incx]))._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i*incx]))._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i])) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i*incx])) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif	
static inline void cdotu_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i])._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += Cf(&x[i])._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i*incx])._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cf(&x[i*incx])._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i]) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i*incx]) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotu_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i])._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += Cd(&x[i])._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i*incx])._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cd(&x[i*incx])._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i]) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i*incx]) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif
/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static integer c__1 = 1;
static logical c_true = TRUE_;
static logical c_false = FALSE_;

/* > \brief \b DTRSNA */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DTRSNA + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtrsna.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtrsna.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtrsna.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DTRSNA( JOB, HOWMNY, SELECT, N, T, LDT, VL, LDVL, VR, */
/*                          LDVR, S, SEP, MM, M, WORK, LDWORK, IWORK, */
/*                          INFO ) */

/*       CHARACTER          HOWMNY, JOB */
/*       INTEGER            INFO, LDT, LDVL, LDVR, LDWORK, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       INTEGER            IWORK( * ) */
/*       DOUBLE PRECISION   S( * ), SEP( * ), T( LDT, * ), VL( LDVL, * ), */
/*      $                   VR( LDVR, * ), WORK( LDWORK, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DTRSNA estimates reciprocal condition numbers for specified */
/* > eigenvalues and/or right eigenvectors of a real upper */
/* > quasi-triangular matrix T (or of any matrix Q*T*Q**T with Q */
/* > orthogonal). */
/* > */
/* > T must be in Schur canonical form (as returned by DHSEQR), that is, */
/* > block upper triangular with 1-by-1 and 2-by-2 diagonal blocks; each */
/* > 2-by-2 diagonal block has its diagonal elements equal and its */
/* > off-diagonal elements of opposite sign. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOB */
/* > \verbatim */
/* >          JOB is CHARACTER*1 */
/* >          Specifies whether condition numbers are required for */
/* >          eigenvalues (S) or eigenvectors (SEP): */
/* >          = 'E': for eigenvalues only (S); */
/* >          = 'V': for eigenvectors only (SEP); */
/* >          = 'B': for both eigenvalues and eigenvectors (S and SEP). */
/* > \endverbatim */
/* > */
/* > \param[in] HOWMNY */
/* > \verbatim */
/* >          HOWMNY is CHARACTER*1 */
/* >          = 'A': compute condition numbers for all eigenpairs; */
/* >          = 'S': compute condition numbers for selected eigenpairs */
/* >                 specified by the array SELECT. */
/* > \endverbatim */
/* > */
/* > \param[in] SELECT */
/* > \verbatim */
/* >          SELECT is LOGICAL array, dimension (N) */
/* >          If HOWMNY = 'S', SELECT specifies the eigenpairs for which */
/* >          condition numbers are required. To select condition numbers */
/* >          for the eigenpair corresponding to a real eigenvalue w(j), */
/* >          SELECT(j) must be set to .TRUE.. To select condition numbers */
/* >          corresponding to a complex conjugate pair of eigenvalues w(j) */
/* >          and w(j+1), either SELECT(j) or SELECT(j+1) or both, must be */
/* >          set to .TRUE.. */
/* >          If HOWMNY = 'A', SELECT is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix T. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] T */
/* > \verbatim */
/* >          T is DOUBLE PRECISION array, dimension (LDT,N) */
/* >          The upper quasi-triangular matrix T, in Schur canonical form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T. LDT >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] VL */
/* > \verbatim */
/* >          VL is DOUBLE PRECISION array, dimension (LDVL,M) */
/* >          If JOB = 'E' or 'B', VL must contain left eigenvectors of T */
/* >          (or of any Q*T*Q**T with Q orthogonal), corresponding to the */
/* >          eigenpairs specified by HOWMNY and SELECT. The eigenvectors */
/* >          must be stored in consecutive columns of VL, as returned by */
/* >          DHSEIN or DTREVC. */
/* >          If JOB = 'V', VL is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of the array VL. */
/* >          LDVL >= 1; and if JOB = 'E' or 'B', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] VR */
/* > \verbatim */
/* >          VR is DOUBLE PRECISION array, dimension (LDVR,M) */
/* >          If JOB = 'E' or 'B', VR must contain right eigenvectors of T */
/* >          (or of any Q*T*Q**T with Q orthogonal), corresponding to the */
/* >          eigenpairs specified by HOWMNY and SELECT. The eigenvectors */
/* >          must be stored in consecutive columns of VR, as returned by */
/* >          DHSEIN or DTREVC. */
/* >          If JOB = 'V', VR is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the array VR. */
/* >          LDVR >= 1; and if JOB = 'E' or 'B', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is DOUBLE PRECISION array, dimension (MM) */
/* >          If JOB = 'E' or 'B', the reciprocal condition numbers of the */
/* >          selected eigenvalues, stored in consecutive elements of the */
/* >          array. For a complex conjugate pair of eigenvalues two */
/* >          consecutive elements of S are set to the same value. Thus */
/* >          S(j), SEP(j), and the j-th columns of VL and VR all */
/* >          correspond to the same eigenpair (but not in general the */
/* >          j-th eigenpair, unless all eigenpairs are selected). */
/* >          If JOB = 'V', S is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] SEP */
/* > \verbatim */
/* >          SEP is DOUBLE PRECISION array, dimension (MM) */
/* >          If JOB = 'V' or 'B', the estimated reciprocal condition */
/* >          numbers of the selected eigenvectors, stored in consecutive */
/* >          elements of the array. For a complex eigenvector two */
/* >          consecutive elements of SEP are set to the same value. If */
/* >          the eigenvalues cannot be reordered to compute SEP(j), SEP(j) */
/* >          is set to 0; this can only occur when the true value would be */
/* >          very small anyway. */
/* >          If JOB = 'E', SEP is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] MM */
/* > \verbatim */
/* >          MM is INTEGER */
/* >          The number of elements in the arrays S (if JOB = 'E' or 'B') */
/* >           and/or SEP (if JOB = 'V' or 'B'). MM >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of elements of the arrays S and/or SEP actually */
/* >          used to store the estimated condition numbers. */
/* >          If HOWMNY = 'A', M is set to N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (LDWORK,N+6) */
/* >          If JOB = 'E', WORK is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDWORK */
/* > \verbatim */
/* >          LDWORK is INTEGER */
/* >          The leading dimension of the array WORK. */
/* >          LDWORK >= 1; and if JOB = 'V' or 'B', LDWORK >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (2*(N-1)) */
/* >          If JOB = 'E', IWORK is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2017 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The reciprocal of the condition number of an eigenvalue lambda is */
/* >  defined as */
/* > */
/* >          S(lambda) = |v**T*u| / (norm(u)*norm(v)) */
/* > */
/* >  where u and v are the right and left eigenvectors of T corresponding */
/* >  to lambda; v**T denotes the transpose of v, and norm(u) */
/* >  denotes the Euclidean norm. These reciprocal condition numbers always */
/* >  lie between zero (very badly conditioned) and one (very well */
/* >  conditioned). If n = 1, S(lambda) is defined to be 1. */
/* > */
/* >  An approximate error bound for a computed eigenvalue W(i) is given by */
/* > */
/* >                      EPS * norm(T) / S(i) */
/* > */
/* >  where EPS is the machine precision. */
/* > */
/* >  The reciprocal of the condition number of the right eigenvector u */
/* >  corresponding to lambda is defined as follows. Suppose */
/* > */
/* >              T = ( lambda  c  ) */
/* >                  (   0    T22 ) */
/* > */
/* >  Then the reciprocal condition number is */
/* > */
/* >          SEP( lambda, T22 ) = sigma-f2cmin( T22 - lambda*I ) */
/* > */
/* >  where sigma-f2cmin denotes the smallest singular value. We approximate */
/* >  the smallest singular value by the reciprocal of an estimate of the */
/* >  one-norm of the inverse of T22 - lambda*I. If n = 1, SEP(1) is */
/* >  defined to be abs(T(1,1)). */
/* > */
/* >  An approximate error bound for a computed right eigenvector VR(i) */
/* >  is given by */
/* > */
/* >                      EPS * norm(T) / SEP(i) */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dtrsna_(char *job, char *howmny, logical *select, 
	integer *n, doublereal *t, integer *ldt, doublereal *vl, integer *
	ldvl, doublereal *vr, integer *ldvr, doublereal *s, doublereal *sep, 
	integer *mm, integer *m, doublereal *work, integer *ldwork, integer *
	iwork, integer *info)
{
    /* System generated locals */
    integer t_dim1, t_offset, vl_dim1, vl_offset, vr_dim1, vr_offset, 
	    work_dim1, work_offset, i__1, i__2;
    doublereal d__1, d__2;

    /* Local variables */
    integer kase;
    doublereal cond;
    extern doublereal ddot_(integer *, doublereal *, integer *, doublereal *, 
	    integer *);
    logical pair;
    integer ierr;
    doublereal dumm, prod;
    integer ifst;
    doublereal lnrm;
    integer ilst;
    doublereal rnrm;
    extern doublereal dnrm2_(integer *, doublereal *, integer *);
    doublereal prod1, prod2;
    integer i__, j, k;
    doublereal scale, delta;
    extern logical lsame_(char *, char *);
    integer isave[3];
    logical wants;
    doublereal dummy[1];
    integer n2;
    extern /* Subroutine */ void dlacn2_(integer *, doublereal *, doublereal *,
	     integer *, doublereal *, integer *, integer *);
    extern doublereal dlapy2_(doublereal *, doublereal *);
    extern /* Subroutine */ void dlabad_(doublereal *, doublereal *);
    doublereal cs;
    extern doublereal dlamch_(char *);
    integer nn, ks;
    doublereal sn, mu;
    extern /* Subroutine */ void dlacpy_(char *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    doublereal bignum;
    logical wantbh;
    extern /* Subroutine */ void dlaqtr_(logical *, logical *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *, doublereal *,
	     doublereal *, doublereal *, integer *), dtrexc_(char *, integer *
	    , doublereal *, integer *, doublereal *, integer *, integer *, 
	    integer *, doublereal *, integer *);
    logical somcon;
    doublereal smlnum;
    logical wantsp;
    doublereal eps, est;


/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */


/*  ===================================================================== */


/*     Decode and test the input parameters */

    /* Parameter adjustments */
    --select;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --s;
    --sep;
    work_dim1 = *ldwork;
    work_offset = 1 + work_dim1 * 1;
    work -= work_offset;
    --iwork;

    /* Function Body */
    wantbh = lsame_(job, "B");
    wants = lsame_(job, "E") || wantbh;
    wantsp = lsame_(job, "V") || wantbh;

    somcon = lsame_(howmny, "S");

    *info = 0;
    if (! wants && ! wantsp) {
	*info = -1;
    } else if (! lsame_(howmny, "A") && ! somcon) {
	*info = -2;
    } else if (*n < 0) {
	*info = -4;
    } else if (*ldt < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldvl < 1 || wants && *ldvl < *n) {
	*info = -8;
    } else if (*ldvr < 1 || wants && *ldvr < *n) {
	*info = -10;
    } else {

/*        Set M to the number of eigenpairs for which condition numbers */
/*        are required, and test MM. */

	if (somcon) {
	    *m = 0;
	    pair = FALSE_;
	    i__1 = *n;
	    for (k = 1; k <= i__1; ++k) {
		if (pair) {
		    pair = FALSE_;
		} else {
		    if (k < *n) {
			if (t[k + 1 + k * t_dim1] == 0.) {
			    if (select[k]) {
				++(*m);
			    }
			} else {
			    pair = TRUE_;
			    if (select[k] || select[k + 1]) {
				*m += 2;
			    }
			}
		    } else {
			if (select[*n]) {
			    ++(*m);
			}
		    }
		}
/* L10: */
	    }
	} else {
	    *m = *n;
	}

	if (*mm < *m) {
	    *info = -13;
	} else if (*ldwork < 1 || wantsp && *ldwork < *n) {
	    *info = -16;
	}
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DTRSNA", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

    if (*n == 1) {
	if (somcon) {
	    if (! select[1]) {
		return;
	    }
	}
	if (wants) {
	    s[1] = 1.;
	}
	if (wantsp) {
	    sep[1] = (d__1 = t[t_dim1 + 1], abs(d__1));
	}
	return;
    }

/*     Get machine constants */

    eps = dlamch_("P");
    smlnum = dlamch_("S") / eps;
    bignum = 1. / smlnum;
    dlabad_(&smlnum, &bignum);

    ks = 0;
    pair = FALSE_;
    i__1 = *n;
    for (k = 1; k <= i__1; ++k) {

/*        Determine whether T(k,k) begins a 1-by-1 or 2-by-2 block. */

	if (pair) {
	    pair = FALSE_;
	    goto L60;
	} else {
	    if (k < *n) {
		pair = t[k + 1 + k * t_dim1] != 0.;
	    }
	}

/*        Determine whether condition numbers are required for the k-th */
/*        eigenpair. */

	if (somcon) {
	    if (pair) {
		if (! select[k] && ! select[k + 1]) {
		    goto L60;
		}
	    } else {
		if (! select[k]) {
		    goto L60;
		}
	    }
	}

	++ks;

	if (wants) {

/*           Compute the reciprocal condition number of the k-th */
/*           eigenvalue. */

	    if (! pair) {

/*              Real eigenvalue. */

		prod = ddot_(n, &vr[ks * vr_dim1 + 1], &c__1, &vl[ks * 
			vl_dim1 + 1], &c__1);
		rnrm = dnrm2_(n, &vr[ks * vr_dim1 + 1], &c__1);
		lnrm = dnrm2_(n, &vl[ks * vl_dim1 + 1], &c__1);
		s[ks] = abs(prod) / (rnrm * lnrm);
	    } else {

/*              Complex eigenvalue. */

		prod1 = ddot_(n, &vr[ks * vr_dim1 + 1], &c__1, &vl[ks * 
			vl_dim1 + 1], &c__1);
		prod1 += ddot_(n, &vr[(ks + 1) * vr_dim1 + 1], &c__1, &vl[(ks 
			+ 1) * vl_dim1 + 1], &c__1);
		prod2 = ddot_(n, &vl[ks * vl_dim1 + 1], &c__1, &vr[(ks + 1) * 
			vr_dim1 + 1], &c__1);
		prod2 -= ddot_(n, &vl[(ks + 1) * vl_dim1 + 1], &c__1, &vr[ks *
			 vr_dim1 + 1], &c__1);
		d__1 = dnrm2_(n, &vr[ks * vr_dim1 + 1], &c__1);
		d__2 = dnrm2_(n, &vr[(ks + 1) * vr_dim1 + 1], &c__1);
		rnrm = dlapy2_(&d__1, &d__2);
		d__1 = dnrm2_(n, &vl[ks * vl_dim1 + 1], &c__1);
		d__2 = dnrm2_(n, &vl[(ks + 1) * vl_dim1 + 1], &c__1);
		lnrm = dlapy2_(&d__1, &d__2);
		cond = dlapy2_(&prod1, &prod2) / (rnrm * lnrm);
		s[ks] = cond;
		s[ks + 1] = cond;
	    }
	}

	if (wantsp) {

/*           Estimate the reciprocal condition number of the k-th */
/*           eigenvector. */

/*           Copy the matrix T to the array WORK and swap the diagonal */
/*           block beginning at T(k,k) to the (1,1) position. */

	    dlacpy_("Full", n, n, &t[t_offset], ldt, &work[work_offset], 
		    ldwork);
	    ifst = k;
	    ilst = 1;
	    dtrexc_("No Q", n, &work[work_offset], ldwork, dummy, &c__1, &
		    ifst, &ilst, &work[(*n + 1) * work_dim1 + 1], &ierr);

	    if (ierr == 1 || ierr == 2) {

/*              Could not swap because blocks not well separated */

		scale = 1.;
		est = bignum;
	    } else {

/*              Reordering successful */

		if (work[work_dim1 + 2] == 0.) {

/*                 Form C = T22 - lambda*I in WORK(2:N,2:N). */

		    i__2 = *n;
		    for (i__ = 2; i__ <= i__2; ++i__) {
			work[i__ + i__ * work_dim1] -= work[work_dim1 + 1];
/* L20: */
		    }
		    n2 = 1;
		    nn = *n - 1;
		} else {

/*                 Triangularize the 2 by 2 block by unitary */
/*                 transformation U = [  cs   i*ss ] */
/*                                    [ i*ss   cs  ]. */
/*                 such that the (1,1) position of WORK is complex */
/*                 eigenvalue lambda with positive imaginary part. (2,2) */
/*                 position of WORK is the complex eigenvalue lambda */
/*                 with negative imaginary  part. */

		    mu = sqrt((d__1 = work[(work_dim1 << 1) + 1], abs(d__1))) 
			    * sqrt((d__2 = work[work_dim1 + 2], abs(d__2)));
		    delta = dlapy2_(&mu, &work[work_dim1 + 2]);
		    cs = mu / delta;
		    sn = -work[work_dim1 + 2] / delta;

/*                 Form */

/*                 C**T = WORK(2:N,2:N) + i*[rwork(1) ..... rwork(n-1) ] */
/*                                          [   mu                     ] */
/*                                          [         ..               ] */
/*                                          [             ..           ] */
/*                                          [                  mu      ] */
/*                 where C**T is transpose of matrix C, */
/*                 and RWORK is stored starting in the N+1-st column of */
/*                 WORK. */

		    i__2 = *n;
		    for (j = 3; j <= i__2; ++j) {
			work[j * work_dim1 + 2] = cs * work[j * work_dim1 + 2]
				;
			work[j + j * work_dim1] -= work[work_dim1 + 1];
/* L30: */
		    }
		    work[(work_dim1 << 1) + 2] = 0.;

		    work[(*n + 1) * work_dim1 + 1] = mu * 2.;
		    i__2 = *n - 1;
		    for (i__ = 2; i__ <= i__2; ++i__) {
			work[i__ + (*n + 1) * work_dim1] = sn * work[(i__ + 1)
				 * work_dim1 + 1];
/* L40: */
		    }
		    n2 = 2;
		    nn = *n - 1 << 1;
		}

/*              Estimate norm(inv(C**T)) */

		est = 0.;
		kase = 0;
L50:
		dlacn2_(&nn, &work[(*n + 2) * work_dim1 + 1], &work[(*n + 4) *
			 work_dim1 + 1], &iwork[1], &est, &kase, isave);
		if (kase != 0) {
		    if (kase == 1) {
			if (n2 == 1) {

/*                       Real eigenvalue: solve C**T*x = scale*c. */

			    i__2 = *n - 1;
			    dlaqtr_(&c_true, &c_true, &i__2, &work[(work_dim1 
				    << 1) + 2], ldwork, dummy, &dumm, &scale, 
				    &work[(*n + 4) * work_dim1 + 1], &work[(*
				    n + 6) * work_dim1 + 1], &ierr);
			} else {

/*                       Complex eigenvalue: solve */
/*                       C**T*(p+iq) = scale*(c+id) in real arithmetic. */

			    i__2 = *n - 1;
			    dlaqtr_(&c_true, &c_false, &i__2, &work[(
				    work_dim1 << 1) + 2], ldwork, &work[(*n + 
				    1) * work_dim1 + 1], &mu, &scale, &work[(*
				    n + 4) * work_dim1 + 1], &work[(*n + 6) * 
				    work_dim1 + 1], &ierr);
			}
		    } else {
			if (n2 == 1) {

/*                       Real eigenvalue: solve C*x = scale*c. */

			    i__2 = *n - 1;
			    dlaqtr_(&c_false, &c_true, &i__2, &work[(
				    work_dim1 << 1) + 2], ldwork, dummy, &
				    dumm, &scale, &work[(*n + 4) * work_dim1 
				    + 1], &work[(*n + 6) * work_dim1 + 1], &
				    ierr);
			} else {

/*                       Complex eigenvalue: solve */
/*                       C*(p+iq) = scale*(c+id) in real arithmetic. */

			    i__2 = *n - 1;
			    dlaqtr_(&c_false, &c_false, &i__2, &work[(
				    work_dim1 << 1) + 2], ldwork, &work[(*n + 
				    1) * work_dim1 + 1], &mu, &scale, &work[(*
				    n + 4) * work_dim1 + 1], &work[(*n + 6) * 
				    work_dim1 + 1], &ierr);

			}
		    }

		    goto L50;
		}
	    }

	    sep[ks] = scale / f2cmax(est,smlnum);
	    if (pair) {
		sep[ks + 1] = sep[ks];
	    }
	}

	if (pair) {
	    ++ks;
	}

L60:
	;
    }
    return;

/*     End of DTRSNA */

} /* dtrsna_ */


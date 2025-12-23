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
static doublereal c_b6 = 0.;
static integer c__0 = 0;
static doublereal c_b11 = 1.;

/* > \brief \b DLALSD uses the singular value decomposition of A to solve the least squares problem. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLALSD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlalsd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlalsd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlalsd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLALSD( UPLO, SMLSIZ, N, NRHS, D, E, B, LDB, RCOND, */
/*                          RANK, WORK, IWORK, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            INFO, LDB, N, NRHS, RANK, SMLSIZ */
/*       DOUBLE PRECISION   RCOND */
/*       INTEGER            IWORK( * ) */
/*       DOUBLE PRECISION   B( LDB, * ), D( * ), E( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLALSD uses the singular value decomposition of A to solve the least */
/* > squares problem of finding X to minimize the Euclidean norm of each */
/* > column of A*X-B, where A is N-by-N upper bidiagonal, and X and B */
/* > are N-by-NRHS. The solution X overwrites B. */
/* > */
/* > The singular values of A smaller than RCOND times the largest */
/* > singular value are treated as zero in solving the least squares */
/* > problem; in this case a minimum norm solution is returned. */
/* > The actual singular values are returned in D in ascending order. */
/* > */
/* > This code makes very mild assumptions about floating point */
/* > arithmetic. It will work on machines with a guard digit in */
/* > add/subtract, or on those binary machines without guard digits */
/* > which subtract like the Cray XMP, Cray YMP, Cray C 90, or Cray 2. */
/* > It could conceivably fail on hexadecimal or decimal machines */
/* > without guard digits, but we know of none. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >         = 'U': D and E define an upper bidiagonal matrix. */
/* >         = 'L': D and E define a  lower bidiagonal matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] SMLSIZ */
/* > \verbatim */
/* >          SMLSIZ is INTEGER */
/* >         The maximum size of the subproblems at the bottom of the */
/* >         computation tree. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >         The dimension of the  bidiagonal matrix.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >         The number of columns of B. NRHS must be at least 1. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >         On entry D contains the main diagonal of the bidiagonal */
/* >         matrix. On exit, if INFO = 0, D contains its singular values. */
/* > \endverbatim */
/* > */
/* > \param[in,out] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (N-1) */
/* >         Contains the super-diagonal entries of the bidiagonal matrix. */
/* >         On exit, E has been destroyed. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB,NRHS) */
/* >         On input, B contains the right hand sides of the least */
/* >         squares problem. On output, B contains the solution X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >         The leading dimension of B in the calling subprogram. */
/* >         LDB must be at least f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] RCOND */
/* > \verbatim */
/* >          RCOND is DOUBLE PRECISION */
/* >         The singular values of A less than or equal to RCOND times */
/* >         the largest singular value are treated as zero in solving */
/* >         the least squares problem. If RCOND is negative, */
/* >         machine precision is used instead. */
/* >         For example, if diag(S)*X=B were the least squares problem, */
/* >         where diag(S) is a diagonal matrix of singular values, the */
/* >         solution would be X(i) = B(i) / S(i) if S(i) is greater than */
/* >         RCOND*f2cmax(S), and X(i) = 0 if S(i) is less than or equal to */
/* >         RCOND*f2cmax(S). */
/* > \endverbatim */
/* > */
/* > \param[out] RANK */
/* > \verbatim */
/* >          RANK is INTEGER */
/* >         The number of singular values of A greater than RCOND times */
/* >         the largest singular value. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension at least */
/* >         (9*N + 2*N*SMLSIZ + 8*N*NLVL + N*NRHS + (SMLSIZ+1)**2), */
/* >         where NLVL = f2cmax(0, INT(log_2 (N/(SMLSIZ+1))) + 1). */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension at least */
/* >         (3*N*NLVL + 11*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >         = 0:  successful exit. */
/* >         < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >         > 0:  The algorithm failed to compute a singular value while */
/* >               working on the submatrix lying in rows and columns */
/* >               INFO/(N+1) through MOD(INFO,N+1). */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Ming Gu and Ren-Cang Li, Computer Science Division, University of */
/* >       California at Berkeley, USA \n */
/* >     Osni Marques, LBNL/NERSC, USA \n */

/*  ===================================================================== */
/* Subroutine */ void dlalsd_(char *uplo, integer *smlsiz, integer *n, integer 
	*nrhs, doublereal *d__, doublereal *e, doublereal *b, integer *ldb, 
	doublereal *rcond, integer *rank, doublereal *work, integer *iwork, 
	integer *info)
{
    /* System generated locals */
    integer b_dim1, b_offset, i__1, i__2;
    doublereal d__1;

    /* Local variables */
    integer difl, difr;
    doublereal rcnd;
    integer perm, nsub;
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    integer nlvl, sqre, bxst, c__, i__, j, k;
    doublereal r__;
    integer s, u;
    extern /* Subroutine */ void dgemm_(char *, char *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, integer *);
    integer z__;
    extern /* Subroutine */ void dcopy_(integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer poles, sizei, nsize, nwork, icmpq1, icmpq2;
    doublereal cs;
    extern doublereal dlamch_(char *);
    extern /* Subroutine */ void dlasda_(integer *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *, doublereal *,
	     doublereal *, integer *, integer *, integer *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, integer *,
	     integer *);
    integer bx;
    extern /* Subroutine */ void dlalsa_(integer *, integer *, integer *, 
	    integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, integer *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, integer *, integer *, 
	    integer *, integer *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, integer *, integer *);
    doublereal sn;
    extern /* Subroutine */ void dlascl_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, integer *, doublereal *, 
	    integer *, integer *);
    extern integer idamax_(integer *, doublereal *, integer *);
    integer st;
    extern /* Subroutine */ void dlasdq_(char *, integer *, integer *, integer 
	    *, integer *, integer *, doublereal *, doublereal *, doublereal *,
	     integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer vt;
    extern /* Subroutine */ void dlacpy_(char *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *), 
	    dlartg_(doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *), dlaset_(char *, integer *, integer *, doublereal *,
	     doublereal *, doublereal *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    integer givcol;
    extern doublereal dlanst_(char *, integer *, doublereal *, doublereal *);
    extern /* Subroutine */ void dlasrt_(char *, integer *, doublereal *, 
	    integer *);
    doublereal orgnrm;
    integer givnum, givptr, nm1, smlszp, st1;
    doublereal eps;
    integer iwk;
    doublereal tol;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    --e;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --work;
    --iwork;

    /* Function Body */
    *info = 0;

    if (*n < 0) {
	*info = -3;
    } else if (*nrhs < 1) {
	*info = -4;
    } else if (*ldb < 1 || *ldb < *n) {
	*info = -8;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DLALSD", &i__1, (ftnlen)6);
	return;
    }

    eps = dlamch_("Epsilon");

/*     Set up the tolerance. */

    if (*rcond <= 0. || *rcond >= 1.) {
	rcnd = eps;
    } else {
	rcnd = *rcond;
    }

    *rank = 0;

/*     Quick return if possible. */

    if (*n == 0) {
	return;
    } else if (*n == 1) {
	if (d__[1] == 0.) {
	    dlaset_("A", &c__1, nrhs, &c_b6, &c_b6, &b[b_offset], ldb);
	} else {
	    *rank = 1;
	    dlascl_("G", &c__0, &c__0, &d__[1], &c_b11, &c__1, nrhs, &b[
		    b_offset], ldb, info);
	    d__[1] = abs(d__[1]);
	}
	return;
    }

/*     Rotate the matrix if it is lower bidiagonal. */

    if (*(unsigned char *)uplo == 'L') {
	i__1 = *n - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    dlartg_(&d__[i__], &e[i__], &cs, &sn, &r__);
	    d__[i__] = r__;
	    e[i__] = sn * d__[i__ + 1];
	    d__[i__ + 1] = cs * d__[i__ + 1];
	    if (*nrhs == 1) {
		drot_(&c__1, &b[i__ + b_dim1], &c__1, &b[i__ + 1 + b_dim1], &
			c__1, &cs, &sn);
	    } else {
		work[(i__ << 1) - 1] = cs;
		work[i__ * 2] = sn;
	    }
/* L10: */
	}
	if (*nrhs > 1) {
	    i__1 = *nrhs;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = *n - 1;
		for (j = 1; j <= i__2; ++j) {
		    cs = work[(j << 1) - 1];
		    sn = work[j * 2];
		    drot_(&c__1, &b[j + i__ * b_dim1], &c__1, &b[j + 1 + i__ *
			     b_dim1], &c__1, &cs, &sn);
/* L20: */
		}
/* L30: */
	    }
	}
    }

/*     Scale. */

    nm1 = *n - 1;
    orgnrm = dlanst_("M", n, &d__[1], &e[1]);
    if (orgnrm == 0.) {
	dlaset_("A", n, nrhs, &c_b6, &c_b6, &b[b_offset], ldb);
	return;
    }

    dlascl_("G", &c__0, &c__0, &orgnrm, &c_b11, n, &c__1, &d__[1], n, info);
    dlascl_("G", &c__0, &c__0, &orgnrm, &c_b11, &nm1, &c__1, &e[1], &nm1, 
	    info);

/*     If N is smaller than the minimum divide size SMLSIZ, then solve */
/*     the problem with another solver. */

    if (*n <= *smlsiz) {
	nwork = *n * *n + 1;
	dlaset_("A", n, n, &c_b6, &c_b11, &work[1], n);
	dlasdq_("U", &c__0, n, n, &c__0, nrhs, &d__[1], &e[1], &work[1], n, &
		work[1], n, &b[b_offset], ldb, &work[nwork], info);
	if (*info != 0) {
	    return;
	}
	tol = rcnd * (d__1 = d__[idamax_(n, &d__[1], &c__1)], abs(d__1));
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if (d__[i__] <= tol) {
		dlaset_("A", &c__1, nrhs, &c_b6, &c_b6, &b[i__ + b_dim1], ldb);
	    } else {
		dlascl_("G", &c__0, &c__0, &d__[i__], &c_b11, &c__1, nrhs, &b[
			i__ + b_dim1], ldb, info);
		++(*rank);
	    }
/* L40: */
	}
	dgemm_("T", "N", n, nrhs, n, &c_b11, &work[1], n, &b[b_offset], ldb, &
		c_b6, &work[nwork], n);
	dlacpy_("A", n, nrhs, &work[nwork], n, &b[b_offset], ldb);

/*        Unscale. */

	dlascl_("G", &c__0, &c__0, &c_b11, &orgnrm, n, &c__1, &d__[1], n, 
		info);
	dlasrt_("D", n, &d__[1], info);
	dlascl_("G", &c__0, &c__0, &orgnrm, &c_b11, n, nrhs, &b[b_offset], 
		ldb, info);

	return;
    }

/*     Book-keeping and setting up some constants. */

    nlvl = (integer) (log((doublereal) (*n) / (doublereal) (*smlsiz + 1)) / 
	    log(2.)) + 1;

    smlszp = *smlsiz + 1;

    u = 1;
    vt = *smlsiz * *n + 1;
    difl = vt + smlszp * *n;
    difr = difl + nlvl * *n;
    z__ = difr + (nlvl * *n << 1);
    c__ = z__ + nlvl * *n;
    s = c__ + *n;
    poles = s + *n;
    givnum = poles + (nlvl << 1) * *n;
    bx = givnum + (nlvl << 1) * *n;
    nwork = bx + *n * *nrhs;

    sizei = *n + 1;
    k = sizei + *n;
    givptr = k + *n;
    perm = givptr + *n;
    givcol = perm + nlvl * *n;
    iwk = givcol + (nlvl * *n << 1);

    st = 1;
    sqre = 0;
    icmpq1 = 1;
    icmpq2 = 0;
    nsub = 0;

    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if ((d__1 = d__[i__], abs(d__1)) < eps) {
	    d__[i__] = d_sign(&eps, &d__[i__]);
	}
/* L50: */
    }

    i__1 = nm1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if ((d__1 = e[i__], abs(d__1)) < eps || i__ == nm1) {
	    ++nsub;
	    iwork[nsub] = st;

/*           Subproblem found. First determine its size and then */
/*           apply divide and conquer on it. */

	    if (i__ < nm1) {

/*              A subproblem with E(I) small for I < NM1. */

		nsize = i__ - st + 1;
		iwork[sizei + nsub - 1] = nsize;
	    } else if ((d__1 = e[i__], abs(d__1)) >= eps) {

/*              A subproblem with E(NM1) not too small but I = NM1. */

		nsize = *n - st + 1;
		iwork[sizei + nsub - 1] = nsize;
	    } else {

/*              A subproblem with E(NM1) small. This implies an */
/*              1-by-1 subproblem at D(N), which is not solved */
/*              explicitly. */

		nsize = i__ - st + 1;
		iwork[sizei + nsub - 1] = nsize;
		++nsub;
		iwork[nsub] = *n;
		iwork[sizei + nsub - 1] = 1;
		dcopy_(nrhs, &b[*n + b_dim1], ldb, &work[bx + nm1], n);
	    }
	    st1 = st - 1;
	    if (nsize == 1) {

/*              This is a 1-by-1 subproblem and is not solved */
/*              explicitly. */

		dcopy_(nrhs, &b[st + b_dim1], ldb, &work[bx + st1], n);
	    } else if (nsize <= *smlsiz) {

/*              This is a small subproblem and is solved by DLASDQ. */

		dlaset_("A", &nsize, &nsize, &c_b6, &c_b11, &work[vt + st1], 
			n);
		dlasdq_("U", &c__0, &nsize, &nsize, &c__0, nrhs, &d__[st], &e[
			st], &work[vt + st1], n, &work[nwork], n, &b[st + 
			b_dim1], ldb, &work[nwork], info);
		if (*info != 0) {
		    return;
		}
		dlacpy_("A", &nsize, nrhs, &b[st + b_dim1], ldb, &work[bx + 
			st1], n);
	    } else {

/*              A large problem. Solve it using divide and conquer. */

		dlasda_(&icmpq1, smlsiz, &nsize, &sqre, &d__[st], &e[st], &
			work[u + st1], n, &work[vt + st1], &iwork[k + st1], &
			work[difl + st1], &work[difr + st1], &work[z__ + st1],
			 &work[poles + st1], &iwork[givptr + st1], &iwork[
			givcol + st1], n, &iwork[perm + st1], &work[givnum + 
			st1], &work[c__ + st1], &work[s + st1], &work[nwork], 
			&iwork[iwk], info);
		if (*info != 0) {
		    return;
		}
		bxst = bx + st1;
		dlalsa_(&icmpq2, smlsiz, &nsize, nrhs, &b[st + b_dim1], ldb, &
			work[bxst], n, &work[u + st1], n, &work[vt + st1], &
			iwork[k + st1], &work[difl + st1], &work[difr + st1], 
			&work[z__ + st1], &work[poles + st1], &iwork[givptr + 
			st1], &iwork[givcol + st1], n, &iwork[perm + st1], &
			work[givnum + st1], &work[c__ + st1], &work[s + st1], 
			&work[nwork], &iwork[iwk], info);
		if (*info != 0) {
		    return;
		}
	    }
	    st = i__ + 1;
	}
/* L60: */
    }

/*     Apply the singular values and treat the tiny ones as zero. */

    tol = rcnd * (d__1 = d__[idamax_(n, &d__[1], &c__1)], abs(d__1));

    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {

/*        Some of the elements in D can be negative because 1-by-1 */
/*        subproblems were not solved explicitly. */

	if ((d__1 = d__[i__], abs(d__1)) <= tol) {
	    dlaset_("A", &c__1, nrhs, &c_b6, &c_b6, &work[bx + i__ - 1], n);
	} else {
	    ++(*rank);
	    dlascl_("G", &c__0, &c__0, &d__[i__], &c_b11, &c__1, nrhs, &work[
		    bx + i__ - 1], n, info);
	}
	d__[i__] = (d__1 = d__[i__], abs(d__1));
/* L70: */
    }

/*     Now apply back the right singular vectors. */

    icmpq2 = 1;
    i__1 = nsub;
    for (i__ = 1; i__ <= i__1; ++i__) {
	st = iwork[i__];
	st1 = st - 1;
	nsize = iwork[sizei + i__ - 1];
	bxst = bx + st1;
	if (nsize == 1) {
	    dcopy_(nrhs, &work[bxst], n, &b[st + b_dim1], ldb);
	} else if (nsize <= *smlsiz) {
	    dgemm_("T", "N", &nsize, nrhs, &nsize, &c_b11, &work[vt + st1], n,
		     &work[bxst], n, &c_b6, &b[st + b_dim1], ldb);
	} else {
	    dlalsa_(&icmpq2, smlsiz, &nsize, nrhs, &work[bxst], n, &b[st + 
		    b_dim1], ldb, &work[u + st1], n, &work[vt + st1], &iwork[
		    k + st1], &work[difl + st1], &work[difr + st1], &work[z__ 
		    + st1], &work[poles + st1], &iwork[givptr + st1], &iwork[
		    givcol + st1], n, &iwork[perm + st1], &work[givnum + st1],
		     &work[c__ + st1], &work[s + st1], &work[nwork], &iwork[
		    iwk], info);
	    if (*info != 0) {
		return;
	    }
	}
/* L80: */
    }

/*     Unscale and sort the singular values. */

    dlascl_("G", &c__0, &c__0, &c_b11, &orgnrm, n, &c__1, &d__[1], n, info);
    dlasrt_("D", n, &d__[1], info);
    dlascl_("G", &c__0, &c__0, &orgnrm, &c_b11, n, nrhs, &b[b_offset], ldb, 
	    info);

    return;

/*     End of DLALSD */

} /* dlalsd_ */


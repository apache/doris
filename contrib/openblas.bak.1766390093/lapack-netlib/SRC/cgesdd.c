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

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c_n1 = -1;
static integer c__0 = 0;
static integer c__1 = 1;

/* > \brief \b CGESDD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGESDD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgesdd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgesdd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgesdd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CGESDD( JOBZ, M, N, A, LDA, S, U, LDU, VT, LDVT, */
/*                          WORK, LWORK, RWORK, IWORK, INFO ) */

/*       CHARACTER          JOBZ */
/*       INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N */
/*       INTEGER            IWORK( * ) */
/*       REAL               RWORK( * ), S( * ) */
/*       COMPLEX            A( LDA, * ), U( LDU, * ), VT( LDVT, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CGESDD computes the singular value decomposition (SVD) of a complex */
/* > M-by-N matrix A, optionally computing the left and/or right singular */
/* > vectors, by using divide-and-conquer method. The SVD is written */
/* > */
/* >      A = U * SIGMA * conjugate-transpose(V) */
/* > */
/* > where SIGMA is an M-by-N matrix which is zero except for its */
/* > f2cmin(m,n) diagonal elements, U is an M-by-M unitary matrix, and */
/* > V is an N-by-N unitary matrix.  The diagonal elements of SIGMA */
/* > are the singular values of A; they are real and non-negative, and */
/* > are returned in descending order.  The first f2cmin(m,n) columns of */
/* > U and V are the left and right singular vectors of A. */
/* > */
/* > Note that the routine returns VT = V**H, not V. */
/* > */
/* > The divide and conquer algorithm makes very mild assumptions about */
/* > floating point arithmetic. It will work on machines with a guard */
/* > digit in add/subtract, or on those binary machines without guard */
/* > digits which subtract like the Cray X-MP, Cray Y-MP, Cray C-90, or */
/* > Cray-2. It could conceivably fail on hexadecimal or decimal machines */
/* > without guard digits, but we know of none. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBZ */
/* > \verbatim */
/* >          JOBZ is CHARACTER*1 */
/* >          Specifies options for computing all or part of the matrix U: */
/* >          = 'A':  all M columns of U and all N rows of V**H are */
/* >                  returned in the arrays U and VT; */
/* >          = 'S':  the first f2cmin(M,N) columns of U and the first */
/* >                  f2cmin(M,N) rows of V**H are returned in the arrays U */
/* >                  and VT; */
/* >          = 'O':  If M >= N, the first N columns of U are overwritten */
/* >                  in the array A and all rows of V**H are returned in */
/* >                  the array VT; */
/* >                  otherwise, all columns of U are returned in the */
/* >                  array U and the first M rows of V**H are overwritten */
/* >                  in the array A; */
/* >          = 'N':  no columns of U or rows of V**H are computed. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the input matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the input matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, */
/* >          if JOBZ = 'O',  A is overwritten with the first N columns */
/* >                          of U (the left singular vectors, stored */
/* >                          columnwise) if M >= N; */
/* >                          A is overwritten with the first M rows */
/* >                          of V**H (the right singular vectors, stored */
/* >                          rowwise) otherwise. */
/* >          if JOBZ .ne. 'O', the contents of A are destroyed. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL array, dimension (f2cmin(M,N)) */
/* >          The singular values of A, sorted so that S(i) >= S(i+1). */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is COMPLEX array, dimension (LDU,UCOL) */
/* >          UCOL = M if JOBZ = 'A' or JOBZ = 'O' and M < N; */
/* >          UCOL = f2cmin(M,N) if JOBZ = 'S'. */
/* >          If JOBZ = 'A' or JOBZ = 'O' and M < N, U contains the M-by-M */
/* >          unitary matrix U; */
/* >          if JOBZ = 'S', U contains the first f2cmin(M,N) columns of U */
/* >          (the left singular vectors, stored columnwise); */
/* >          if JOBZ = 'O' and M >= N, or JOBZ = 'N', U is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >          The leading dimension of the array U.  LDU >= 1; */
/* >          if JOBZ = 'S' or 'A' or JOBZ = 'O' and M < N, LDU >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] VT */
/* > \verbatim */
/* >          VT is COMPLEX array, dimension (LDVT,N) */
/* >          If JOBZ = 'A' or JOBZ = 'O' and M >= N, VT contains the */
/* >          N-by-N unitary matrix V**H; */
/* >          if JOBZ = 'S', VT contains the first f2cmin(M,N) rows of */
/* >          V**H (the right singular vectors, stored rowwise); */
/* >          if JOBZ = 'O' and M < N, or JOBZ = 'N', VT is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVT */
/* > \verbatim */
/* >          LDVT is INTEGER */
/* >          The leading dimension of the array VT.  LDVT >= 1; */
/* >          if JOBZ = 'A' or JOBZ = 'O' and M >= N, LDVT >= N; */
/* >          if JOBZ = 'S', LDVT >= f2cmin(M,N). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= 1. */
/* >          If LWORK = -1, a workspace query is assumed.  The optimal */
/* >          size for the WORK array is calculated and stored in WORK(1), */
/* >          and no other work except argument checking is performed. */
/* > */
/* >          Let mx = f2cmax(M,N) and mn = f2cmin(M,N). */
/* >          If JOBZ = 'N', LWORK >= 2*mn + mx. */
/* >          If JOBZ = 'O', LWORK >= 2*mn*mn + 2*mn + mx. */
/* >          If JOBZ = 'S', LWORK >=   mn*mn + 3*mn. */
/* >          If JOBZ = 'A', LWORK >=   mn*mn + 2*mn + mx. */
/* >          These are not tight minimums in all cases; see comments inside code. */
/* >          For good performance, LWORK should generally be larger; */
/* >          a query is recommended. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (MAX(1,LRWORK)) */
/* >          Let mx = f2cmax(M,N) and mn = f2cmin(M,N). */
/* >          If JOBZ = 'N',    LRWORK >= 5*mn (LAPACK <= 3.6 needs 7*mn); */
/* >          else if mx >> mn, LRWORK >= 5*mn*mn + 5*mn; */
/* >          else              LRWORK >= f2cmax( 5*mn*mn + 5*mn, */
/* >                                           2*mx*mn + 2*mn*mn + mn ). */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (8*f2cmin(M,N)) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  The updating process of SBDSDC did not converge. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complexGEsing */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Ming Gu and Huan Ren, Computer Science Division, University of */
/* >     California at Berkeley, USA */
/* > */
/*  ===================================================================== */
/* Subroutine */ void cgesdd_(char *jobz, integer *m, integer *n, complex *a, 
	integer *lda, real *s, complex *u, integer *ldu, complex *vt, integer 
	*ldvt, complex *work, integer *lwork, real *rwork, integer *iwork, 
	integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, u_dim1, u_offset, vt_dim1, vt_offset, i__1, 
	    i__2, i__3;

    /* Local variables */
    integer lwork_cunglq_mn__, lwork_cunglq_nn__, lwork_cungqr_mm__, 
	    lwork_cungqr_mn__;
    complex cdum[1];
    integer iscl, lwork_cunmbr_prc_mm__, lwork_cunmbr_prc_mn__, 
	    lwork_cunmbr_prc_nn__;
    real anrm;
    integer ierr, itau, lwork_cunmbr_qln_mm__, lwork_cunmbr_qln_mn__, 
	    lwork_cunmbr_qln_nn__, idum[1], irvt, i__;
    extern /* Subroutine */ void cgemm_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    extern logical lsame_(char *, char *);
    integer chunk, minmn, wrkbl, itaup, itauq;
    logical wntqa;
    integer nwork;
    extern /* Subroutine */ void clacp2_(char *, integer *, integer *, real *, 
	    integer *, complex *, integer *);
    logical wntqn, wntqo, wntqs;
    integer mnthr1, mnthr2, ie, lwork_cungbr_p_mn__, il, lwork_cungbr_p_nn__, 
	    lwork_cungbr_q_mn__, lwork_cungbr_q_mm__;
    extern /* Subroutine */ void cgebrd_(integer *, integer *, complex *, 
	    integer *, real *, real *, complex *, complex *, complex *, 
	    integer *, integer *);
    integer ir;
    extern real clange_(char *, integer *, integer *, complex *, integer *, 
	    real *);
    integer iu;
    extern /* Subroutine */ void cgelqf_(integer *, integer *, complex *, 
	    integer *, complex *, complex *, integer *, integer *), clacrm_(
	    integer *, integer *, complex *, integer *, real *, integer *, 
	    complex *, integer *, real *), clarcm_(integer *, integer *, real 
	    *, integer *, complex *, integer *, complex *, integer *, real *),
	     clascl_(char *, integer *, integer *, real *, real *, integer *, 
	    integer *, complex *, integer *, integer *), sbdsdc_(char 
	    *, char *, integer *, real *, real *, real *, integer *, real *, 
	    integer *, real *, integer *, real *, integer *, integer *), cgeqrf_(integer *, integer *, complex *, integer 
	    *, complex *, complex *, integer *, integer *);
    extern real slamch_(char *);
    extern /* Subroutine */ void clacpy_(char *, integer *, integer *, complex 
	    *, integer *, complex *, integer *), claset_(char *, 
	    integer *, integer *, complex *, complex *, complex *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern void cungbr_(char *, 
	    integer *, integer *, integer *, complex *, integer *, complex *, 
	    complex *, integer *, integer *);
    real bignum;
    extern /* Subroutine */ void slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *), cunmbr_(char *, char *, char *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, complex *, integer *, 
	    complex *, integer *, integer *), cunglq_(
	    integer *, integer *, integer *, complex *, integer *, complex *, 
	    complex *, integer *, integer *);
    extern logical sisnan_(real *);
    integer ldwrkl;
    extern /* Subroutine */ void cungqr_(integer *, integer *, integer *, 
	    complex *, integer *, complex *, complex *, integer *, integer *);
    integer ldwrkr, minwrk, ldwrku, maxwrk, ldwkvt;
    real smlnum;
    logical wntqas, lquery;
    integer nrwork, blk;
    real dum[1], eps;
    integer iru, ivt, lwork_cgebrd_mm__, lwork_cgebrd_mn__, lwork_cgebrd_nn__,
	     lwork_cgelqf_mn__, lwork_cgeqrf_mn__;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


/*     Test the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --s;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    vt_dim1 = *ldvt;
    vt_offset = 1 + vt_dim1 * 1;
    vt -= vt_offset;
    --work;
    --rwork;
    --iwork;

    /* Function Body */
    *info = 0;
    minmn = f2cmin(*m,*n);
    mnthr1 = (integer) (minmn * 17.f / 9.f);
    mnthr2 = (integer) (minmn * 5.f / 3.f);
    wntqa = lsame_(jobz, "A");
    wntqs = lsame_(jobz, "S");
    wntqas = wntqa || wntqs;
    wntqo = lsame_(jobz, "O");
    wntqn = lsame_(jobz, "N");
    lquery = *lwork == -1;
    minwrk = 1;
    maxwrk = 1;

    if (! (wntqa || wntqs || wntqo || wntqn)) {
	*info = -1;
    } else if (*m < 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -5;
    } else if (*ldu < 1 || wntqas && *ldu < *m || wntqo && *m < *n && *ldu < *
	    m) {
	*info = -8;
    } else if (*ldvt < 1 || wntqa && *ldvt < *n || wntqs && *ldvt < minmn || 
	    wntqo && *m >= *n && *ldvt < *n) {
	*info = -10;
    }

/*     Compute workspace */
/*       Note: Comments in the code beginning "Workspace:" describe the */
/*       minimal amount of workspace allocated at that point in the code, */
/*       as well as the preferred amount for good performance. */
/*       CWorkspace refers to complex workspace, and RWorkspace to */
/*       real workspace. NB refers to the optimal block size for the */
/*       immediately following subroutine, as returned by ILAENV.) */

    if (*info == 0) {
	minwrk = 1;
	maxwrk = 1;
	if (*m >= *n && minmn > 0) {

/*           There is no complex work space needed for bidiagonal SVD */
/*           The real work space needed for bidiagonal SVD (sbdsdc) is */
/*           BDSPAC = 3*N*N + 4*N for singular values and vectors; */
/*           BDSPAC = 4*N         for singular values only; */
/*           not including e, RU, and RVT matrices. */

/*           Compute space preferred for each routine */
	    cgebrd_(m, n, cdum, m, dum, dum, cdum, cdum, cdum, &c_n1, &ierr);
	    lwork_cgebrd_mn__ = (integer) cdum[0].r;

	    cgebrd_(n, n, cdum, n, dum, dum, cdum, cdum, cdum, &c_n1, &ierr);
	    lwork_cgebrd_nn__ = (integer) cdum[0].r;

	    cgeqrf_(m, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cgeqrf_mn__ = (integer) cdum[0].r;

	    cungbr_("P", n, n, n, cdum, n, cdum, cdum, &c_n1, &ierr);
	    lwork_cungbr_p_nn__ = (integer) cdum[0].r;

	    cungbr_("Q", m, m, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cungbr_q_mm__ = (integer) cdum[0].r;

	    cungbr_("Q", m, n, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cungbr_q_mn__ = (integer) cdum[0].r;

	    cungqr_(m, m, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cungqr_mm__ = (integer) cdum[0].r;

	    cungqr_(m, n, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cungqr_mn__ = (integer) cdum[0].r;

	    cunmbr_("P", "R", "C", n, n, n, cdum, n, cdum, cdum, n, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_prc_nn__ = (integer) cdum[0].r;

	    cunmbr_("Q", "L", "N", m, m, n, cdum, m, cdum, cdum, m, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_qln_mm__ = (integer) cdum[0].r;

	    cunmbr_("Q", "L", "N", m, n, n, cdum, m, cdum, cdum, m, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_qln_mn__ = (integer) cdum[0].r;

	    cunmbr_("Q", "L", "N", n, n, n, cdum, n, cdum, cdum, n, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_qln_nn__ = (integer) cdum[0].r;

	    if (*m >= mnthr1) {
		if (wntqn) {

/*                 Path 1 (M >> N, JOBZ='N') */

		    maxwrk = *n + lwork_cgeqrf_mn__;
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cgebrd_nn__;
		    maxwrk = f2cmax(i__1,i__2);
		    minwrk = *n * 3;
		} else if (wntqo) {

/*                 Path 2 (M >> N, JOBZ='O') */

		    wrkbl = *n + lwork_cgeqrf_mn__;
/* Computing MAX */
		    i__1 = wrkbl, i__2 = *n + lwork_cungqr_mn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cgebrd_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cunmbr_qln_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cunmbr_prc_nn__;
		    wrkbl = f2cmax(i__1,i__2);
		    maxwrk = *m * *n + *n * *n + wrkbl;
		    minwrk = (*n << 1) * *n + *n * 3;
		} else if (wntqs) {

/*                 Path 3 (M >> N, JOBZ='S') */

		    wrkbl = *n + lwork_cgeqrf_mn__;
/* Computing MAX */
		    i__1 = wrkbl, i__2 = *n + lwork_cungqr_mn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cgebrd_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cunmbr_qln_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cunmbr_prc_nn__;
		    wrkbl = f2cmax(i__1,i__2);
		    maxwrk = *n * *n + wrkbl;
		    minwrk = *n * *n + *n * 3;
		} else if (wntqa) {

/*                 Path 4 (M >> N, JOBZ='A') */

		    wrkbl = *n + lwork_cgeqrf_mn__;
/* Computing MAX */
		    i__1 = wrkbl, i__2 = *n + lwork_cungqr_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cgebrd_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cunmbr_qln_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*n << 1) + lwork_cunmbr_prc_nn__;
		    wrkbl = f2cmax(i__1,i__2);
		    maxwrk = *n * *n + wrkbl;
/* Computing MAX */
		    i__1 = *n * 3, i__2 = *n + *m;
		    minwrk = *n * *n + f2cmax(i__1,i__2);
		}
	    } else if (*m >= mnthr2) {

/*              Path 5 (M >> N, but not as much as MNTHR1) */

		maxwrk = (*n << 1) + lwork_cgebrd_mn__;
		minwrk = (*n << 1) + *m;
		if (wntqo) {
/*                 Path 5o (M >> N, JOBZ='O') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr_p_nn__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr_q_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		    maxwrk += *m * *n;
		    minwrk += *n * *n;
		} else if (wntqs) {
/*                 Path 5s (M >> N, JOBZ='S') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr_p_nn__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr_q_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		} else if (wntqa) {
/*                 Path 5a (M >> N, JOBZ='A') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr_p_nn__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr_q_mm__;
		    maxwrk = f2cmax(i__1,i__2);
		}
	    } else {

/*              Path 6 (M >= N, but not much larger) */

		maxwrk = (*n << 1) + lwork_cgebrd_mn__;
		minwrk = (*n << 1) + *m;
		if (wntqo) {
/*                 Path 6o (M >= N, JOBZ='O') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr_prc_nn__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr_qln_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		    maxwrk += *m * *n;
		    minwrk += *n * *n;
		} else if (wntqs) {
/*                 Path 6s (M >= N, JOBZ='S') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr_qln_mn__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr_prc_nn__;
		    maxwrk = f2cmax(i__1,i__2);
		} else if (wntqa) {
/*                 Path 6a (M >= N, JOBZ='A') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr_qln_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr_prc_nn__;
		    maxwrk = f2cmax(i__1,i__2);
		}
	    }
	} else if (minmn > 0) {

/*           There is no complex work space needed for bidiagonal SVD */
/*           The real work space needed for bidiagonal SVD (sbdsdc) is */
/*           BDSPAC = 3*M*M + 4*M for singular values and vectors; */
/*           BDSPAC = 4*M         for singular values only; */
/*           not including e, RU, and RVT matrices. */

/*           Compute space preferred for each routine */
	    cgebrd_(m, n, cdum, m, dum, dum, cdum, cdum, cdum, &c_n1, &ierr);
	    lwork_cgebrd_mn__ = (integer) cdum[0].r;

	    cgebrd_(m, m, cdum, m, dum, dum, cdum, cdum, cdum, &c_n1, &ierr);
	    lwork_cgebrd_mm__ = (integer) cdum[0].r;

	    cgelqf_(m, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cgelqf_mn__ = (integer) cdum[0].r;

	    cungbr_("P", m, n, m, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cungbr_p_mn__ = (integer) cdum[0].r;

	    cungbr_("P", n, n, m, cdum, n, cdum, cdum, &c_n1, &ierr);
	    lwork_cungbr_p_nn__ = (integer) cdum[0].r;

	    cungbr_("Q", m, m, n, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cungbr_q_mm__ = (integer) cdum[0].r;

	    cunglq_(m, n, m, cdum, m, cdum, cdum, &c_n1, &ierr);
	    lwork_cunglq_mn__ = (integer) cdum[0].r;

	    cunglq_(n, n, m, cdum, n, cdum, cdum, &c_n1, &ierr);
	    lwork_cunglq_nn__ = (integer) cdum[0].r;

	    cunmbr_("P", "R", "C", m, m, m, cdum, m, cdum, cdum, m, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_prc_mm__ = (integer) cdum[0].r;

	    cunmbr_("P", "R", "C", m, n, m, cdum, m, cdum, cdum, m, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_prc_mn__ = (integer) cdum[0].r;

	    cunmbr_("P", "R", "C", n, n, m, cdum, n, cdum, cdum, n, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_prc_nn__ = (integer) cdum[0].r;

	    cunmbr_("Q", "L", "N", m, m, m, cdum, m, cdum, cdum, m, cdum, &
		    c_n1, &ierr);
	    lwork_cunmbr_qln_mm__ = (integer) cdum[0].r;

	    if (*n >= mnthr1) {
		if (wntqn) {

/*                 Path 1t (N >> M, JOBZ='N') */

		    maxwrk = *m + lwork_cgelqf_mn__;
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cgebrd_mm__;
		    maxwrk = f2cmax(i__1,i__2);
		    minwrk = *m * 3;
		} else if (wntqo) {

/*                 Path 2t (N >> M, JOBZ='O') */

		    wrkbl = *m + lwork_cgelqf_mn__;
/* Computing MAX */
		    i__1 = wrkbl, i__2 = *m + lwork_cunglq_mn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cgebrd_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cunmbr_qln_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cunmbr_prc_mm__;
		    wrkbl = f2cmax(i__1,i__2);
		    maxwrk = *m * *n + *m * *m + wrkbl;
		    minwrk = (*m << 1) * *m + *m * 3;
		} else if (wntqs) {

/*                 Path 3t (N >> M, JOBZ='S') */

		    wrkbl = *m + lwork_cgelqf_mn__;
/* Computing MAX */
		    i__1 = wrkbl, i__2 = *m + lwork_cunglq_mn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cgebrd_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cunmbr_qln_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cunmbr_prc_mm__;
		    wrkbl = f2cmax(i__1,i__2);
		    maxwrk = *m * *m + wrkbl;
		    minwrk = *m * *m + *m * 3;
		} else if (wntqa) {

/*                 Path 4t (N >> M, JOBZ='A') */

		    wrkbl = *m + lwork_cgelqf_mn__;
/* Computing MAX */
		    i__1 = wrkbl, i__2 = *m + lwork_cunglq_nn__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cgebrd_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cunmbr_qln_mm__;
		    wrkbl = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = wrkbl, i__2 = (*m << 1) + lwork_cunmbr_prc_mm__;
		    wrkbl = f2cmax(i__1,i__2);
		    maxwrk = *m * *m + wrkbl;
/* Computing MAX */
		    i__1 = *m * 3, i__2 = *m + *n;
		    minwrk = *m * *m + f2cmax(i__1,i__2);
		}
	    } else if (*n >= mnthr2) {

/*              Path 5t (N >> M, but not as much as MNTHR1) */

		maxwrk = (*m << 1) + lwork_cgebrd_mn__;
		minwrk = (*m << 1) + *n;
		if (wntqo) {
/*                 Path 5to (N >> M, JOBZ='O') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr_q_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr_p_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		    maxwrk += *m * *n;
		    minwrk += *m * *m;
		} else if (wntqs) {
/*                 Path 5ts (N >> M, JOBZ='S') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr_q_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr_p_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		} else if (wntqa) {
/*                 Path 5ta (N >> M, JOBZ='A') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr_q_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr_p_nn__;
		    maxwrk = f2cmax(i__1,i__2);
		}
	    } else {

/*              Path 6t (N > M, but not much larger) */

		maxwrk = (*m << 1) + lwork_cgebrd_mn__;
		minwrk = (*m << 1) + *n;
		if (wntqo) {
/*                 Path 6to (N > M, JOBZ='O') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr_qln_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr_prc_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		    maxwrk += *m * *n;
		    minwrk += *m * *m;
		} else if (wntqs) {
/*                 Path 6ts (N > M, JOBZ='S') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr_qln_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr_prc_mn__;
		    maxwrk = f2cmax(i__1,i__2);
		} else if (wntqa) {
/*                 Path 6ta (N > M, JOBZ='A') */
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr_qln_mm__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr_prc_nn__;
		    maxwrk = f2cmax(i__1,i__2);
		}
	    }
	}
	maxwrk = f2cmax(maxwrk,minwrk);
    }
    if (*info == 0) {
	work[1].r = (real) maxwrk, work[1].i = 0.f;
	if (*lwork < minwrk && ! lquery) {
	    *info = -12;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CGESDD", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Get machine constants */

    eps = slamch_("P");
    smlnum = sqrt(slamch_("S")) / eps;
    bignum = 1.f / smlnum;

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = clange_("M", m, n, &a[a_offset], lda, dum);
    if (sisnan_(&anrm)) {
	*info = -4;
	return;
    }
    iscl = 0;
    if (anrm > 0.f && anrm < smlnum) {
	iscl = 1;
	clascl_("G", &c__0, &c__0, &anrm, &smlnum, m, n, &a[a_offset], lda, &
		ierr);
    } else if (anrm > bignum) {
	iscl = 1;
	clascl_("G", &c__0, &c__0, &anrm, &bignum, m, n, &a[a_offset], lda, &
		ierr);
    }

    if (*m >= *n) {

/*        A has at least as many rows as columns. If A has sufficiently */
/*        more rows than columns, first reduce using the QR */
/*        decomposition (if sufficient workspace available) */

	if (*m >= mnthr1) {

	    if (wntqn) {

/*              Path 1 (M >> N, JOBZ='N') */
/*              No singular vectors to be computed */

		itau = 1;
		nwork = itau + *n;

/*              Compute A=Q*R */
/*              CWorkspace: need   N [tau] + N    [work] */
/*              CWorkspace: prefer N [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__1, &ierr);

/*              Zero out below R */

		i__1 = *n - 1;
		i__2 = *n - 1;
		claset_("L", &i__1, &i__2, &c_b1, &c_b1, &a[a_dim1 + 2], lda);
		ie = 1;
		itauq = 1;
		itaup = itauq + *n;
		nwork = itaup + *n;

/*              Bidiagonalize R in A */
/*              CWorkspace: need   2*N [tauq, taup] + N      [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + 2*N*NB [work] */
/*              RWorkspace: need   N [e] */

		i__1 = *lwork - nwork + 1;
		cgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__1, &ierr);
		nrwork = ie + *n;

/*              Perform bidiagonal SVD, compute singular values only */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + BDSPAC */

		sbdsdc_("U", "N", n, &s[1], &rwork[ie], dum, &c__1, dum, &
			c__1, dum, idum, &rwork[nrwork], &iwork[1], info);

	    } else if (wntqo) {

/*              Path 2 (M >> N, JOBZ='O') */
/*              N left singular vectors to be overwritten on A and */
/*              N right singular vectors to be computed in VT */

		iu = 1;

/*              WORK(IU) is N by N */

		ldwrku = *n;
		ir = iu + ldwrku * *n;
		if (*lwork >= *m * *n + *n * *n + *n * 3) {

/*                 WORK(IR) is M by N */

		    ldwrkr = *m;
		} else {
		    ldwrkr = (*lwork - *n * *n - *n * 3) / *n;
		}
		itau = ir + ldwrkr * *n;
		nwork = itau + *n;

/*              Compute A=Q*R */
/*              CWorkspace: need   N*N [U] + N*N [R] + N [tau] + N    [work] */
/*              CWorkspace: prefer N*N [U] + N*N [R] + N [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__1, &ierr);

/*              Copy R to WORK( IR ), zeroing out below it */

		clacpy_("U", n, n, &a[a_offset], lda, &work[ir], &ldwrkr);
		i__1 = *n - 1;
		i__2 = *n - 1;
		claset_("L", &i__1, &i__2, &c_b1, &c_b1, &work[ir + 1], &
			ldwrkr);

/*              Generate Q in A */
/*              CWorkspace: need   N*N [U] + N*N [R] + N [tau] + N    [work] */
/*              CWorkspace: prefer N*N [U] + N*N [R] + N [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cungqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[nwork],
			 &i__1, &ierr);
		ie = 1;
		itauq = itau;
		itaup = itauq + *n;
		nwork = itaup + *n;

/*              Bidiagonalize R in WORK(IR) */
/*              CWorkspace: need   N*N [U] + N*N [R] + 2*N [tauq, taup] + N      [work] */
/*              CWorkspace: prefer N*N [U] + N*N [R] + 2*N [tauq, taup] + 2*N*NB [work] */
/*              RWorkspace: need   N [e] */

		i__1 = *lwork - nwork + 1;
		cgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__1, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of R in WORK(IRU) and computing right singular vectors */
/*              of R in WORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		iru = ie + *n;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix WORK(IU) */
/*              Overwrite WORK(IU) by the left singular vectors of R */
/*              CWorkspace: need   N*N [U] + N*N [R] + 2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer N*N [U] + N*N [R] + 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", n, n, &rwork[iru], n, &work[iu], &ldwrku);
		i__1 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", n, n, n, &work[ir], &ldwrkr, &work[
			itauq], &work[iu], &ldwrku, &work[nwork], &i__1, &
			ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by the right singular vectors of R */
/*              CWorkspace: need   N*N [U] + N*N [R] + 2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer N*N [U] + N*N [R] + 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", n, n, &rwork[irvt], n, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, n, &work[ir], &ldwrkr, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__1, &
			ierr);

/*              Multiply Q in A by left singular vectors of R in */
/*              WORK(IU), storing result in WORK(IR) and copying to A */
/*              CWorkspace: need   N*N [U] + N*N [R] */
/*              CWorkspace: prefer N*N [U] + M*N [R] */
/*              RWorkspace: need   0 */

		i__1 = *m;
		i__2 = ldwrkr;
		for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += 
			i__2) {
/* Computing MIN */
		    i__3 = *m - i__ + 1;
		    chunk = f2cmin(i__3,ldwrkr);
		    cgemm_("N", "N", &chunk, n, n, &c_b2, &a[i__ + a_dim1], 
			    lda, &work[iu], &ldwrku, &c_b1, &work[ir], &
			    ldwrkr);
		    clacpy_("F", &chunk, n, &work[ir], &ldwrkr, &a[i__ + 
			    a_dim1], lda);
/* L10: */
		}

	    } else if (wntqs) {

/*              Path 3 (M >> N, JOBZ='S') */
/*              N left singular vectors to be computed in U and */
/*              N right singular vectors to be computed in VT */

		ir = 1;

/*              WORK(IR) is N by N */

		ldwrkr = *n;
		itau = ir + ldwrkr * *n;
		nwork = itau + *n;

/*              Compute A=Q*R */
/*              CWorkspace: need   N*N [R] + N [tau] + N    [work] */
/*              CWorkspace: prefer N*N [R] + N [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__2, &ierr);

/*              Copy R to WORK(IR), zeroing out below it */

		clacpy_("U", n, n, &a[a_offset], lda, &work[ir], &ldwrkr);
		i__2 = *n - 1;
		i__1 = *n - 1;
		claset_("L", &i__2, &i__1, &c_b1, &c_b1, &work[ir + 1], &
			ldwrkr);

/*              Generate Q in A */
/*              CWorkspace: need   N*N [R] + N [tau] + N    [work] */
/*              CWorkspace: prefer N*N [R] + N [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cungqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[nwork],
			 &i__2, &ierr);
		ie = 1;
		itauq = itau;
		itaup = itauq + *n;
		nwork = itaup + *n;

/*              Bidiagonalize R in WORK(IR) */
/*              CWorkspace: need   N*N [R] + 2*N [tauq, taup] + N      [work] */
/*              CWorkspace: prefer N*N [R] + 2*N [tauq, taup] + 2*N*NB [work] */
/*              RWorkspace: need   N [e] */

		i__2 = *lwork - nwork + 1;
		cgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__2, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		iru = ie + *n;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of R */
/*              CWorkspace: need   N*N [R] + 2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer N*N [R] + 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", n, n, &rwork[iru], n, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", n, n, n, &work[ir], &ldwrkr, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__2, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of R */
/*              CWorkspace: need   N*N [R] + 2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer N*N [R] + 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", n, n, &rwork[irvt], n, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, n, &work[ir], &ldwrkr, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__2, &
			ierr);

/*              Multiply Q in A by left singular vectors of R in */
/*              WORK(IR), storing result in U */
/*              CWorkspace: need   N*N [R] */
/*              RWorkspace: need   0 */

		clacpy_("F", n, n, &u[u_offset], ldu, &work[ir], &ldwrkr);
		cgemm_("N", "N", m, n, n, &c_b2, &a[a_offset], lda, &work[ir],
			 &ldwrkr, &c_b1, &u[u_offset], ldu);

	    } else if (wntqa) {

/*              Path 4 (M >> N, JOBZ='A') */
/*              M left singular vectors to be computed in U and */
/*              N right singular vectors to be computed in VT */

		iu = 1;

/*              WORK(IU) is N by N */

		ldwrku = *n;
		itau = iu + ldwrku * *n;
		nwork = itau + *n;

/*              Compute A=Q*R, copying result to U */
/*              CWorkspace: need   N*N [U] + N [tau] + N    [work] */
/*              CWorkspace: prefer N*N [U] + N [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__2, &ierr);
		clacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], ldu);

/*              Generate Q in U */
/*              CWorkspace: need   N*N [U] + N [tau] + M    [work] */
/*              CWorkspace: prefer N*N [U] + N [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &work[nwork],
			 &i__2, &ierr);

/*              Produce R in A, zeroing out below it */

		i__2 = *n - 1;
		i__1 = *n - 1;
		claset_("L", &i__2, &i__1, &c_b1, &c_b1, &a[a_dim1 + 2], lda);
		ie = 1;
		itauq = itau;
		itaup = itauq + *n;
		nwork = itaup + *n;

/*              Bidiagonalize R in A */
/*              CWorkspace: need   N*N [U] + 2*N [tauq, taup] + N      [work] */
/*              CWorkspace: prefer N*N [U] + 2*N [tauq, taup] + 2*N*NB [work] */
/*              RWorkspace: need   N [e] */

		i__2 = *lwork - nwork + 1;
		cgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__2, &ierr);
		iru = ie + *n;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix WORK(IU) */
/*              Overwrite WORK(IU) by left singular vectors of R */
/*              CWorkspace: need   N*N [U] + 2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer N*N [U] + 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", n, n, &rwork[iru], n, &work[iu], &ldwrku);
		i__2 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", n, n, n, &a[a_offset], lda, &work[
			itauq], &work[iu], &ldwrku, &work[nwork], &i__2, &
			ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of R */
/*              CWorkspace: need   N*N [U] + 2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer N*N [U] + 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", n, n, &rwork[irvt], n, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, n, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__2, &
			ierr);

/*              Multiply Q in U by left singular vectors of R in */
/*              WORK(IU), storing result in A */
/*              CWorkspace: need   N*N [U] */
/*              RWorkspace: need   0 */

		cgemm_("N", "N", m, n, n, &c_b2, &u[u_offset], ldu, &work[iu],
			 &ldwrku, &c_b1, &a[a_offset], lda);

/*              Copy left singular vectors of A from A to U */

		clacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], ldu);

	    }

	} else if (*m >= mnthr2) {

/*           MNTHR2 <= M < MNTHR1 */

/*           Path 5 (M >> N, but not as much as MNTHR1) */
/*           Reduce to bidiagonal form without QR decomposition, use */
/*           CUNGBR and matrix multiplication to compute singular vectors */

	    ie = 1;
	    nrwork = ie + *n;
	    itauq = 1;
	    itaup = itauq + *n;
	    nwork = itaup + *n;

/*           Bidiagonalize A */
/*           CWorkspace: need   2*N [tauq, taup] + M        [work] */
/*           CWorkspace: prefer 2*N [tauq, taup] + (M+N)*NB [work] */
/*           RWorkspace: need   N [e] */

	    i__2 = *lwork - nwork + 1;
	    cgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[nwork], &i__2, &ierr);
	    if (wntqn) {

/*              Path 5n (M >> N, JOBZ='N') */
/*              Compute singular values only */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + BDSPAC */

		sbdsdc_("U", "N", n, &s[1], &rwork[ie], dum, &c__1, dum, &
			c__1, dum, idum, &rwork[nrwork], &iwork[1], info);
	    } else if (wntqo) {
		iu = nwork;
		iru = nrwork;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;

/*              Path 5o (M >> N, JOBZ='O') */
/*              Copy A to VT, generate P**H */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], &
			work[nwork], &i__2, &ierr);

/*              Generate Q in A */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cungbr_("Q", m, n, n, &a[a_offset], lda, &work[itauq], &work[
			nwork], &i__2, &ierr);

		if (*lwork >= *m * *n + *n * 3) {

/*                 WORK( IU ) is M by N */

		    ldwrku = *m;
		} else {

/*                 WORK(IU) is LDWRKU by N */

		    ldwrku = (*lwork - *n * 3) / *n;
		}
		nwork = iu + ldwrku * *n;

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Multiply real matrix RWORK(IRVT) by P**H in VT, */
/*              storing the result in WORK(IU), copying to VT */
/*              CWorkspace: need   2*N [tauq, taup] + N*N [U] */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + 2*N*N [rwork] */

		clarcm_(n, n, &rwork[irvt], n, &vt[vt_offset], ldvt, &work[iu]
			, &ldwrku, &rwork[nrwork]);
		clacpy_("F", n, n, &work[iu], &ldwrku, &vt[vt_offset], ldvt);

/*              Multiply Q in A by real matrix RWORK(IRU), storing the */
/*              result in WORK(IU), copying to A */
/*              CWorkspace: need   2*N [tauq, taup] + N*N [U] */
/*              CWorkspace: prefer 2*N [tauq, taup] + M*N [U] */
/*              RWorkspace: need   N [e] + N*N [RU] + 2*N*N [rwork] */
/*              RWorkspace: prefer N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here */

		nrwork = irvt;
		i__2 = *m;
		i__1 = ldwrku;
		for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += 
			i__1) {
/* Computing MIN */
		    i__3 = *m - i__ + 1;
		    chunk = f2cmin(i__3,ldwrku);
		    clacrm_(&chunk, n, &a[i__ + a_dim1], lda, &rwork[iru], n, 
			    &work[iu], &ldwrku, &rwork[nrwork]);
		    clacpy_("F", &chunk, n, &work[iu], &ldwrku, &a[i__ + 
			    a_dim1], lda);
/* L20: */
		}

	    } else if (wntqs) {

/*              Path 5s (M >> N, JOBZ='S') */
/*              Copy A to VT, generate P**H */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], &
			work[nwork], &i__1, &ierr);

/*              Copy A to U, generate Q */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cungbr_("Q", m, n, n, &u[u_offset], ldu, &work[itauq], &work[
			nwork], &i__1, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		iru = nrwork;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Multiply real matrix RWORK(IRVT) by P**H in VT, */
/*              storing the result in A, copying to VT */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + 2*N*N [rwork] */

		clarcm_(n, n, &rwork[irvt], n, &vt[vt_offset], ldvt, &a[
			a_offset], lda, &rwork[nrwork]);
		clacpy_("F", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);

/*              Multiply Q in U by real matrix RWORK(IRU), storing the */
/*              result in A, copying to U */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here */

		nrwork = irvt;
		clacrm_(m, n, &u[u_offset], ldu, &rwork[iru], n, &a[a_offset],
			 lda, &rwork[nrwork]);
		clacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], ldu);
	    } else {

/*              Path 5a (M >> N, JOBZ='A') */
/*              Copy A to VT, generate P**H */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], &
			work[nwork], &i__1, &ierr);

/*              Copy A to U, generate Q */
/*              CWorkspace: need   2*N [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cungbr_("Q", m, m, n, &u[u_offset], ldu, &work[itauq], &work[
			nwork], &i__1, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		iru = nrwork;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Multiply real matrix RWORK(IRVT) by P**H in VT, */
/*              storing the result in A, copying to VT */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + 2*N*N [rwork] */

		clarcm_(n, n, &rwork[irvt], n, &vt[vt_offset], ldvt, &a[
			a_offset], lda, &rwork[nrwork]);
		clacpy_("F", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);

/*              Multiply Q in U by real matrix RWORK(IRU), storing the */
/*              result in A, copying to U */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here */

		nrwork = irvt;
		clacrm_(m, n, &u[u_offset], ldu, &rwork[iru], n, &a[a_offset],
			 lda, &rwork[nrwork]);
		clacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], ldu);
	    }

	} else {

/*           M .LT. MNTHR2 */

/*           Path 6 (M >= N, but not much larger) */
/*           Reduce to bidiagonal form without QR decomposition */
/*           Use CUNMBR to compute singular vectors */

	    ie = 1;
	    nrwork = ie + *n;
	    itauq = 1;
	    itaup = itauq + *n;
	    nwork = itaup + *n;

/*           Bidiagonalize A */
/*           CWorkspace: need   2*N [tauq, taup] + M        [work] */
/*           CWorkspace: prefer 2*N [tauq, taup] + (M+N)*NB [work] */
/*           RWorkspace: need   N [e] */

	    i__1 = *lwork - nwork + 1;
	    cgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[nwork], &i__1, &ierr);
	    if (wntqn) {

/*              Path 6n (M >= N, JOBZ='N') */
/*              Compute singular values only */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + BDSPAC */

		sbdsdc_("U", "N", n, &s[1], &rwork[ie], dum, &c__1, dum, &
			c__1, dum, idum, &rwork[nrwork], &iwork[1], info);
	    } else if (wntqo) {
		iu = nwork;
		iru = nrwork;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		if (*lwork >= *m * *n + *n * 3) {

/*                 WORK( IU ) is M by N */

		    ldwrku = *m;
		} else {

/*                 WORK( IU ) is LDWRKU by N */

		    ldwrku = (*lwork - *n * 3) / *n;
		}
		nwork = iu + ldwrku * *n;

/*              Path 6o (M >= N, JOBZ='O') */
/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of A */
/*              CWorkspace: need   2*N [tauq, taup] + N*N [U] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*N [U] + N*NB [work] */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] */

		clacp2_("F", n, n, &rwork[irvt], n, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, n, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__1, &
			ierr);

		if (*lwork >= *m * *n + *n * 3) {

/*                 Path 6o-fast */
/*                 Copy real matrix RWORK(IRU) to complex matrix WORK(IU) */
/*                 Overwrite WORK(IU) by left singular vectors of A, copying */
/*                 to A */
/*                 CWorkspace: need   2*N [tauq, taup] + M*N [U] + N    [work] */
/*                 CWorkspace: prefer 2*N [tauq, taup] + M*N [U] + N*NB [work] */
/*                 RWorkspace: need   N [e] + N*N [RU] */

		    claset_("F", m, n, &c_b1, &c_b1, &work[iu], &ldwrku);
		    clacp2_("F", n, n, &rwork[iru], n, &work[iu], &ldwrku);
		    i__1 = *lwork - nwork + 1;
		    cunmbr_("Q", "L", "N", m, n, n, &a[a_offset], lda, &work[
			    itauq], &work[iu], &ldwrku, &work[nwork], &i__1, &
			    ierr);
		    clacpy_("F", m, n, &work[iu], &ldwrku, &a[a_offset], lda);
		} else {

/*                 Path 6o-slow */
/*                 Generate Q in A */
/*                 CWorkspace: need   2*N [tauq, taup] + N*N [U] + N    [work] */
/*                 CWorkspace: prefer 2*N [tauq, taup] + N*N [U] + N*NB [work] */
/*                 RWorkspace: need   0 */

		    i__1 = *lwork - nwork + 1;
		    cungbr_("Q", m, n, n, &a[a_offset], lda, &work[itauq], &
			    work[nwork], &i__1, &ierr);

/*                 Multiply Q in A by real matrix RWORK(IRU), storing the */
/*                 result in WORK(IU), copying to A */
/*                 CWorkspace: need   2*N [tauq, taup] + N*N [U] */
/*                 CWorkspace: prefer 2*N [tauq, taup] + M*N [U] */
/*                 RWorkspace: need   N [e] + N*N [RU] + 2*N*N [rwork] */
/*                 RWorkspace: prefer N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here */

		    nrwork = irvt;
		    i__1 = *m;
		    i__2 = ldwrku;
		    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ +=
			     i__2) {
/* Computing MIN */
			i__3 = *m - i__ + 1;
			chunk = f2cmin(i__3,ldwrku);
			clacrm_(&chunk, n, &a[i__ + a_dim1], lda, &rwork[iru],
				 n, &work[iu], &ldwrku, &rwork[nrwork]);
			clacpy_("F", &chunk, n, &work[iu], &ldwrku, &a[i__ + 
				a_dim1], lda);
/* L30: */
		    }
		}

	    } else if (wntqs) {

/*              Path 6s (M >= N, JOBZ='S') */
/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		iru = nrwork;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of A */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] */

		claset_("F", m, n, &c_b1, &c_b1, &u[u_offset], ldu)
			;
		clacp2_("F", n, n, &rwork[iru], n, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, n, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__2, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of A */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] */

		clacp2_("F", n, n, &rwork[irvt], n, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, n, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__2, &
			ierr);
	    } else {

/*              Path 6a (M >= N, JOBZ='A') */
/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC */

		iru = nrwork;
		irvt = iru + *n * *n;
		nrwork = irvt + *n * *n;
		sbdsdc_("U", "I", n, &s[1], &rwork[ie], &rwork[iru], n, &
			rwork[irvt], n, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Set the right corner of U to identity matrix */

		claset_("F", m, m, &c_b1, &c_b1, &u[u_offset], ldu)
			;
		if (*m > *n) {
		    i__2 = *m - *n;
		    i__1 = *m - *n;
		    claset_("F", &i__2, &i__1, &c_b1, &c_b2, &u[*n + 1 + (*n 
			    + 1) * u_dim1], ldu);
		}

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of A */
/*              CWorkspace: need   2*N [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] */

		clacp2_("F", n, n, &rwork[iru], n, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__2, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of A */
/*              CWorkspace: need   2*N [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] */

		clacp2_("F", n, n, &rwork[irvt], n, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, n, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__2, &
			ierr);
	    }

	}

    } else {

/*        A has more columns than rows. If A has sufficiently more */
/*        columns than rows, first reduce using the LQ decomposition (if */
/*        sufficient workspace available) */

	if (*n >= mnthr1) {

	    if (wntqn) {

/*              Path 1t (N >> M, JOBZ='N') */
/*              No singular vectors to be computed */

		itau = 1;
		nwork = itau + *m;

/*              Compute A=L*Q */
/*              CWorkspace: need   M [tau] + M    [work] */
/*              CWorkspace: prefer M [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__2, &ierr);

/*              Zero out above L */

		i__2 = *m - 1;
		i__1 = *m - 1;
		claset_("U", &i__2, &i__1, &c_b1, &c_b1, &a[(a_dim1 << 1) + 1]
			, lda);
		ie = 1;
		itauq = 1;
		itaup = itauq + *m;
		nwork = itaup + *m;

/*              Bidiagonalize L in A */
/*              CWorkspace: need   2*M [tauq, taup] + M      [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + 2*M*NB [work] */
/*              RWorkspace: need   M [e] */

		i__2 = *lwork - nwork + 1;
		cgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__2, &ierr);
		nrwork = ie + *m;

/*              Perform bidiagonal SVD, compute singular values only */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + BDSPAC */

		sbdsdc_("U", "N", m, &s[1], &rwork[ie], dum, &c__1, dum, &
			c__1, dum, idum, &rwork[nrwork], &iwork[1], info);

	    } else if (wntqo) {

/*              Path 2t (N >> M, JOBZ='O') */
/*              M right singular vectors to be overwritten on A and */
/*              M left singular vectors to be computed in U */

		ivt = 1;
		ldwkvt = *m;

/*              WORK(IVT) is M by M */

		il = ivt + ldwkvt * *m;
		if (*lwork >= *m * *n + *m * *m + *m * 3) {

/*                 WORK(IL) M by N */

		    ldwrkl = *m;
		    chunk = *n;
		} else {

/*                 WORK(IL) is M by CHUNK */

		    ldwrkl = *m;
		    chunk = (*lwork - *m * *m - *m * 3) / *m;
		}
		itau = il + ldwrkl * chunk;
		nwork = itau + *m;

/*              Compute A=L*Q */
/*              CWorkspace: need   M*M [VT] + M*M [L] + M [tau] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + M*M [L] + M [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__2, &ierr);

/*              Copy L to WORK(IL), zeroing about above it */

		clacpy_("L", m, m, &a[a_offset], lda, &work[il], &ldwrkl);
		i__2 = *m - 1;
		i__1 = *m - 1;
		claset_("U", &i__2, &i__1, &c_b1, &c_b1, &work[il + ldwrkl], &
			ldwrkl);

/*              Generate Q in A */
/*              CWorkspace: need   M*M [VT] + M*M [L] + M [tau] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + M*M [L] + M [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__2 = *lwork - nwork + 1;
		cunglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[nwork],
			 &i__2, &ierr);
		ie = 1;
		itauq = itau;
		itaup = itauq + *m;
		nwork = itaup + *m;

/*              Bidiagonalize L in WORK(IL) */
/*              CWorkspace: need   M*M [VT] + M*M [L] + 2*M [tauq, taup] + M      [work] */
/*              CWorkspace: prefer M*M [VT] + M*M [L] + 2*M [tauq, taup] + 2*M*NB [work] */
/*              RWorkspace: need   M [e] */

		i__2 = *lwork - nwork + 1;
		cgebrd_(m, m, &work[il], &ldwrkl, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__2, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RU] + M*M [RVT] + BDSPAC */

		iru = ie + *m;
		irvt = iru + *m * *m;
		nrwork = irvt + *m * *m;
		sbdsdc_("U", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix WORK(IU) */
/*              Overwrite WORK(IU) by the left singular vectors of L */
/*              CWorkspace: need   M*M [VT] + M*M [L] + 2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + M*M [L] + 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", m, m, &rwork[iru], m, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, m, &work[il], &ldwrkl, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__2, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix WORK(IVT) */
/*              Overwrite WORK(IVT) by the right singular vectors of L */
/*              CWorkspace: need   M*M [VT] + M*M [L] + 2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + M*M [L] + 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", m, m, &rwork[irvt], m, &work[ivt], &ldwkvt);
		i__2 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", m, m, m, &work[il], &ldwrkl, &work[
			itaup], &work[ivt], &ldwkvt, &work[nwork], &i__2, &
			ierr);

/*              Multiply right singular vectors of L in WORK(IL) by Q */
/*              in A, storing result in WORK(IL) and copying to A */
/*              CWorkspace: need   M*M [VT] + M*M [L] */
/*              CWorkspace: prefer M*M [VT] + M*N [L] */
/*              RWorkspace: need   0 */

		i__2 = *n;
		i__1 = chunk;
		for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += 
			i__1) {
/* Computing MIN */
		    i__3 = *n - i__ + 1;
		    blk = f2cmin(i__3,chunk);
		    cgemm_("N", "N", m, &blk, m, &c_b2, &work[ivt], m, &a[i__ 
			    * a_dim1 + 1], lda, &c_b1, &work[il], &ldwrkl);
		    clacpy_("F", m, &blk, &work[il], &ldwrkl, &a[i__ * a_dim1 
			    + 1], lda);
/* L40: */
		}

	    } else if (wntqs) {

/*              Path 3t (N >> M, JOBZ='S') */
/*              M right singular vectors to be computed in VT and */
/*              M left singular vectors to be computed in U */

		il = 1;

/*              WORK(IL) is M by M */

		ldwrkl = *m;
		itau = il + ldwrkl * *m;
		nwork = itau + *m;

/*              Compute A=L*Q */
/*              CWorkspace: need   M*M [L] + M [tau] + M    [work] */
/*              CWorkspace: prefer M*M [L] + M [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__1, &ierr);

/*              Copy L to WORK(IL), zeroing out above it */

		clacpy_("L", m, m, &a[a_offset], lda, &work[il], &ldwrkl);
		i__1 = *m - 1;
		i__2 = *m - 1;
		claset_("U", &i__1, &i__2, &c_b1, &c_b1, &work[il + ldwrkl], &
			ldwrkl);

/*              Generate Q in A */
/*              CWorkspace: need   M*M [L] + M [tau] + M    [work] */
/*              CWorkspace: prefer M*M [L] + M [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cunglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[nwork],
			 &i__1, &ierr);
		ie = 1;
		itauq = itau;
		itaup = itauq + *m;
		nwork = itaup + *m;

/*              Bidiagonalize L in WORK(IL) */
/*              CWorkspace: need   M*M [L] + 2*M [tauq, taup] + M      [work] */
/*              CWorkspace: prefer M*M [L] + 2*M [tauq, taup] + 2*M*NB [work] */
/*              RWorkspace: need   M [e] */

		i__1 = *lwork - nwork + 1;
		cgebrd_(m, m, &work[il], &ldwrkl, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__1, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RU] + M*M [RVT] + BDSPAC */

		iru = ie + *m;
		irvt = iru + *m * *m;
		nrwork = irvt + *m * *m;
		sbdsdc_("U", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of L */
/*              CWorkspace: need   M*M [L] + 2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer M*M [L] + 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", m, m, &rwork[iru], m, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, m, &work[il], &ldwrkl, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__1, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by left singular vectors of L */
/*              CWorkspace: need   M*M [L] + 2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer M*M [L] + 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", m, m, &rwork[irvt], m, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", m, m, m, &work[il], &ldwrkl, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__1, &
			ierr);

/*              Copy VT to WORK(IL), multiply right singular vectors of L */
/*              in WORK(IL) by Q in A, storing result in VT */
/*              CWorkspace: need   M*M [L] */
/*              RWorkspace: need   0 */

		clacpy_("F", m, m, &vt[vt_offset], ldvt, &work[il], &ldwrkl);
		cgemm_("N", "N", m, n, m, &c_b2, &work[il], &ldwrkl, &a[
			a_offset], lda, &c_b1, &vt[vt_offset], ldvt);

	    } else if (wntqa) {

/*              Path 4t (N >> M, JOBZ='A') */
/*              N right singular vectors to be computed in VT and */
/*              M left singular vectors to be computed in U */

		ivt = 1;

/*              WORK(IVT) is M by M */

		ldwkvt = *m;
		itau = ivt + ldwkvt * *m;
		nwork = itau + *m;

/*              Compute A=L*Q, copying result to VT */
/*              CWorkspace: need   M*M [VT] + M [tau] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + M [tau] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[nwork], &
			i__1, &ierr);
		clacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);

/*              Generate Q in VT */
/*              CWorkspace: need   M*M [VT] + M [tau] + N    [work] */
/*              CWorkspace: prefer M*M [VT] + M [tau] + N*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &work[
			nwork], &i__1, &ierr);

/*              Produce L in A, zeroing out above it */

		i__1 = *m - 1;
		i__2 = *m - 1;
		claset_("U", &i__1, &i__2, &c_b1, &c_b1, &a[(a_dim1 << 1) + 1]
			, lda);
		ie = 1;
		itauq = itau;
		itaup = itauq + *m;
		nwork = itaup + *m;

/*              Bidiagonalize L in A */
/*              CWorkspace: need   M*M [VT] + 2*M [tauq, taup] + M      [work] */
/*              CWorkspace: prefer M*M [VT] + 2*M [tauq, taup] + 2*M*NB [work] */
/*              RWorkspace: need   M [e] */

		i__1 = *lwork - nwork + 1;
		cgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[nwork], &i__1, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RU] + M*M [RVT] + BDSPAC */

		iru = ie + *m;
		irvt = iru + *m * *m;
		nrwork = irvt + *m * *m;
		sbdsdc_("U", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of L */
/*              CWorkspace: need   M*M [VT] + 2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", m, m, &rwork[iru], m, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, m, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__1, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix WORK(IVT) */
/*              Overwrite WORK(IVT) by right singular vectors of L */
/*              CWorkspace: need   M*M [VT] + 2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer M*M [VT] + 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacp2_("F", m, m, &rwork[irvt], m, &work[ivt], &ldwkvt);
		i__1 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", m, m, m, &a[a_offset], lda, &work[
			itaup], &work[ivt], &ldwkvt, &work[nwork], &i__1, &
			ierr);

/*              Multiply right singular vectors of L in WORK(IVT) by */
/*              Q in VT, storing result in A */
/*              CWorkspace: need   M*M [VT] */
/*              RWorkspace: need   0 */

		cgemm_("N", "N", m, n, m, &c_b2, &work[ivt], &ldwkvt, &vt[
			vt_offset], ldvt, &c_b1, &a[a_offset], lda);

/*              Copy right singular vectors of A from A to VT */

		clacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);

	    }

	} else if (*n >= mnthr2) {

/*           MNTHR2 <= N < MNTHR1 */

/*           Path 5t (N >> M, but not as much as MNTHR1) */
/*           Reduce to bidiagonal form without QR decomposition, use */
/*           CUNGBR and matrix multiplication to compute singular vectors */

	    ie = 1;
	    nrwork = ie + *m;
	    itauq = 1;
	    itaup = itauq + *m;
	    nwork = itaup + *m;

/*           Bidiagonalize A */
/*           CWorkspace: need   2*M [tauq, taup] + N        [work] */
/*           CWorkspace: prefer 2*M [tauq, taup] + (M+N)*NB [work] */
/*           RWorkspace: need   M [e] */

	    i__1 = *lwork - nwork + 1;
	    cgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[nwork], &i__1, &ierr);

	    if (wntqn) {

/*              Path 5tn (N >> M, JOBZ='N') */
/*              Compute singular values only */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + BDSPAC */

		sbdsdc_("L", "N", m, &s[1], &rwork[ie], dum, &c__1, dum, &
			c__1, dum, idum, &rwork[nrwork], &iwork[1], info);
	    } else if (wntqo) {
		irvt = nrwork;
		iru = irvt + *m * *m;
		nrwork = iru + *m * *m;
		ivt = nwork;

/*              Path 5to (N >> M, JOBZ='O') */
/*              Copy A to U, generate Q */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cungbr_("Q", m, m, n, &u[u_offset], ldu, &work[itauq], &work[
			nwork], &i__1, &ierr);

/*              Generate P**H in A */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		i__1 = *lwork - nwork + 1;
		cungbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &work[
			nwork], &i__1, &ierr);

		ldwkvt = *m;
		if (*lwork >= *m * *n + *m * 3) {

/*                 WORK( IVT ) is M by N */

		    nwork = ivt + ldwkvt * *n;
		    chunk = *n;
		} else {

/*                 WORK( IVT ) is M by CHUNK */

		    chunk = (*lwork - *m * 3) / *m;
		    nwork = ivt + ldwkvt * chunk;
		}

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC */

		sbdsdc_("L", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Multiply Q in U by real matrix RWORK(IRVT) */
/*              storing the result in WORK(IVT), copying to U */
/*              CWorkspace: need   2*M [tauq, taup] + M*M [VT] */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + 2*M*M [rwork] */

		clacrm_(m, m, &u[u_offset], ldu, &rwork[iru], m, &work[ivt], &
			ldwkvt, &rwork[nrwork]);
		clacpy_("F", m, m, &work[ivt], &ldwkvt, &u[u_offset], ldu);

/*              Multiply RWORK(IRVT) by P**H in A, storing the */
/*              result in WORK(IVT), copying to A */
/*              CWorkspace: need   2*M [tauq, taup] + M*M [VT] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*N [VT] */
/*              RWorkspace: need   M [e] + M*M [RVT] + 2*M*M [rwork] */
/*              RWorkspace: prefer M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here */

		nrwork = iru;
		i__1 = *n;
		i__2 = chunk;
		for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += 
			i__2) {
/* Computing MIN */
		    i__3 = *n - i__ + 1;
		    blk = f2cmin(i__3,chunk);
		    clarcm_(m, &blk, &rwork[irvt], m, &a[i__ * a_dim1 + 1], 
			    lda, &work[ivt], &ldwkvt, &rwork[nrwork]);
		    clacpy_("F", m, &blk, &work[ivt], &ldwkvt, &a[i__ * 
			    a_dim1 + 1], lda);
/* L50: */
		}
	    } else if (wntqs) {

/*              Path 5ts (N >> M, JOBZ='S') */
/*              Copy A to U, generate Q */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cungbr_("Q", m, m, n, &u[u_offset], ldu, &work[itauq], &work[
			nwork], &i__2, &ierr);

/*              Copy A to VT, generate P**H */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cungbr_("P", m, n, m, &vt[vt_offset], ldvt, &work[itaup], &
			work[nwork], &i__2, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC */

		irvt = nrwork;
		iru = irvt + *m * *m;
		nrwork = iru + *m * *m;
		sbdsdc_("L", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Multiply Q in U by real matrix RWORK(IRU), storing the */
/*              result in A, copying to U */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + 2*M*M [rwork] */

		clacrm_(m, m, &u[u_offset], ldu, &rwork[iru], m, &a[a_offset],
			 lda, &rwork[nrwork]);
		clacpy_("F", m, m, &a[a_offset], lda, &u[u_offset], ldu);

/*              Multiply real matrix RWORK(IRVT) by P**H in VT, */
/*              storing the result in A, copying to VT */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here */

		nrwork = iru;
		clarcm_(m, n, &rwork[irvt], m, &vt[vt_offset], ldvt, &a[
			a_offset], lda, &rwork[nrwork]);
		clacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
	    } else {

/*              Path 5ta (N >> M, JOBZ='A') */
/*              Copy A to U, generate Q */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cungbr_("Q", m, m, n, &u[u_offset], ldu, &work[itauq], &work[
			nwork], &i__2, &ierr);

/*              Copy A to VT, generate P**H */
/*              CWorkspace: need   2*M [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   0 */

		clacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__2 = *lwork - nwork + 1;
		cungbr_("P", n, n, m, &vt[vt_offset], ldvt, &work[itaup], &
			work[nwork], &i__2, &ierr);

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC */

		irvt = nrwork;
		iru = irvt + *m * *m;
		nrwork = iru + *m * *m;
		sbdsdc_("L", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Multiply Q in U by real matrix RWORK(IRU), storing the */
/*              result in A, copying to U */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + 2*M*M [rwork] */

		clacrm_(m, m, &u[u_offset], ldu, &rwork[iru], m, &a[a_offset],
			 lda, &rwork[nrwork]);
		clacpy_("F", m, m, &a[a_offset], lda, &u[u_offset], ldu);

/*              Multiply real matrix RWORK(IRVT) by P**H in VT, */
/*              storing the result in A, copying to VT */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here */

		nrwork = iru;
		clarcm_(m, n, &rwork[irvt], m, &vt[vt_offset], ldvt, &a[
			a_offset], lda, &rwork[nrwork]);
		clacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
	    }

	} else {

/*           N .LT. MNTHR2 */

/*           Path 6t (N > M, but not much larger) */
/*           Reduce to bidiagonal form without LQ decomposition */
/*           Use CUNMBR to compute singular vectors */

	    ie = 1;
	    nrwork = ie + *m;
	    itauq = 1;
	    itaup = itauq + *m;
	    nwork = itaup + *m;

/*           Bidiagonalize A */
/*           CWorkspace: need   2*M [tauq, taup] + N        [work] */
/*           CWorkspace: prefer 2*M [tauq, taup] + (M+N)*NB [work] */
/*           RWorkspace: need   M [e] */

	    i__2 = *lwork - nwork + 1;
	    cgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[nwork], &i__2, &ierr);
	    if (wntqn) {

/*              Path 6tn (N > M, JOBZ='N') */
/*              Compute singular values only */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + BDSPAC */

		sbdsdc_("L", "N", m, &s[1], &rwork[ie], dum, &c__1, dum, &
			c__1, dum, idum, &rwork[nrwork], &iwork[1], info);
	    } else if (wntqo) {
/*              Path 6to (N > M, JOBZ='O') */
		ldwkvt = *m;
		ivt = nwork;
		if (*lwork >= *m * *n + *m * 3) {

/*                 WORK( IVT ) is M by N */

		    claset_("F", m, n, &c_b1, &c_b1, &work[ivt], &ldwkvt);
		    nwork = ivt + ldwkvt * *n;
		} else {

/*                 WORK( IVT ) is M by CHUNK */

		    chunk = (*lwork - *m * 3) / *m;
		    nwork = ivt + ldwkvt * chunk;
		}

/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC */

		irvt = nrwork;
		iru = irvt + *m * *m;
		nrwork = iru + *m * *m;
		sbdsdc_("L", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of A */
/*              CWorkspace: need   2*M [tauq, taup] + M*M [VT] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*M [VT] + M*NB [work] */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] */

		clacp2_("F", m, m, &rwork[iru], m, &u[u_offset], ldu);
		i__2 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__2, &ierr);

		if (*lwork >= *m * *n + *m * 3) {

/*                 Path 6to-fast */
/*                 Copy real matrix RWORK(IRVT) to complex matrix WORK(IVT) */
/*                 Overwrite WORK(IVT) by right singular vectors of A, */
/*                 copying to A */
/*                 CWorkspace: need   2*M [tauq, taup] + M*N [VT] + M    [work] */
/*                 CWorkspace: prefer 2*M [tauq, taup] + M*N [VT] + M*NB [work] */
/*                 RWorkspace: need   M [e] + M*M [RVT] */

		    clacp2_("F", m, m, &rwork[irvt], m, &work[ivt], &ldwkvt);
		    i__2 = *lwork - nwork + 1;
		    cunmbr_("P", "R", "C", m, n, m, &a[a_offset], lda, &work[
			    itaup], &work[ivt], &ldwkvt, &work[nwork], &i__2, 
			    &ierr);
		    clacpy_("F", m, n, &work[ivt], &ldwkvt, &a[a_offset], lda);
		} else {

/*                 Path 6to-slow */
/*                 Generate P**H in A */
/*                 CWorkspace: need   2*M [tauq, taup] + M*M [VT] + M    [work] */
/*                 CWorkspace: prefer 2*M [tauq, taup] + M*M [VT] + M*NB [work] */
/*                 RWorkspace: need   0 */

		    i__2 = *lwork - nwork + 1;
		    cungbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &
			    work[nwork], &i__2, &ierr);

/*                 Multiply Q in A by real matrix RWORK(IRU), storing the */
/*                 result in WORK(IU), copying to A */
/*                 CWorkspace: need   2*M [tauq, taup] + M*M [VT] */
/*                 CWorkspace: prefer 2*M [tauq, taup] + M*N [VT] */
/*                 RWorkspace: need   M [e] + M*M [RVT] + 2*M*M [rwork] */
/*                 RWorkspace: prefer M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here */

		    nrwork = iru;
		    i__2 = *n;
		    i__1 = chunk;
		    for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ +=
			     i__1) {
/* Computing MIN */
			i__3 = *n - i__ + 1;
			blk = f2cmin(i__3,chunk);
			clarcm_(m, &blk, &rwork[irvt], m, &a[i__ * a_dim1 + 1]
				, lda, &work[ivt], &ldwkvt, &rwork[nrwork]);
			clacpy_("F", m, &blk, &work[ivt], &ldwkvt, &a[i__ * 
				a_dim1 + 1], lda);
/* L60: */
		    }
		}
	    } else if (wntqs) {

/*              Path 6ts (N > M, JOBZ='S') */
/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC */

		irvt = nrwork;
		iru = irvt + *m * *m;
		nrwork = iru + *m * *m;
		sbdsdc_("L", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of A */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] */

		clacp2_("F", m, m, &rwork[iru], m, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__1, &ierr);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of A */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   M [e] + M*M [RVT] */

		claset_("F", m, n, &c_b1, &c_b1, &vt[vt_offset], ldvt);
		clacp2_("F", m, m, &rwork[irvt], m, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", m, n, m, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__1, &
			ierr);
	    } else {

/*              Path 6ta (N > M, JOBZ='A') */
/*              Perform bidiagonal SVD, computing left singular vectors */
/*              of bidiagonal matrix in RWORK(IRU) and computing right */
/*              singular vectors of bidiagonal matrix in RWORK(IRVT) */
/*              CWorkspace: need   0 */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC */

		irvt = nrwork;
		iru = irvt + *m * *m;
		nrwork = iru + *m * *m;

		sbdsdc_("L", "I", m, &s[1], &rwork[ie], &rwork[iru], m, &
			rwork[irvt], m, dum, idum, &rwork[nrwork], &iwork[1], 
			info);

/*              Copy real matrix RWORK(IRU) to complex matrix U */
/*              Overwrite U by left singular vectors of A */
/*              CWorkspace: need   2*M [tauq, taup] + M    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work] */
/*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] */

		clacp2_("F", m, m, &rwork[iru], m, &u[u_offset], ldu);
		i__1 = *lwork - nwork + 1;
		cunmbr_("Q", "L", "N", m, m, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[nwork], &i__1, &ierr);

/*              Set all of VT to identity matrix */

		claset_("F", n, n, &c_b1, &c_b2, &vt[vt_offset], ldvt);

/*              Copy real matrix RWORK(IRVT) to complex matrix VT */
/*              Overwrite VT by right singular vectors of A */
/*              CWorkspace: need   2*M [tauq, taup] + N    [work] */
/*              CWorkspace: prefer 2*M [tauq, taup] + N*NB [work] */
/*              RWorkspace: need   M [e] + M*M [RVT] */

		clacp2_("F", m, m, &rwork[irvt], m, &vt[vt_offset], ldvt);
		i__1 = *lwork - nwork + 1;
		cunmbr_("P", "R", "C", n, n, m, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[nwork], &i__1, &
			ierr);
	    }

	}

    }

/*     Undo scaling if necessary */

    if (iscl == 1) {
	if (anrm > bignum) {
	    slascl_("G", &c__0, &c__0, &bignum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, &ierr);
	}
	if (*info != 0 && anrm > bignum) {
	    i__1 = minmn - 1;
	    slascl_("G", &c__0, &c__0, &bignum, &anrm, &i__1, &c__1, &rwork[
		    ie], &minmn, &ierr);
	}
	if (anrm < smlnum) {
	    slascl_("G", &c__0, &c__0, &smlnum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, &ierr);
	}
	if (*info != 0 && anrm < smlnum) {
	    i__1 = minmn - 1;
	    slascl_("G", &c__0, &c__0, &smlnum, &anrm, &i__1, &c__1, &rwork[
		    ie], &minmn, &ierr);
	}
    }

/*     Return optimal workspace in WORK(1) */

    work[1].r = (real) maxwrk, work[1].i = 0.f;

    return;

/*     End of CGESDD */

} /* cgesdd_ */


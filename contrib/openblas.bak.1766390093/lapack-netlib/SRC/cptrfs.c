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
static complex c_b16 = {1.f,0.f};

/* > \brief \b CPTRFS */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CPTRFS + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cptrfs.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cptrfs.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cptrfs.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CPTRFS( UPLO, N, NRHS, D, E, DF, EF, B, LDB, X, LDX, */
/*                          FERR, BERR, WORK, RWORK, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            INFO, LDB, LDX, N, NRHS */
/*       REAL               BERR( * ), D( * ), DF( * ), FERR( * ), */
/*      $                   RWORK( * ) */
/*       COMPLEX            B( LDB, * ), E( * ), EF( * ), WORK( * ), */
/*      $                   X( LDX, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CPTRFS improves the computed solution to a system of linear */
/* > equations when the coefficient matrix is Hermitian positive definite */
/* > and tridiagonal, and provides error bounds and backward error */
/* > estimates for the solution. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          Specifies whether the superdiagonal or the subdiagonal of the */
/* >          tridiagonal matrix A is stored and the form of the */
/* >          factorization: */
/* >          = 'U':  E is the superdiagonal of A, and A = U**H*D*U; */
/* >          = 'L':  E is the subdiagonal of A, and A = L*D*L**H. */
/* >          (The two forms are equivalent if A is real.) */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >          The number of right hand sides, i.e., the number of columns */
/* >          of the matrix B.  NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is REAL array, dimension (N) */
/* >          The n real diagonal elements of the tridiagonal matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is COMPLEX array, dimension (N-1) */
/* >          The (n-1) off-diagonal elements of the tridiagonal matrix A */
/* >          (see UPLO). */
/* > \endverbatim */
/* > */
/* > \param[in] DF */
/* > \verbatim */
/* >          DF is REAL array, dimension (N) */
/* >          The n diagonal elements of the diagonal matrix D from */
/* >          the factorization computed by CPTTRF. */
/* > \endverbatim */
/* > */
/* > \param[in] EF */
/* > \verbatim */
/* >          EF is COMPLEX array, dimension (N-1) */
/* >          The (n-1) off-diagonal elements of the unit bidiagonal */
/* >          factor U or L from the factorization computed by CPTTRF */
/* >          (see UPLO). */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDB,NRHS) */
/* >          The right hand side matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X */
/* > \verbatim */
/* >          X is COMPLEX array, dimension (LDX,NRHS) */
/* >          On entry, the solution matrix X, as computed by CPTTRS. */
/* >          On exit, the improved solution matrix X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >          The leading dimension of the array X.  LDX >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] FERR */
/* > \verbatim */
/* >          FERR is REAL array, dimension (NRHS) */
/* >          The forward error bound for each solution vector */
/* >          X(j) (the j-th column of the solution matrix X). */
/* >          If XTRUE is the true solution corresponding to X(j), FERR(j) */
/* >          is an estimated upper bound for the magnitude of the largest */
/* >          element in (X(j) - XTRUE) divided by the magnitude of the */
/* >          largest element in X(j). */
/* > \endverbatim */
/* > */
/* > \param[out] BERR */
/* > \verbatim */
/* >          BERR is REAL array, dimension (NRHS) */
/* >          The componentwise relative backward error of each solution */
/* >          vector X(j) (i.e., the smallest relative change in */
/* >          any element of A or B that makes X(j) an exact solution). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/* > \par Internal Parameters: */
/*  ========================= */
/* > */
/* > \verbatim */
/* >  ITMAX is the maximum number of steps of iterative refinement. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complexPTcomputational */

/*  ===================================================================== */
/* Subroutine */ void cptrfs_(char *uplo, integer *n, integer *nrhs, real *d__,
	 complex *e, real *df, complex *ef, complex *b, integer *ldb, complex 
	*x, integer *ldx, real *ferr, real *berr, complex *work, real *rwork, 
	integer *info)
{
    /* System generated locals */
    integer b_dim1, b_offset, x_dim1, x_offset, i__1, i__2, i__3, i__4, i__5, 
	    i__6;
    real r__1, r__2, r__3, r__4, r__5, r__6, r__7, r__8, r__9, r__10, r__11, 
	    r__12;
    complex q__1, q__2, q__3;

    /* Local variables */
    real safe1, safe2;
    integer i__, j;
    real s;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void caxpy_(integer *, complex *, complex *, 
	    integer *, complex *, integer *);
    integer count;
    logical upper;
    complex bi, cx, dx, ex;
    integer ix;
    extern real slamch_(char *);
    integer nz;
    real safmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer isamax_(integer *, real *, integer *);
    real lstres;
    extern /* Subroutine */ void cpttrs_(char *, integer *, integer *, real *, 
	    complex *, complex *, integer *, integer *);
    real eps;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    --e;
    --df;
    --ef;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    --ferr;
    --berr;
    --work;
    --rwork;

    /* Function Body */
    *info = 0;
    upper = lsame_(uplo, "U");
    if (! upper && ! lsame_(uplo, "L")) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*nrhs < 0) {
	*info = -3;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -9;
    } else if (*ldx < f2cmax(1,*n)) {
	*info = -11;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CPTRFS", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0 || *nrhs == 0) {
	i__1 = *nrhs;
	for (j = 1; j <= i__1; ++j) {
	    ferr[j] = 0.f;
	    berr[j] = 0.f;
/* L10: */
	}
	return;
    }

/*     NZ = maximum number of nonzero elements in each row of A, plus 1 */

    nz = 4;
    eps = slamch_("Epsilon");
    safmin = slamch_("Safe minimum");
    safe1 = nz * safmin;
    safe2 = safe1 / eps;

/*     Do for each right hand side */

    i__1 = *nrhs;
    for (j = 1; j <= i__1; ++j) {

	count = 1;
	lstres = 3.f;
L20:

/*        Loop until stopping criterion is satisfied. */

/*        Compute residual R = B - A * X.  Also compute */
/*        abs(A)*abs(x) + abs(b) for use in the backward error bound. */

	if (upper) {
	    if (*n == 1) {
		i__2 = j * b_dim1 + 1;
		bi.r = b[i__2].r, bi.i = b[i__2].i;
		i__2 = j * x_dim1 + 1;
		q__1.r = d__[1] * x[i__2].r, q__1.i = d__[1] * x[i__2].i;
		dx.r = q__1.r, dx.i = q__1.i;
		q__1.r = bi.r - dx.r, q__1.i = bi.i - dx.i;
		work[1].r = q__1.r, work[1].i = q__1.i;
		rwork[1] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&bi), 
			abs(r__2)) + ((r__3 = dx.r, abs(r__3)) + (r__4 = 
			r_imag(&dx), abs(r__4)));
	    } else {
		i__2 = j * b_dim1 + 1;
		bi.r = b[i__2].r, bi.i = b[i__2].i;
		i__2 = j * x_dim1 + 1;
		q__1.r = d__[1] * x[i__2].r, q__1.i = d__[1] * x[i__2].i;
		dx.r = q__1.r, dx.i = q__1.i;
		i__2 = j * x_dim1 + 2;
		q__1.r = e[1].r * x[i__2].r - e[1].i * x[i__2].i, q__1.i = e[
			1].r * x[i__2].i + e[1].i * x[i__2].r;
		ex.r = q__1.r, ex.i = q__1.i;
		q__2.r = bi.r - dx.r, q__2.i = bi.i - dx.i;
		q__1.r = q__2.r - ex.r, q__1.i = q__2.i - ex.i;
		work[1].r = q__1.r, work[1].i = q__1.i;
		i__2 = j * x_dim1 + 2;
		rwork[1] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&bi), 
			abs(r__2)) + ((r__3 = dx.r, abs(r__3)) + (r__4 = 
			r_imag(&dx), abs(r__4))) + ((r__5 = e[1].r, abs(r__5))
			 + (r__6 = r_imag(&e[1]), abs(r__6))) * ((r__7 = x[
			i__2].r, abs(r__7)) + (r__8 = r_imag(&x[j * x_dim1 + 
			2]), abs(r__8)));
		i__2 = *n - 1;
		for (i__ = 2; i__ <= i__2; ++i__) {
		    i__3 = i__ + j * b_dim1;
		    bi.r = b[i__3].r, bi.i = b[i__3].i;
		    r_cnjg(&q__2, &e[i__ - 1]);
		    i__3 = i__ - 1 + j * x_dim1;
		    q__1.r = q__2.r * x[i__3].r - q__2.i * x[i__3].i, q__1.i =
			     q__2.r * x[i__3].i + q__2.i * x[i__3].r;
		    cx.r = q__1.r, cx.i = q__1.i;
		    i__3 = i__;
		    i__4 = i__ + j * x_dim1;
		    q__1.r = d__[i__3] * x[i__4].r, q__1.i = d__[i__3] * x[
			    i__4].i;
		    dx.r = q__1.r, dx.i = q__1.i;
		    i__3 = i__;
		    i__4 = i__ + 1 + j * x_dim1;
		    q__1.r = e[i__3].r * x[i__4].r - e[i__3].i * x[i__4].i, 
			    q__1.i = e[i__3].r * x[i__4].i + e[i__3].i * x[
			    i__4].r;
		    ex.r = q__1.r, ex.i = q__1.i;
		    i__3 = i__;
		    q__3.r = bi.r - cx.r, q__3.i = bi.i - cx.i;
		    q__2.r = q__3.r - dx.r, q__2.i = q__3.i - dx.i;
		    q__1.r = q__2.r - ex.r, q__1.i = q__2.i - ex.i;
		    work[i__3].r = q__1.r, work[i__3].i = q__1.i;
		    i__3 = i__ - 1;
		    i__4 = i__ - 1 + j * x_dim1;
		    i__5 = i__;
		    i__6 = i__ + 1 + j * x_dim1;
		    rwork[i__] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&
			    bi), abs(r__2)) + ((r__3 = e[i__3].r, abs(r__3)) 
			    + (r__4 = r_imag(&e[i__ - 1]), abs(r__4))) * ((
			    r__5 = x[i__4].r, abs(r__5)) + (r__6 = r_imag(&x[
			    i__ - 1 + j * x_dim1]), abs(r__6))) + ((r__7 = 
			    dx.r, abs(r__7)) + (r__8 = r_imag(&dx), abs(r__8))
			    ) + ((r__9 = e[i__5].r, abs(r__9)) + (r__10 = 
			    r_imag(&e[i__]), abs(r__10))) * ((r__11 = x[i__6]
			    .r, abs(r__11)) + (r__12 = r_imag(&x[i__ + 1 + j *
			     x_dim1]), abs(r__12)));
/* L30: */
		}
		i__2 = *n + j * b_dim1;
		bi.r = b[i__2].r, bi.i = b[i__2].i;
		r_cnjg(&q__2, &e[*n - 1]);
		i__2 = *n - 1 + j * x_dim1;
		q__1.r = q__2.r * x[i__2].r - q__2.i * x[i__2].i, q__1.i = 
			q__2.r * x[i__2].i + q__2.i * x[i__2].r;
		cx.r = q__1.r, cx.i = q__1.i;
		i__2 = *n;
		i__3 = *n + j * x_dim1;
		q__1.r = d__[i__2] * x[i__3].r, q__1.i = d__[i__2] * x[i__3]
			.i;
		dx.r = q__1.r, dx.i = q__1.i;
		i__2 = *n;
		q__2.r = bi.r - cx.r, q__2.i = bi.i - cx.i;
		q__1.r = q__2.r - dx.r, q__1.i = q__2.i - dx.i;
		work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		i__2 = *n - 1;
		i__3 = *n - 1 + j * x_dim1;
		rwork[*n] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&bi), 
			abs(r__2)) + ((r__3 = e[i__2].r, abs(r__3)) + (r__4 = 
			r_imag(&e[*n - 1]), abs(r__4))) * ((r__5 = x[i__3].r, 
			abs(r__5)) + (r__6 = r_imag(&x[*n - 1 + j * x_dim1]), 
			abs(r__6))) + ((r__7 = dx.r, abs(r__7)) + (r__8 = 
			r_imag(&dx), abs(r__8)));
	    }
	} else {
	    if (*n == 1) {
		i__2 = j * b_dim1 + 1;
		bi.r = b[i__2].r, bi.i = b[i__2].i;
		i__2 = j * x_dim1 + 1;
		q__1.r = d__[1] * x[i__2].r, q__1.i = d__[1] * x[i__2].i;
		dx.r = q__1.r, dx.i = q__1.i;
		q__1.r = bi.r - dx.r, q__1.i = bi.i - dx.i;
		work[1].r = q__1.r, work[1].i = q__1.i;
		rwork[1] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&bi), 
			abs(r__2)) + ((r__3 = dx.r, abs(r__3)) + (r__4 = 
			r_imag(&dx), abs(r__4)));
	    } else {
		i__2 = j * b_dim1 + 1;
		bi.r = b[i__2].r, bi.i = b[i__2].i;
		i__2 = j * x_dim1 + 1;
		q__1.r = d__[1] * x[i__2].r, q__1.i = d__[1] * x[i__2].i;
		dx.r = q__1.r, dx.i = q__1.i;
		r_cnjg(&q__2, &e[1]);
		i__2 = j * x_dim1 + 2;
		q__1.r = q__2.r * x[i__2].r - q__2.i * x[i__2].i, q__1.i = 
			q__2.r * x[i__2].i + q__2.i * x[i__2].r;
		ex.r = q__1.r, ex.i = q__1.i;
		q__2.r = bi.r - dx.r, q__2.i = bi.i - dx.i;
		q__1.r = q__2.r - ex.r, q__1.i = q__2.i - ex.i;
		work[1].r = q__1.r, work[1].i = q__1.i;
		i__2 = j * x_dim1 + 2;
		rwork[1] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&bi), 
			abs(r__2)) + ((r__3 = dx.r, abs(r__3)) + (r__4 = 
			r_imag(&dx), abs(r__4))) + ((r__5 = e[1].r, abs(r__5))
			 + (r__6 = r_imag(&e[1]), abs(r__6))) * ((r__7 = x[
			i__2].r, abs(r__7)) + (r__8 = r_imag(&x[j * x_dim1 + 
			2]), abs(r__8)));
		i__2 = *n - 1;
		for (i__ = 2; i__ <= i__2; ++i__) {
		    i__3 = i__ + j * b_dim1;
		    bi.r = b[i__3].r, bi.i = b[i__3].i;
		    i__3 = i__ - 1;
		    i__4 = i__ - 1 + j * x_dim1;
		    q__1.r = e[i__3].r * x[i__4].r - e[i__3].i * x[i__4].i, 
			    q__1.i = e[i__3].r * x[i__4].i + e[i__3].i * x[
			    i__4].r;
		    cx.r = q__1.r, cx.i = q__1.i;
		    i__3 = i__;
		    i__4 = i__ + j * x_dim1;
		    q__1.r = d__[i__3] * x[i__4].r, q__1.i = d__[i__3] * x[
			    i__4].i;
		    dx.r = q__1.r, dx.i = q__1.i;
		    r_cnjg(&q__2, &e[i__]);
		    i__3 = i__ + 1 + j * x_dim1;
		    q__1.r = q__2.r * x[i__3].r - q__2.i * x[i__3].i, q__1.i =
			     q__2.r * x[i__3].i + q__2.i * x[i__3].r;
		    ex.r = q__1.r, ex.i = q__1.i;
		    i__3 = i__;
		    q__3.r = bi.r - cx.r, q__3.i = bi.i - cx.i;
		    q__2.r = q__3.r - dx.r, q__2.i = q__3.i - dx.i;
		    q__1.r = q__2.r - ex.r, q__1.i = q__2.i - ex.i;
		    work[i__3].r = q__1.r, work[i__3].i = q__1.i;
		    i__3 = i__ - 1;
		    i__4 = i__ - 1 + j * x_dim1;
		    i__5 = i__;
		    i__6 = i__ + 1 + j * x_dim1;
		    rwork[i__] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&
			    bi), abs(r__2)) + ((r__3 = e[i__3].r, abs(r__3)) 
			    + (r__4 = r_imag(&e[i__ - 1]), abs(r__4))) * ((
			    r__5 = x[i__4].r, abs(r__5)) + (r__6 = r_imag(&x[
			    i__ - 1 + j * x_dim1]), abs(r__6))) + ((r__7 = 
			    dx.r, abs(r__7)) + (r__8 = r_imag(&dx), abs(r__8))
			    ) + ((r__9 = e[i__5].r, abs(r__9)) + (r__10 = 
			    r_imag(&e[i__]), abs(r__10))) * ((r__11 = x[i__6]
			    .r, abs(r__11)) + (r__12 = r_imag(&x[i__ + 1 + j *
			     x_dim1]), abs(r__12)));
/* L40: */
		}
		i__2 = *n + j * b_dim1;
		bi.r = b[i__2].r, bi.i = b[i__2].i;
		i__2 = *n - 1;
		i__3 = *n - 1 + j * x_dim1;
		q__1.r = e[i__2].r * x[i__3].r - e[i__2].i * x[i__3].i, 
			q__1.i = e[i__2].r * x[i__3].i + e[i__2].i * x[i__3]
			.r;
		cx.r = q__1.r, cx.i = q__1.i;
		i__2 = *n;
		i__3 = *n + j * x_dim1;
		q__1.r = d__[i__2] * x[i__3].r, q__1.i = d__[i__2] * x[i__3]
			.i;
		dx.r = q__1.r, dx.i = q__1.i;
		i__2 = *n;
		q__2.r = bi.r - cx.r, q__2.i = bi.i - cx.i;
		q__1.r = q__2.r - dx.r, q__1.i = q__2.i - dx.i;
		work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		i__2 = *n - 1;
		i__3 = *n - 1 + j * x_dim1;
		rwork[*n] = (r__1 = bi.r, abs(r__1)) + (r__2 = r_imag(&bi), 
			abs(r__2)) + ((r__3 = e[i__2].r, abs(r__3)) + (r__4 = 
			r_imag(&e[*n - 1]), abs(r__4))) * ((r__5 = x[i__3].r, 
			abs(r__5)) + (r__6 = r_imag(&x[*n - 1 + j * x_dim1]), 
			abs(r__6))) + ((r__7 = dx.r, abs(r__7)) + (r__8 = 
			r_imag(&dx), abs(r__8)));
	    }
	}

/*        Compute componentwise relative backward error from formula */

/*        f2cmax(i) ( abs(R(i)) / ( abs(A)*abs(X) + abs(B) )(i) ) */

/*        where abs(Z) is the componentwise absolute value of the matrix */
/*        or vector Z.  If the i-th component of the denominator is less */
/*        than SAFE2, then SAFE1 is added to the i-th components of the */
/*        numerator and denominator before dividing. */

	s = 0.f;
	i__2 = *n;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    if (rwork[i__] > safe2) {
/* Computing MAX */
		i__3 = i__;
		r__3 = s, r__4 = ((r__1 = work[i__3].r, abs(r__1)) + (r__2 = 
			r_imag(&work[i__]), abs(r__2))) / rwork[i__];
		s = f2cmax(r__3,r__4);
	    } else {
/* Computing MAX */
		i__3 = i__;
		r__3 = s, r__4 = ((r__1 = work[i__3].r, abs(r__1)) + (r__2 = 
			r_imag(&work[i__]), abs(r__2)) + safe1) / (rwork[i__] 
			+ safe1);
		s = f2cmax(r__3,r__4);
	    }
/* L50: */
	}
	berr[j] = s;

/*        Test stopping criterion. Continue iterating if */
/*           1) The residual BERR(J) is larger than machine epsilon, and */
/*           2) BERR(J) decreased by at least a factor of 2 during the */
/*              last iteration, and */
/*           3) At most ITMAX iterations tried. */

	if (berr[j] > eps && berr[j] * 2.f <= lstres && count <= 5) {

/*           Update solution and try again. */

	    cpttrs_(uplo, n, &c__1, &df[1], &ef[1], &work[1], n, info);
	    caxpy_(n, &c_b16, &work[1], &c__1, &x[j * x_dim1 + 1], &c__1);
	    lstres = berr[j];
	    ++count;
	    goto L20;
	}

/*        Bound error from formula */

/*        norm(X - XTRUE) / norm(X) .le. FERR = */
/*        norm( abs(inv(A))* */
/*           ( abs(R) + NZ*EPS*( abs(A)*abs(X)+abs(B) ))) / norm(X) */

/*        where */
/*          norm(Z) is the magnitude of the largest component of Z */
/*          inv(A) is the inverse of A */
/*          abs(Z) is the componentwise absolute value of the matrix or */
/*             vector Z */
/*          NZ is the maximum number of nonzeros in any row of A, plus 1 */
/*          EPS is machine epsilon */

/*        The i-th component of abs(R)+NZ*EPS*(abs(A)*abs(X)+abs(B)) */
/*        is incremented by SAFE1 if the i-th component of */
/*        abs(A)*abs(X) + abs(B) is less than SAFE2. */

	i__2 = *n;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    if (rwork[i__] > safe2) {
		i__3 = i__;
		rwork[i__] = (r__1 = work[i__3].r, abs(r__1)) + (r__2 = 
			r_imag(&work[i__]), abs(r__2)) + nz * eps * rwork[i__]
			;
	    } else {
		i__3 = i__;
		rwork[i__] = (r__1 = work[i__3].r, abs(r__1)) + (r__2 = 
			r_imag(&work[i__]), abs(r__2)) + nz * eps * rwork[i__]
			 + safe1;
	    }
/* L60: */
	}
	ix = isamax_(n, &rwork[1], &c__1);
	ferr[j] = rwork[ix];

/*        Estimate the norm of inv(A). */

/*        Solve M(A) * x = e, where M(A) = (m(i,j)) is given by */

/*           m(i,j) =  abs(A(i,j)), i = j, */
/*           m(i,j) = -abs(A(i,j)), i .ne. j, */

/*        and e = [ 1, 1, ..., 1 ]**T.  Note M(A) = M(L)*D*M(L)**H. */

/*        Solve M(L) * x = e. */

	rwork[1] = 1.f;
	i__2 = *n;
	for (i__ = 2; i__ <= i__2; ++i__) {
	    rwork[i__] = rwork[i__ - 1] * c_abs(&ef[i__ - 1]) + 1.f;
/* L70: */
	}

/*        Solve D * M(L)**H * x = b. */

	rwork[*n] /= df[*n];
	for (i__ = *n - 1; i__ >= 1; --i__) {
	    rwork[i__] = rwork[i__] / df[i__] + rwork[i__ + 1] * c_abs(&ef[
		    i__]);
/* L80: */
	}

/*        Compute norm(inv(A)) = f2cmax(x(i)), 1<=i<=n. */

	ix = isamax_(n, &rwork[1], &c__1);
	ferr[j] *= (r__1 = rwork[ix], abs(r__1));

/*        Normalize error. */

	lstres = 0.f;
	i__2 = *n;
	for (i__ = 1; i__ <= i__2; ++i__) {
/* Computing MAX */
	    r__1 = lstres, r__2 = c_abs(&x[i__ + j * x_dim1]);
	    lstres = f2cmax(r__1,r__2);
/* L90: */
	}
	if (lstres != 0.f) {
	    ferr[j] /= lstres;
	}

/* L100: */
    }

    return;

/*     End of CPTRFS */

} /* cptrfs_ */


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
static real c_b19 = 1.f;
static real c_b21 = 0.f;
static integer c__2 = 2;
static logical c_false = FALSE_;
static integer c__3 = 3;

/* > \brief \b STGSNA */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download STGSNA + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/stgsna.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/stgsna.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/stgsna.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE STGSNA( JOB, HOWMNY, SELECT, N, A, LDA, B, LDB, VL, */
/*                          LDVL, VR, LDVR, S, DIF, MM, M, WORK, LWORK, */
/*                          IWORK, INFO ) */

/*       CHARACTER          HOWMNY, JOB */
/*       INTEGER            INFO, LDA, LDB, LDVL, LDVR, LWORK, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       INTEGER            IWORK( * ) */
/*       REAL               A( LDA, * ), B( LDB, * ), DIF( * ), S( * ), */
/*      $                   VL( LDVL, * ), VR( LDVR, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > STGSNA estimates reciprocal condition numbers for specified */
/* > eigenvalues and/or eigenvectors of a matrix pair (A, B) in */
/* > generalized real Schur canonical form (or of any matrix pair */
/* > (Q*A*Z**T, Q*B*Z**T) with orthogonal matrices Q and Z, where */
/* > Z**T denotes the transpose of Z. */
/* > */
/* > (A, B) must be in generalized real Schur form (as returned by SGGES), */
/* > i.e. A is block upper triangular with 1-by-1 and 2-by-2 diagonal */
/* > blocks. B is upper triangular. */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOB */
/* > \verbatim */
/* >          JOB is CHARACTER*1 */
/* >          Specifies whether condition numbers are required for */
/* >          eigenvalues (S) or eigenvectors (DIF): */
/* >          = 'E': for eigenvalues only (S); */
/* >          = 'V': for eigenvectors only (DIF); */
/* >          = 'B': for both eigenvalues and eigenvectors (S and DIF). */
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
/* >          The order of the square matrix pair (A, B). N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,N) */
/* >          The upper quasi-triangular matrix A in the pair (A,B). */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB,N) */
/* >          The upper triangular matrix B in the pair (A,B). */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] VL */
/* > \verbatim */
/* >          VL is REAL array, dimension (LDVL,M) */
/* >          If JOB = 'E' or 'B', VL must contain left eigenvectors of */
/* >          (A, B), corresponding to the eigenpairs specified by HOWMNY */
/* >          and SELECT. The eigenvectors must be stored in consecutive */
/* >          columns of VL, as returned by STGEVC. */
/* >          If JOB = 'V', VL is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of the array VL. LDVL >= 1. */
/* >          If JOB = 'E' or 'B', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] VR */
/* > \verbatim */
/* >          VR is REAL array, dimension (LDVR,M) */
/* >          If JOB = 'E' or 'B', VR must contain right eigenvectors of */
/* >          (A, B), corresponding to the eigenpairs specified by HOWMNY */
/* >          and SELECT. The eigenvectors must be stored in consecutive */
/* >          columns ov VR, as returned by STGEVC. */
/* >          If JOB = 'V', VR is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the array VR. LDVR >= 1. */
/* >          If JOB = 'E' or 'B', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL array, dimension (MM) */
/* >          If JOB = 'E' or 'B', the reciprocal condition numbers of the */
/* >          selected eigenvalues, stored in consecutive elements of the */
/* >          array. For a complex conjugate pair of eigenvalues two */
/* >          consecutive elements of S are set to the same value. Thus */
/* >          S(j), DIF(j), and the j-th columns of VL and VR all */
/* >          correspond to the same eigenpair (but not in general the */
/* >          j-th eigenpair, unless all eigenpairs are selected). */
/* >          If JOB = 'V', S is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] DIF */
/* > \verbatim */
/* >          DIF is REAL array, dimension (MM) */
/* >          If JOB = 'V' or 'B', the estimated reciprocal condition */
/* >          numbers of the selected eigenvectors, stored in consecutive */
/* >          elements of the array. For a complex eigenvector two */
/* >          consecutive elements of DIF are set to the same value. If */
/* >          the eigenvalues cannot be reordered to compute DIF(j), DIF(j) */
/* >          is set to 0; this can only occur when the true value would be */
/* >          very small anyway. */
/* >          If JOB = 'E', DIF is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] MM */
/* > \verbatim */
/* >          MM is INTEGER */
/* >          The number of elements in the arrays S and DIF. MM >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of elements of the arrays S and DIF used to store */
/* >          the specified condition numbers; for each selected real */
/* >          eigenvalue one element is used, and for each selected complex */
/* >          conjugate pair of eigenvalues, two elements are used. */
/* >          If HOWMNY = 'A', M is set to N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= f2cmax(1,N). */
/* >          If JOB = 'V' or 'B' LWORK >= 2*N*(N+2)+16. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (N + 6) */
/* >          If JOB = 'E', IWORK is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          =0: Successful exit */
/* >          <0: If INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The reciprocal of the condition number of a generalized eigenvalue */
/* >  w = (a, b) is defined as */
/* > */
/* >       S(w) = (|u**TAv|**2 + |u**TBv|**2)**(1/2) / (norm(u)*norm(v)) */
/* > */
/* >  where u and v are the left and right eigenvectors of (A, B) */
/* >  corresponding to w; |z| denotes the absolute value of the complex */
/* >  number, and norm(u) denotes the 2-norm of the vector u. */
/* >  The pair (a, b) corresponds to an eigenvalue w = a/b (= u**TAv/u**TBv) */
/* >  of the matrix pair (A, B). If both a and b equal zero, then (A B) is */
/* >  singular and S(I) = -1 is returned. */
/* > */
/* >  An approximate error bound on the chordal distance between the i-th */
/* >  computed generalized eigenvalue w and the corresponding exact */
/* >  eigenvalue lambda is */
/* > */
/* >       chord(w, lambda) <= EPS * norm(A, B) / S(I) */
/* > */
/* >  where EPS is the machine precision. */
/* > */
/* >  The reciprocal of the condition number DIF(i) of right eigenvector u */
/* >  and left eigenvector v corresponding to the generalized eigenvalue w */
/* >  is defined as follows: */
/* > */
/* >  a) If the i-th eigenvalue w = (a,b) is real */
/* > */
/* >     Suppose U and V are orthogonal transformations such that */
/* > */
/* >              U**T*(A, B)*V  = (S, T) = ( a   *  ) ( b  *  )  1 */
/* >                                        ( 0  S22 ),( 0 T22 )  n-1 */
/* >                                          1  n-1     1 n-1 */
/* > */
/* >     Then the reciprocal condition number DIF(i) is */
/* > */
/* >                Difl((a, b), (S22, T22)) = sigma-f2cmin( Zl ), */
/* > */
/* >     where sigma-f2cmin(Zl) denotes the smallest singular value of the */
/* >     2(n-1)-by-2(n-1) matrix */
/* > */
/* >         Zl = [ kron(a, In-1)  -kron(1, S22) ] */
/* >              [ kron(b, In-1)  -kron(1, T22) ] . */
/* > */
/* >     Here In-1 is the identity matrix of size n-1. kron(X, Y) is the */
/* >     Kronecker product between the matrices X and Y. */
/* > */
/* >     Note that if the default method for computing DIF(i) is wanted */
/* >     (see SLATDF), then the parameter DIFDRI (see below) should be */
/* >     changed from 3 to 4 (routine SLATDF(IJOB = 2 will be used)). */
/* >     See STGSYL for more details. */
/* > */
/* >  b) If the i-th and (i+1)-th eigenvalues are complex conjugate pair, */
/* > */
/* >     Suppose U and V are orthogonal transformations such that */
/* > */
/* >              U**T*(A, B)*V = (S, T) = ( S11  *   ) ( T11  *  )  2 */
/* >                                       ( 0    S22 ),( 0    T22) n-2 */
/* >                                         2    n-2     2    n-2 */
/* > */
/* >     and (S11, T11) corresponds to the complex conjugate eigenvalue */
/* >     pair (w, conjg(w)). There exist unitary matrices U1 and V1 such */
/* >     that */
/* > */
/* >       U1**T*S11*V1 = ( s11 s12 ) and U1**T*T11*V1 = ( t11 t12 ) */
/* >                      (  0  s22 )                    (  0  t22 ) */
/* > */
/* >     where the generalized eigenvalues w = s11/t11 and */
/* >     conjg(w) = s22/t22. */
/* > */
/* >     Then the reciprocal condition number DIF(i) is bounded by */
/* > */
/* >         f2cmin( d1, f2cmax( 1, |real(s11)/real(s22)| )*d2 ) */
/* > */
/* >     where, d1 = Difl((s11, t11), (s22, t22)) = sigma-f2cmin(Z1), where */
/* >     Z1 is the complex 2-by-2 matrix */
/* > */
/* >              Z1 =  [ s11  -s22 ] */
/* >                    [ t11  -t22 ], */
/* > */
/* >     This is done by computing (using real arithmetic) the */
/* >     roots of the characteristical polynomial det(Z1**T * Z1 - lambda I), */
/* >     where Z1**T denotes the transpose of Z1 and det(X) denotes */
/* >     the determinant of X. */
/* > */
/* >     and d2 is an upper bound on Difl((S11, T11), (S22, T22)), i.e. an */
/* >     upper bound on sigma-f2cmin(Z2), where Z2 is (2n-2)-by-(2n-2) */
/* > */
/* >              Z2 = [ kron(S11**T, In-2)  -kron(I2, S22) ] */
/* >                   [ kron(T11**T, In-2)  -kron(I2, T22) ] */
/* > */
/* >     Note that if the default method for computing DIF is wanted (see */
/* >     SLATDF), then the parameter DIFDRI (see below) should be changed */
/* >     from 3 to 4 (routine SLATDF(IJOB = 2 will be used)). See STGSYL */
/* >     for more details. */
/* > */
/* >  For each eigenvalue/vector specified by SELECT, DIF stores a */
/* >  Frobenius norm-based estimate of Difl. */
/* > */
/* >  An approximate error bound for the i-th computed eigenvector VL(i) or */
/* >  VR(i) is given by */
/* > */
/* >             EPS * norm(A, B) / DIF(i). */
/* > */
/* >  See ref. [2-3] for more details and further references. */
/* > \endverbatim */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Bo Kagstrom and Peter Poromaa, Department of Computing Science, */
/* >     Umea University, S-901 87 Umea, Sweden. */

/* > \par References: */
/*  ================ */
/* > */
/* > \verbatim */
/* > */
/* >  [1] B. Kagstrom; A Direct Method for Reordering Eigenvalues in the */
/* >      Generalized Real Schur Form of a Regular Matrix Pair (A, B), in */
/* >      M.S. Moonen et al (eds), Linear Algebra for Large Scale and */
/* >      Real-Time Applications, Kluwer Academic Publ. 1993, pp 195-218. */
/* > */
/* >  [2] B. Kagstrom and P. Poromaa; Computing Eigenspaces with Specified */
/* >      Eigenvalues of a Regular Matrix Pair (A, B) and Condition */
/* >      Estimation: Theory, Algorithms and Software, */
/* >      Report UMINF - 94.04, Department of Computing Science, Umea */
/* >      University, S-901 87 Umea, Sweden, 1994. Also as LAPACK Working */
/* >      Note 87. To appear in Numerical Algorithms, 1996. */
/* > */
/* >  [3] B. Kagstrom and P. Poromaa, LAPACK-Style Algorithms and Software */
/* >      for Solving the Generalized Sylvester Equation and Estimating the */
/* >      Separation between Regular Matrix Pairs, Report UMINF - 93.23, */
/* >      Department of Computing Science, Umea University, S-901 87 Umea, */
/* >      Sweden, December 1993, Revised April 1994, Also as LAPACK Working */
/* >      Note 75.  To appear in ACM Trans. on Math. Software, Vol 22, */
/* >      No 1, 1996. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void stgsna_(char *job, char *howmny, logical *select, 
	integer *n, real *a, integer *lda, real *b, integer *ldb, real *vl, 
	integer *ldvl, real *vr, integer *ldvr, real *s, real *dif, integer *
	mm, integer *m, real *work, integer *lwork, integer *iwork, integer *
	info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, vl_dim1, vl_offset, vr_dim1, 
	    vr_offset, i__1, i__2;
    real r__1, r__2;

    /* Local variables */
    real beta, cond;
    logical pair;
    integer ierr;
    real uhav, uhbv;
    integer ifst;
    real lnrm;
    extern real sdot_(integer *, real *, integer *, real *, integer *);
    integer ilst;
    real rnrm;
    extern /* Subroutine */ void slag2_(real *, integer *, real *, integer *, 
	    real *, real *, real *, real *, real *, real *);
    extern real snrm2_(integer *, real *, integer *);
    real root1, root2;
    integer i__, k;
    real scale;
    extern logical lsame_(char *, char *);
    real uhavi, uhbvi;
    extern /* Subroutine */ void sgemv_(char *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *);
    real tmpii, c1, c2;
    integer lwmin;
    logical wants;
    real tmpir;
    integer n1, n2;
    real tmpri, dummy[1], tmprr;
    extern real slapy2_(real *, real *);
    real dummy1[1];
    integer ks;
    real alphai;
    integer iz;
    real alphar;
    extern real slamch_(char *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    logical wantbh, wantdf;
    extern /* Subroutine */ void slacpy_(char *, integer *, integer *, real *, 
	    integer *, real *, integer *), stgexc_(logical *, logical 
	    *, integer *, real *, integer *, real *, integer *, real *, 
	    integer *, real *, integer *, integer *, integer *, real *, 
	    integer *, integer *);
    logical somcon;
    real alprqt, smlnum;
    logical lquery;
    extern /* Subroutine */ void stgsyl_(char *, integer *, integer *, integer 
	    *, real *, integer *, real *, integer *, real *, integer *, real *
	    , integer *, real *, integer *, real *, integer *, real *, real *,
	     real *, integer *, integer *, integer *);
    real eps;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Decode and test the input parameters */

    /* Parameter adjustments */
    --select;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --s;
    --dif;
    --work;
    --iwork;

    /* Function Body */
    wantbh = lsame_(job, "B");
    wants = lsame_(job, "E") || wantbh;
    wantdf = lsame_(job, "V") || wantbh;

    somcon = lsame_(howmny, "S");

    *info = 0;
    lquery = *lwork == -1;

    if (! wants && ! wantdf) {
	*info = -1;
    } else if (! lsame_(howmny, "A") && ! somcon) {
	*info = -2;
    } else if (*n < 0) {
	*info = -4;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -8;
    } else if (wants && *ldvl < *n) {
	*info = -10;
    } else if (wants && *ldvr < *n) {
	*info = -12;
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
			if (a[k + 1 + k * a_dim1] == 0.f) {
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

	if (*n == 0) {
	    lwmin = 1;
	} else if (lsame_(job, "V") || lsame_(job, 
		"B")) {
	    lwmin = (*n << 1) * (*n + 2) + 16;
	} else {
	    lwmin = *n;
	}
	work[1] = (real) lwmin;

	if (*mm < *m) {
	    *info = -15;
	} else if (*lwork < lwmin && ! lquery) {
	    *info = -18;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("STGSNA", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Get machine constants */

    eps = slamch_("P");
    smlnum = slamch_("S") / eps;
    ks = 0;
    pair = FALSE_;

    i__1 = *n;
    for (k = 1; k <= i__1; ++k) {

/*        Determine whether A(k,k) begins a 1-by-1 or 2-by-2 block. */

	if (pair) {
	    pair = FALSE_;
	    goto L20;
	} else {
	    if (k < *n) {
		pair = a[k + 1 + k * a_dim1] != 0.f;
	    }
	}

/*        Determine whether condition numbers are required for the k-th */
/*        eigenpair. */

	if (somcon) {
	    if (pair) {
		if (! select[k] && ! select[k + 1]) {
		    goto L20;
		}
	    } else {
		if (! select[k]) {
		    goto L20;
		}
	    }
	}

	++ks;

	if (wants) {

/*           Compute the reciprocal condition number of the k-th */
/*           eigenvalue. */

	    if (pair) {

/*              Complex eigenvalue pair. */

		r__1 = snrm2_(n, &vr[ks * vr_dim1 + 1], &c__1);
		r__2 = snrm2_(n, &vr[(ks + 1) * vr_dim1 + 1], &c__1);
		rnrm = slapy2_(&r__1, &r__2);
		r__1 = snrm2_(n, &vl[ks * vl_dim1 + 1], &c__1);
		r__2 = snrm2_(n, &vl[(ks + 1) * vl_dim1 + 1], &c__1);
		lnrm = slapy2_(&r__1, &r__2);
		sgemv_("N", n, n, &c_b19, &a[a_offset], lda, &vr[ks * vr_dim1 
			+ 1], &c__1, &c_b21, &work[1], &c__1);
		tmprr = sdot_(n, &work[1], &c__1, &vl[ks * vl_dim1 + 1], &
			c__1);
		tmpri = sdot_(n, &work[1], &c__1, &vl[(ks + 1) * vl_dim1 + 1],
			 &c__1);
		sgemv_("N", n, n, &c_b19, &a[a_offset], lda, &vr[(ks + 1) * 
			vr_dim1 + 1], &c__1, &c_b21, &work[1], &c__1);
		tmpii = sdot_(n, &work[1], &c__1, &vl[(ks + 1) * vl_dim1 + 1],
			 &c__1);
		tmpir = sdot_(n, &work[1], &c__1, &vl[ks * vl_dim1 + 1], &
			c__1);
		uhav = tmprr + tmpii;
		uhavi = tmpir - tmpri;
		sgemv_("N", n, n, &c_b19, &b[b_offset], ldb, &vr[ks * vr_dim1 
			+ 1], &c__1, &c_b21, &work[1], &c__1);
		tmprr = sdot_(n, &work[1], &c__1, &vl[ks * vl_dim1 + 1], &
			c__1);
		tmpri = sdot_(n, &work[1], &c__1, &vl[(ks + 1) * vl_dim1 + 1],
			 &c__1);
		sgemv_("N", n, n, &c_b19, &b[b_offset], ldb, &vr[(ks + 1) * 
			vr_dim1 + 1], &c__1, &c_b21, &work[1], &c__1);
		tmpii = sdot_(n, &work[1], &c__1, &vl[(ks + 1) * vl_dim1 + 1],
			 &c__1);
		tmpir = sdot_(n, &work[1], &c__1, &vl[ks * vl_dim1 + 1], &
			c__1);
		uhbv = tmprr + tmpii;
		uhbvi = tmpir - tmpri;
		uhav = slapy2_(&uhav, &uhavi);
		uhbv = slapy2_(&uhbv, &uhbvi);
		cond = slapy2_(&uhav, &uhbv);
		s[ks] = cond / (rnrm * lnrm);
		s[ks + 1] = s[ks];

	    } else {

/*              Real eigenvalue. */

		rnrm = snrm2_(n, &vr[ks * vr_dim1 + 1], &c__1);
		lnrm = snrm2_(n, &vl[ks * vl_dim1 + 1], &c__1);
		sgemv_("N", n, n, &c_b19, &a[a_offset], lda, &vr[ks * vr_dim1 
			+ 1], &c__1, &c_b21, &work[1], &c__1);
		uhav = sdot_(n, &work[1], &c__1, &vl[ks * vl_dim1 + 1], &c__1)
			;
		sgemv_("N", n, n, &c_b19, &b[b_offset], ldb, &vr[ks * vr_dim1 
			+ 1], &c__1, &c_b21, &work[1], &c__1);
		uhbv = sdot_(n, &work[1], &c__1, &vl[ks * vl_dim1 + 1], &c__1)
			;
		cond = slapy2_(&uhav, &uhbv);
		if (cond == 0.f) {
		    s[ks] = -1.f;
		} else {
		    s[ks] = cond / (rnrm * lnrm);
		}
	    }
	}

	if (wantdf) {
	    if (*n == 1) {
		dif[ks] = slapy2_(&a[a_dim1 + 1], &b[b_dim1 + 1]);
		goto L20;
	    }

/*           Estimate the reciprocal condition number of the k-th */
/*           eigenvectors. */
	    if (pair) {

/*              Copy the  2-by 2 pencil beginning at (A(k,k), B(k, k)). */
/*              Compute the eigenvalue(s) at position K. */

		work[1] = a[k + k * a_dim1];
		work[2] = a[k + 1 + k * a_dim1];
		work[3] = a[k + (k + 1) * a_dim1];
		work[4] = a[k + 1 + (k + 1) * a_dim1];
		work[5] = b[k + k * b_dim1];
		work[6] = b[k + 1 + k * b_dim1];
		work[7] = b[k + (k + 1) * b_dim1];
		work[8] = b[k + 1 + (k + 1) * b_dim1];
		r__1 = smlnum * eps;
		slag2_(&work[1], &c__2, &work[5], &c__2, &r__1, &beta, dummy1,
			 &alphar, dummy, &alphai);
		alprqt = 1.f;
		c1 = (alphar * alphar + alphai * alphai + beta * beta) * 2.f;
		c2 = beta * 4.f * beta * alphai * alphai;
		root1 = c1 + sqrt(c1 * c1 - c2 * 4.f);
		root2 = c2 / root1;
		root1 /= 2.f;
/* Computing MIN */
		r__1 = sqrt(root1), r__2 = sqrt(root2);
		cond = f2cmin(r__1,r__2);
	    }

/*           Copy the matrix (A, B) to the array WORK and swap the */
/*           diagonal block beginning at A(k,k) to the (1,1) position. */

	    slacpy_("Full", n, n, &a[a_offset], lda, &work[1], n);
	    slacpy_("Full", n, n, &b[b_offset], ldb, &work[*n * *n + 1], n);
	    ifst = k;
	    ilst = 1;

	    i__2 = *lwork - (*n << 1) * *n;
	    stgexc_(&c_false, &c_false, n, &work[1], n, &work[*n * *n + 1], n,
		     dummy, &c__1, dummy1, &c__1, &ifst, &ilst, &work[(*n * *
		    n << 1) + 1], &i__2, &ierr);

	    if (ierr > 0) {

/*              Ill-conditioned problem - swap rejected. */

		dif[ks] = 0.f;
	    } else {

/*              Reordering successful, solve generalized Sylvester */
/*              equation for R and L, */
/*                         A22 * R - L * A11 = A12 */
/*                         B22 * R - L * B11 = B12, */
/*              and compute estimate of Difl((A11,B11), (A22, B22)). */

		n1 = 1;
		if (work[2] != 0.f) {
		    n1 = 2;
		}
		n2 = *n - n1;
		if (n2 == 0) {
		    dif[ks] = cond;
		} else {
		    i__ = *n * *n + 1;
		    iz = (*n << 1) * *n + 1;
		    i__2 = *lwork - (*n << 1) * *n;
		    stgsyl_("N", &c__3, &n2, &n1, &work[*n * n1 + n1 + 1], n, 
			    &work[1], n, &work[n1 + 1], n, &work[*n * n1 + n1 
			    + i__], n, &work[i__], n, &work[n1 + i__], n, &
			    scale, &dif[ks], &work[iz + 1], &i__2, &iwork[1], 
			    &ierr);

		    if (pair) {
/* Computing MIN */
			r__1 = f2cmax(1.f,alprqt) * dif[ks];
			dif[ks] = f2cmin(r__1,cond);
		    }
		}
	    }
	    if (pair) {
		dif[ks + 1] = dif[ks];
	    }
	}
	if (pair) {
	    ++ks;
	}

L20:
	;
    }
    work[1] = (real) lwmin;
    return;

/*     End of STGSNA */

} /* stgsna_ */


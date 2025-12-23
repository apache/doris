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

static doublecomplex c_b1 = {0.,0.};
static doublecomplex c_b2 = {1.,0.};
static integer c__6 = 6;
static integer c__0 = 0;
static integer c__2 = 2;
static integer c_n1 = -1;
static integer c__1 = 1;

/* > \brief <b> ZGESVD computes the singular value decomposition (SVD) for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZGESVD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgesvd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgesvd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgesvd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZGESVD( JOBU, JOBVT, M, N, A, LDA, S, U, LDU, VT, LDVT, */
/*                          WORK, LWORK, RWORK, INFO ) */

/*       CHARACTER          JOBU, JOBVT */
/*       INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N */
/*       DOUBLE PRECISION   RWORK( * ), S( * ) */
/*       COMPLEX*16         A( LDA, * ), U( LDU, * ), VT( LDVT, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZGESVD computes the singular value decomposition (SVD) of a complex */
/* > M-by-N matrix A, optionally computing the left and/or right singular */
/* > vectors. The SVD is written */
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
/* > Note that the routine returns V**H, not V. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >          Specifies options for computing all or part of the matrix U: */
/* >          = 'A':  all M columns of U are returned in array U: */
/* >          = 'S':  the first f2cmin(m,n) columns of U (the left singular */
/* >                  vectors) are returned in the array U; */
/* >          = 'O':  the first f2cmin(m,n) columns of U (the left singular */
/* >                  vectors) are overwritten on the array A; */
/* >          = 'N':  no columns of U (no left singular vectors) are */
/* >                  computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVT */
/* > \verbatim */
/* >          JOBVT is CHARACTER*1 */
/* >          Specifies options for computing all or part of the matrix */
/* >          V**H: */
/* >          = 'A':  all N rows of V**H are returned in the array VT; */
/* >          = 'S':  the first f2cmin(m,n) rows of V**H (the right singular */
/* >                  vectors) are returned in the array VT; */
/* >          = 'O':  the first f2cmin(m,n) rows of V**H (the right singular */
/* >                  vectors) are overwritten on the array A; */
/* >          = 'N':  no rows of V**H (no right singular vectors) are */
/* >                  computed. */
/* > */
/* >          JOBVT and JOBU cannot both be 'O'. */
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
/* >          A is COMPLEX*16 array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, */
/* >          if JOBU = 'O',  A is overwritten with the first f2cmin(m,n) */
/* >                          columns of U (the left singular vectors, */
/* >                          stored columnwise); */
/* >          if JOBVT = 'O', A is overwritten with the first f2cmin(m,n) */
/* >                          rows of V**H (the right singular vectors, */
/* >                          stored rowwise); */
/* >          if JOBU .ne. 'O' and JOBVT .ne. 'O', the contents of A */
/* >                          are destroyed. */
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
/* >          S is DOUBLE PRECISION array, dimension (f2cmin(M,N)) */
/* >          The singular values of A, sorted so that S(i) >= S(i+1). */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is COMPLEX*16 array, dimension (LDU,UCOL) */
/* >          (LDU,M) if JOBU = 'A' or (LDU,f2cmin(M,N)) if JOBU = 'S'. */
/* >          If JOBU = 'A', U contains the M-by-M unitary matrix U; */
/* >          if JOBU = 'S', U contains the first f2cmin(m,n) columns of U */
/* >          (the left singular vectors, stored columnwise); */
/* >          if JOBU = 'N' or 'O', U is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >          The leading dimension of the array U.  LDU >= 1; if */
/* >          JOBU = 'S' or 'A', LDU >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] VT */
/* > \verbatim */
/* >          VT is COMPLEX*16 array, dimension (LDVT,N) */
/* >          If JOBVT = 'A', VT contains the N-by-N unitary matrix */
/* >          V**H; */
/* >          if JOBVT = 'S', VT contains the first f2cmin(m,n) rows of */
/* >          V**H (the right singular vectors, stored rowwise); */
/* >          if JOBVT = 'N' or 'O', VT is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVT */
/* > \verbatim */
/* >          LDVT is INTEGER */
/* >          The leading dimension of the array VT.  LDVT >= 1; if */
/* >          JOBVT = 'A', LDVT >= N; if JOBVT = 'S', LDVT >= f2cmin(M,N). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. */
/* >          LWORK >=  MAX(1,2*MIN(M,N)+MAX(M,N)). */
/* >          For good performance, LWORK should generally be larger. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (5*f2cmin(M,N)) */
/* >          On exit, if INFO > 0, RWORK(1:MIN(M,N)-1) contains the */
/* >          unconverged superdiagonal elements of an upper bidiagonal */
/* >          matrix B whose diagonal is in S (not necessarily sorted). */
/* >          B satisfies A = U * B * VT, so it has the same singular */
/* >          values as A, and singular vectors related by U and VT. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  if ZBDSQR did not converge, INFO specifies how many */
/* >                superdiagonals of an intermediate bidiagonal form B */
/* >                did not converge to zero. See the description of RWORK */
/* >                above for details. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date April 2012 */

/* > \ingroup complex16GEsing */

/*  ===================================================================== */
/* Subroutine */ void zgesvd_(char *jobu, char *jobvt, integer *m, integer *n, 
	doublecomplex *a, integer *lda, doublereal *s, doublecomplex *u, 
	integer *ldu, doublecomplex *vt, integer *ldvt, doublecomplex *work, 
	integer *lwork, doublereal *rwork, integer *info)
{
    /* System generated locals */
    address a__1[2];
    integer a_dim1, a_offset, u_dim1, u_offset, vt_dim1, vt_offset, i__1[2], 
	    i__2, i__3, i__4;
    char ch__1[2];

    /* Local variables */
    doublecomplex cdum[1];
    integer iscl;
    doublereal anrm;
    integer ierr, itau, ncvt, nrvt, lwork_zgebrd__, lwork_zgelqf__, i__, 
	    lwork_zgeqrf__;
    extern logical lsame_(char *, char *);
    integer chunk, minmn;
    extern /* Subroutine */ void zgemm_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *);
    integer wrkbl, itaup, itauq, mnthr, iwork;
    logical wntua, wntva, wntun, wntuo, wntvn, wntvo, wntus, wntvs;
    integer ie;
    extern doublereal dlamch_(char *);
    integer ir, iu;
    extern /* Subroutine */ void dlascl_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, integer *, doublereal *, 
	    integer *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern void zgebrd_(integer *, integer *, doublecomplex *, integer *, 
	    doublereal *, doublereal *, doublecomplex *, doublecomplex *, 
	    doublecomplex *, integer *, integer *);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern doublereal zlange_(char *, integer *, integer *, doublecomplex *, 
	    integer *, doublereal *);
    doublereal bignum;
    extern /* Subroutine */ void zgelqf_(integer *, integer *, doublecomplex *,
	     integer *, doublecomplex *, doublecomplex *, integer *, integer *
	    ), zlascl_(char *, integer *, integer *, doublereal *, doublereal 
	    *, integer *, integer *, doublecomplex *, integer *, integer *), zgeqrf_(integer *, integer *, doublecomplex *, integer *,
	     doublecomplex *, doublecomplex *, integer *, integer *), zlacpy_(
	    char *, integer *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *), zlaset_(char *, integer *, 
	    integer *, doublecomplex *, doublecomplex *, doublecomplex *, 
	    integer *);
    integer ldwrkr;
    extern /* Subroutine */ void zbdsqr_(char *, integer *, integer *, integer 
	    *, integer *, doublereal *, doublereal *, doublecomplex *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, integer *,
	     doublereal *, integer *);
    integer minwrk, ldwrku, maxwrk;
    extern /* Subroutine */ void zungbr_(char *, integer *, integer *, integer 
	    *, doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *);
    doublereal smlnum;
    integer irwork;
    extern /* Subroutine */ void zunmbr_(char *, char *, char *, integer *, 
	    integer *, integer *, doublecomplex *, integer *, doublecomplex *,
	     doublecomplex *, integer *, doublecomplex *, integer *, integer *
	    ), zunglq_(integer *, integer *, integer *
	    , doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *);
    logical lquery, wntuas, wntvas;
    extern /* Subroutine */ void zungqr_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *);
    integer blk, lwork_zungbr_p__, lwork_zungbr_q__, ncu;
    doublereal dum[1];
    integer lwork_zunglq_m__, lwork_zunglq_n__;
    doublereal eps;
    integer lwork_zungqr_m__, lwork_zungqr_n__, nru;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     April 2012 */


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

    /* Function Body */
    *info = 0;
    minmn = f2cmin(*m,*n);
    wntua = lsame_(jobu, "A");
    wntus = lsame_(jobu, "S");
    wntuas = wntua || wntus;
    wntuo = lsame_(jobu, "O");
    wntun = lsame_(jobu, "N");
    wntva = lsame_(jobvt, "A");
    wntvs = lsame_(jobvt, "S");
    wntvas = wntva || wntvs;
    wntvo = lsame_(jobvt, "O");
    wntvn = lsame_(jobvt, "N");
    lquery = *lwork == -1;

    if (! (wntua || wntus || wntuo || wntun)) {
	*info = -1;
    } else if (! (wntva || wntvs || wntvo || wntvn) || wntvo && wntuo) {
	*info = -2;
    } else if (*m < 0) {
	*info = -3;
    } else if (*n < 0) {
	*info = -4;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -6;
    } else if (*ldu < 1 || wntuas && *ldu < *m) {
	*info = -9;
    } else if (*ldvt < 1 || wntva && *ldvt < *n || wntvs && *ldvt < minmn) {
	*info = -11;
    }

/*     Compute workspace */
/*      (Note: Comments in the code beginning "Workspace:" describe the */
/*       minimal amount of workspace needed at that point in the code, */
/*       as well as the preferred amount for good performance. */
/*       CWorkspace refers to complex workspace, and RWorkspace to */
/*       real workspace. NB refers to the optimal block size for the */
/*       immediately following subroutine, as returned by ILAENV.) */

    if (*info == 0) {
	minwrk = 1;
	maxwrk = 1;
	if (*m >= *n && minmn > 0) {

/*           Space needed for ZBDSQR is BDSPAC = 5*N */

/* Writing concatenation */
	    i__1[0] = 1, a__1[0] = jobu;
	    i__1[1] = 1, a__1[1] = jobvt;
	    s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
	    mnthr = ilaenv_(&c__6, "ZGESVD", ch__1, m, n, &c__0, &c__0, (
		    ftnlen)6, (ftnlen)2);
/*           Compute space needed for ZGEQRF */
	    zgeqrf_(m, n, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zgeqrf__ = (integer) cdum[0].r;
/*           Compute space needed for ZUNGQR */
	    zungqr_(m, n, n, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zungqr_n__ = (integer) cdum[0].r;
	    zungqr_(m, m, n, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zungqr_m__ = (integer) cdum[0].r;
/*           Compute space needed for ZGEBRD */
	    zgebrd_(n, n, &a[a_offset], lda, &s[1], dum, cdum, cdum, cdum, &
		    c_n1, &ierr);
	    lwork_zgebrd__ = (integer) cdum[0].r;
/*           Compute space needed for ZUNGBR */
	    zungbr_("P", n, n, n, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zungbr_p__ = (integer) cdum[0].r;
	    zungbr_("Q", n, n, n, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zungbr_q__ = (integer) cdum[0].r;

	    if (*m >= mnthr) {
		if (wntun) {

/*                 Path 1 (M much larger than N, JOBU='N') */

		    maxwrk = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*n << 1) + lwork_zgebrd__;
		    maxwrk = f2cmax(i__2,i__3);
		    if (wntvo || wntvas) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = (*n << 1) + lwork_zungbr_p__;
			maxwrk = f2cmax(i__2,i__3);
		    }
		    minwrk = *n * 3;
		} else if (wntuo && wntvn) {

/*                 Path 2 (M much larger than N, JOBU='O', JOBVT='N') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *n * *n + wrkbl, i__3 = *n * *n + *m * *n;
		    maxwrk = f2cmax(i__2,i__3);
		    minwrk = (*n << 1) + *m;
		} else if (wntuo && wntvas) {

/*                 Path 3 (M much larger than N, JOBU='O', JOBVT='S' or */
/*                 'A') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *n * *n + wrkbl, i__3 = *n * *n + *m * *n;
		    maxwrk = f2cmax(i__2,i__3);
		    minwrk = (*n << 1) + *m;
		} else if (wntus && wntvn) {

/*                 Path 4 (M much larger than N, JOBU='S', JOBVT='N') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *n * *n + wrkbl;
		    minwrk = (*n << 1) + *m;
		} else if (wntus && wntvo) {

/*                 Path 5 (M much larger than N, JOBU='S', JOBVT='O') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = (*n << 1) * *n + wrkbl;
		    minwrk = (*n << 1) + *m;
		} else if (wntus && wntvas) {

/*                 Path 6 (M much larger than N, JOBU='S', JOBVT='S' or */
/*                 'A') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *n * *n + wrkbl;
		    minwrk = (*n << 1) + *m;
		} else if (wntua && wntvn) {

/*                 Path 7 (M much larger than N, JOBU='A', JOBVT='N') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *n * *n + wrkbl;
		    minwrk = (*n << 1) + *m;
		} else if (wntua && wntvo) {

/*                 Path 8 (M much larger than N, JOBU='A', JOBVT='O') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = (*n << 1) * *n + wrkbl;
		    minwrk = (*n << 1) + *m;
		} else if (wntua && wntvas) {

/*                 Path 9 (M much larger than N, JOBU='A', JOBVT='S' or */
/*                 'A') */

		    wrkbl = *n + lwork_zgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_zungqr_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*n << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *n * *n + wrkbl;
		    minwrk = (*n << 1) + *m;
		}
	    } else {

/*              Path 10 (M at least N, but not much larger) */

		zgebrd_(m, n, &a[a_offset], lda, &s[1], dum, cdum, cdum, cdum,
			 &c_n1, &ierr);
		lwork_zgebrd__ = (integer) cdum[0].r;
		maxwrk = (*n << 1) + lwork_zgebrd__;
		if (wntus || wntuo) {
		    zungbr_("Q", m, n, n, &a[a_offset], lda, cdum, cdum, &
			    c_n1, &ierr);
		    lwork_zungbr_q__ = (integer) cdum[0].r;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*n << 1) + lwork_zungbr_q__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (wntua) {
		    zungbr_("Q", m, m, n, &a[a_offset], lda, cdum, cdum, &
			    c_n1, &ierr);
		    lwork_zungbr_q__ = (integer) cdum[0].r;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*n << 1) + lwork_zungbr_q__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (! wntvn) {
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*n << 1) + lwork_zungbr_p__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		minwrk = (*n << 1) + *m;
	    }
	} else if (minmn > 0) {

/*           Space needed for ZBDSQR is BDSPAC = 5*M */

/* Writing concatenation */
	    i__1[0] = 1, a__1[0] = jobu;
	    i__1[1] = 1, a__1[1] = jobvt;
	    s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
	    mnthr = ilaenv_(&c__6, "ZGESVD", ch__1, m, n, &c__0, &c__0, (
		    ftnlen)6, (ftnlen)2);
/*           Compute space needed for ZGELQF */
	    zgelqf_(m, n, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zgelqf__ = (integer) cdum[0].r;
/*           Compute space needed for ZUNGLQ */
	    zunglq_(n, n, m, cdum, n, cdum, cdum, &c_n1, &ierr);
	    lwork_zunglq_n__ = (integer) cdum[0].r;
	    zunglq_(m, n, m, &a[a_offset], lda, cdum, cdum, &c_n1, &ierr);
	    lwork_zunglq_m__ = (integer) cdum[0].r;
/*           Compute space needed for ZGEBRD */
	    zgebrd_(m, m, &a[a_offset], lda, &s[1], dum, cdum, cdum, cdum, &
		    c_n1, &ierr);
	    lwork_zgebrd__ = (integer) cdum[0].r;
/*            Compute space needed for ZUNGBR P */
	    zungbr_("P", m, m, m, &a[a_offset], n, cdum, cdum, &c_n1, &ierr);
	    lwork_zungbr_p__ = (integer) cdum[0].r;
/*           Compute space needed for ZUNGBR Q */
	    zungbr_("Q", m, m, m, &a[a_offset], n, cdum, cdum, &c_n1, &ierr);
	    lwork_zungbr_q__ = (integer) cdum[0].r;
	    if (*n >= mnthr) {
		if (wntvn) {

/*                 Path 1t(N much larger than M, JOBVT='N') */

		    maxwrk = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*m << 1) + lwork_zgebrd__;
		    maxwrk = f2cmax(i__2,i__3);
		    if (wntuo || wntuas) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = (*m << 1) + lwork_zungbr_q__;
			maxwrk = f2cmax(i__2,i__3);
		    }
		    minwrk = *m * 3;
		} else if (wntvo && wntun) {

/*                 Path 2t(N much larger than M, JOBU='N', JOBVT='O') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *m * *m + wrkbl, i__3 = *m * *m + *m * *n;
		    maxwrk = f2cmax(i__2,i__3);
		    minwrk = (*m << 1) + *n;
		} else if (wntvo && wntuas) {

/*                 Path 3t(N much larger than M, JOBU='S' or 'A', */
/*                 JOBVT='O') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *m * *m + wrkbl, i__3 = *m * *m + *m * *n;
		    maxwrk = f2cmax(i__2,i__3);
		    minwrk = (*m << 1) + *n;
		} else if (wntvs && wntun) {

/*                 Path 4t(N much larger than M, JOBU='N', JOBVT='S') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *m * *m + wrkbl;
		    minwrk = (*m << 1) + *n;
		} else if (wntvs && wntuo) {

/*                 Path 5t(N much larger than M, JOBU='O', JOBVT='S') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = (*m << 1) * *m + wrkbl;
		    minwrk = (*m << 1) + *n;
		} else if (wntvs && wntuas) {

/*                 Path 6t(N much larger than M, JOBU='S' or 'A', */
/*                 JOBVT='S') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *m * *m + wrkbl;
		    minwrk = (*m << 1) + *n;
		} else if (wntva && wntun) {

/*                 Path 7t(N much larger than M, JOBU='N', JOBVT='A') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *m * *m + wrkbl;
		    minwrk = (*m << 1) + *n;
		} else if (wntva && wntuo) {

/*                 Path 8t(N much larger than M, JOBU='O', JOBVT='A') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = (*m << 1) * *m + wrkbl;
		    minwrk = (*m << 1) + *n;
		} else if (wntva && wntuas) {

/*                 Path 9t(N much larger than M, JOBU='S' or 'A', */
/*                 JOBVT='A') */

		    wrkbl = *m + lwork_zgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_zunglq_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = (*m << 1) + lwork_zungbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    maxwrk = *m * *m + wrkbl;
		    minwrk = (*m << 1) + *n;
		}
	    } else {

/*              Path 10t(N greater than M, but not much larger) */

		zgebrd_(m, n, &a[a_offset], lda, &s[1], dum, cdum, cdum, cdum,
			 &c_n1, &ierr);
		lwork_zgebrd__ = (integer) cdum[0].r;
		maxwrk = (*m << 1) + lwork_zgebrd__;
		if (wntvs || wntvo) {
/*                Compute space needed for ZUNGBR P */
		    zungbr_("P", m, n, m, &a[a_offset], n, cdum, cdum, &c_n1, 
			    &ierr);
		    lwork_zungbr_p__ = (integer) cdum[0].r;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*m << 1) + lwork_zungbr_p__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (wntva) {
		    zungbr_("P", n, n, m, &a[a_offset], n, cdum, cdum, &c_n1, 
			    &ierr);
		    lwork_zungbr_p__ = (integer) cdum[0].r;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*m << 1) + lwork_zungbr_p__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (! wntun) {
/* Computing MAX */
		    i__2 = maxwrk, i__3 = (*m << 1) + lwork_zungbr_q__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		minwrk = (*m << 1) + *n;
	    }
	}
	maxwrk = f2cmax(maxwrk,minwrk);
	work[1].r = (doublereal) maxwrk, work[1].i = 0.;

	if (*lwork < minwrk && ! lquery) {
	    *info = -13;
	}
    }

    if (*info != 0) {
	i__2 = -(*info);
	xerbla_("ZGESVD", &i__2, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Get machine constants */

    eps = dlamch_("P");
    smlnum = sqrt(dlamch_("S")) / eps;
    bignum = 1. / smlnum;

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = zlange_("M", m, n, &a[a_offset], lda, dum);
    iscl = 0;
    if (anrm > 0. && anrm < smlnum) {
	iscl = 1;
	zlascl_("G", &c__0, &c__0, &anrm, &smlnum, m, n, &a[a_offset], lda, &
		ierr);
    } else if (anrm > bignum) {
	iscl = 1;
	zlascl_("G", &c__0, &c__0, &anrm, &bignum, m, n, &a[a_offset], lda, &
		ierr);
    }

    if (*m >= *n) {

/*        A has at least as many rows as columns. If A has sufficiently */
/*        more rows than columns, first reduce using the QR */
/*        decomposition (if sufficient workspace available) */

	if (*m >= mnthr) {

	    if (wntun) {

/*              Path 1 (M much larger than N, JOBU='N') */
/*              No left singular vectors to be computed */

		itau = 1;
		iwork = itau + *n;

/*              Compute A=Q*R */
/*              (CWorkspace: need 2*N, prefer N+N*NB) */
/*              (RWorkspace: need 0) */

		i__2 = *lwork - iwork + 1;
		zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &
			i__2, &ierr);

/*              Zero out below R */

		if (*n > 1) {
		    i__2 = *n - 1;
		    i__3 = *n - 1;
		    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &a[a_dim1 + 2], 
			    lda);
		}
		ie = 1;
		itauq = 1;
		itaup = itauq + *n;
		iwork = itaup + *n;

/*              Bidiagonalize R in A */
/*              (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*              (RWorkspace: need N) */

		i__2 = *lwork - iwork + 1;
		zgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[iwork], &i__2, &ierr);
		ncvt = 0;
		if (wntvo || wntvas) {

/*                 If right singular vectors desired, generate P'. */
/*                 (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungbr_("P", n, n, n, &a[a_offset], lda, &work[itaup], &
			    work[iwork], &i__2, &ierr);
		    ncvt = *n;
		}
		irwork = ie + *n;

/*              Perform bidiagonal QR iteration, computing right */
/*              singular vectors of A in A if desired */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("U", n, &ncvt, &c__0, &c__0, &s[1], &rwork[ie], &a[
			a_offset], lda, cdum, &c__1, cdum, &c__1, &rwork[
			irwork], info);

/*              If right singular vectors desired in VT, copy them there */

		if (wntvas) {
		    zlacpy_("F", n, n, &a[a_offset], lda, &vt[vt_offset], 
			    ldvt);
		}

	    } else if (wntuo && wntvn) {

/*              Path 2 (M much larger than N, JOBU='O', JOBVT='N') */
/*              N left singular vectors to be overwritten on A and */
/*              no right singular vectors to be computed */

		if (*lwork >= *n * *n + *n * 3) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *lda * *n;
		    if (*lwork >= f2cmax(i__2,i__3) + *lda * *n) {

/*                    WORK(IU) is LDA by N, WORK(IR) is LDA by N */

			ldwrku = *lda;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__2 = wrkbl, i__3 = *lda * *n;
			if (*lwork >= f2cmax(i__2,i__3) + *n * *n) {

/*                    WORK(IU) is LDA by N, WORK(IR) is N by N */

			    ldwrku = *lda;
			    ldwrkr = *n;
			} else {

/*                    WORK(IU) is LDWRKU by N, WORK(IR) is N by N */

			    ldwrku = (*lwork - *n * *n) / *n;
			    ldwrkr = *n;
			}
		    }
		    itau = ir + ldwrkr * *n;
		    iwork = itau + *n;

/*                 Compute A=Q*R */
/*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy R to WORK(IR) and zero out below it */

		    zlacpy_("U", n, n, &a[a_offset], lda, &work[ir], &ldwrkr);
		    i__2 = *n - 1;
		    i__3 = *n - 1;
		    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[ir + 1], &
			    ldwrkr);

/*                 Generate Q in A */
/*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = 1;
		    itauq = itau;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize R in WORK(IR) */
/*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB) */
/*                 (RWorkspace: need N) */

		    i__2 = *lwork - iwork + 1;
		    zgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &rwork[ie], &
			    work[itauq], &work[itaup], &work[iwork], &i__2, &
			    ierr);

/*                 Generate left vectors bidiagonalizing R */
/*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB) */
/*                 (RWorkspace: need 0) */

		    i__2 = *lwork - iwork + 1;
		    zungbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq], &
			    work[iwork], &i__2, &ierr);
		    irwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of R in WORK(IR) */
/*                 (CWorkspace: need N*N) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", n, &c__0, n, &c__0, &s[1], &rwork[ie], cdum, 
			    &c__1, &work[ir], &ldwrkr, cdum, &c__1, &rwork[
			    irwork], info);
		    iu = itauq;

/*                 Multiply Q in A by left singular vectors of R in */
/*                 WORK(IR), storing result in WORK(IU) and copying to A */
/*                 (CWorkspace: need N*N+N, prefer N*N+M*N) */
/*                 (RWorkspace: 0) */

		    i__2 = *m;
		    i__3 = ldwrku;
		    for (i__ = 1; i__3 < 0 ? i__ >= i__2 : i__ <= i__2; i__ +=
			     i__3) {
/* Computing MIN */
			i__4 = *m - i__ + 1;
			chunk = f2cmin(i__4,ldwrku);
			zgemm_("N", "N", &chunk, n, n, &c_b2, &a[i__ + a_dim1]
				, lda, &work[ir], &ldwrkr, &c_b1, &work[iu], &
				ldwrku);
			zlacpy_("F", &chunk, n, &work[iu], &ldwrku, &a[i__ + 
				a_dim1], lda);
/* L10: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    ie = 1;
		    itauq = 1;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize A */
/*                 (CWorkspace: need 2*N+M, prefer 2*N+(M+N)*NB) */
/*                 (RWorkspace: N) */

		    i__3 = *lwork - iwork + 1;
		    zgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__3, &ierr);

/*                 Generate left vectors bidiagonalizing A */
/*                 (CWorkspace: need 3*N, prefer 2*N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungbr_("Q", m, n, n, &a[a_offset], lda, &work[itauq], &
			    work[iwork], &i__3, &ierr);
		    irwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of A in A */
/*                 (CWorkspace: need 0) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", n, &c__0, m, &c__0, &s[1], &rwork[ie], cdum, 
			    &c__1, &a[a_offset], lda, cdum, &c__1, &rwork[
			    irwork], info);

		}

	    } else if (wntuo && wntvas) {

/*              Path 3 (M much larger than N, JOBU='O', JOBVT='S' or 'A') */
/*              N left singular vectors to be overwritten on A and */
/*              N right singular vectors to be computed in VT */

		if (*lwork >= *n * *n + *n * 3) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__3 = wrkbl, i__2 = *lda * *n;
		    if (*lwork >= f2cmax(i__3,i__2) + *lda * *n) {

/*                    WORK(IU) is LDA by N and WORK(IR) is LDA by N */

			ldwrku = *lda;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__3 = wrkbl, i__2 = *lda * *n;
			if (*lwork >= f2cmax(i__3,i__2) + *n * *n) {

/*                    WORK(IU) is LDA by N and WORK(IR) is N by N */

			    ldwrku = *lda;
			    ldwrkr = *n;
			} else {

/*                    WORK(IU) is LDWRKU by N and WORK(IR) is N by N */

			    ldwrku = (*lwork - *n * *n) / *n;
			    ldwrkr = *n;
			}
		    }
		    itau = ir + ldwrkr * *n;
		    iwork = itau + *n;

/*                 Compute A=Q*R */
/*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__3, &ierr);

/*                 Copy R to VT, zeroing out below it */

		    zlacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
			    ldvt);
		    if (*n > 1) {
			i__3 = *n - 1;
			i__2 = *n - 1;
			zlaset_("L", &i__3, &i__2, &c_b1, &c_b1, &vt[vt_dim1 
				+ 2], ldvt);
		    }

/*                 Generate Q in A */
/*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__3, &ierr);
		    ie = 1;
		    itauq = itau;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize R in VT, copying result to WORK(IR) */
/*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB) */
/*                 (RWorkspace: need N) */

		    i__3 = *lwork - iwork + 1;
		    zgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &rwork[ie], &
			    work[itauq], &work[itaup], &work[iwork], &i__3, &
			    ierr);
		    zlacpy_("L", n, n, &vt[vt_offset], ldvt, &work[ir], &
			    ldwrkr);

/*                 Generate left vectors bidiagonalizing R in WORK(IR) */
/*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq], &
			    work[iwork], &i__3, &ierr);

/*                 Generate right vectors bidiagonalizing R in VT */
/*                 (CWorkspace: need N*N+3*N-1, prefer N*N+2*N+(N-1)*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], 
			    &work[iwork], &i__3, &ierr);
		    irwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of R in WORK(IR) and computing right */
/*                 singular vectors of R in VT */
/*                 (CWorkspace: need N*N) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", n, n, n, &c__0, &s[1], &rwork[ie], &vt[
			    vt_offset], ldvt, &work[ir], &ldwrkr, cdum, &c__1,
			     &rwork[irwork], info);
		    iu = itauq;

/*                 Multiply Q in A by left singular vectors of R in */
/*                 WORK(IR), storing result in WORK(IU) and copying to A */
/*                 (CWorkspace: need N*N+N, prefer N*N+M*N) */
/*                 (RWorkspace: 0) */

		    i__3 = *m;
		    i__2 = ldwrku;
		    for (i__ = 1; i__2 < 0 ? i__ >= i__3 : i__ <= i__3; i__ +=
			     i__2) {
/* Computing MIN */
			i__4 = *m - i__ + 1;
			chunk = f2cmin(i__4,ldwrku);
			zgemm_("N", "N", &chunk, n, n, &c_b2, &a[i__ + a_dim1]
				, lda, &work[ir], &ldwrkr, &c_b1, &work[iu], &
				ldwrku);
			zlacpy_("F", &chunk, n, &work[iu], &ldwrku, &a[i__ + 
				a_dim1], lda);
/* L20: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    itau = 1;
		    iwork = itau + *n;

/*                 Compute A=Q*R */
/*                 (CWorkspace: need 2*N, prefer N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy R to VT, zeroing out below it */

		    zlacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
			    ldvt);
		    if (*n > 1) {
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &vt[vt_dim1 
				+ 2], ldvt);
		    }

/*                 Generate Q in A */
/*                 (CWorkspace: need 2*N, prefer N+N*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = 1;
		    itauq = itau;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize R in VT */
/*                 (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                 (RWorkspace: N) */

		    i__2 = *lwork - iwork + 1;
		    zgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &rwork[ie], &
			    work[itauq], &work[itaup], &work[iwork], &i__2, &
			    ierr);

/*                 Multiply Q in A by left vectors bidiagonalizing R */
/*                 (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zunmbr_("Q", "R", "N", m, n, n, &vt[vt_offset], ldvt, &
			    work[itauq], &a[a_offset], lda, &work[iwork], &
			    i__2, &ierr);

/*                 Generate right vectors bidiagonalizing R in VT */
/*                 (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], 
			    &work[iwork], &i__2, &ierr);
		    irwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of A in A and computing right */
/*                 singular vectors of A in VT */
/*                 (CWorkspace: 0) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", n, n, m, &c__0, &s[1], &rwork[ie], &vt[
			    vt_offset], ldvt, &a[a_offset], lda, cdum, &c__1, 
			    &rwork[irwork], info);

		}

	    } else if (wntus) {

		if (wntvn) {

/*                 Path 4 (M much larger than N, JOBU='S', JOBVT='N') */
/*                 N left singular vectors to be computed in U and */
/*                 no right singular vectors to be computed */

		    if (*lwork >= *n * *n + *n * 3) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IR) is LDA by N */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is N by N */

			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R */
/*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy R to WORK(IR), zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[ir + 1]
				, &ldwrkr);

/*                    Generate Q in A */
/*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, n, n, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IR) */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate left vectors bidiagonalizing R in WORK(IR) */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IR) */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, &c__0, n, &c__0, &s[1], &rwork[ie], 
				cdum, &c__1, &work[ir], &ldwrkr, cdum, &c__1, 
				&rwork[irwork], info);

/*                    Multiply Q in A by left singular vectors of R in */
/*                    WORK(IR), storing result in U */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, n, &c_b2, &a[a_offset], lda, &
				work[ir], &ldwrkr, &c_b1, &u[u_offset], ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, n, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left vectors bidiagonalizing R */
/*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, &c__0, m, &c__0, &s[1], &rwork[ie], 
				cdum, &c__1, &u[u_offset], ldu, cdum, &c__1, &
				rwork[irwork], info);

		    }

		} else if (wntvo) {

/*                 Path 5 (M much larger than N, JOBU='S', JOBVT='O') */
/*                 N left singular vectors to be computed in U and */
/*                 N right singular vectors to be overwritten on A */

		    if (*lwork >= (*n << 1) * *n + *n * 3) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is LDA by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *n) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is N by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			} else {

/*                       WORK(IU) is N by N and WORK(IR) is N by N */

			    ldwrku = *n;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R */
/*                    (CWorkspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 1]
				, &ldwrku);

/*                    Generate Q in A */
/*                    (CWorkspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, n, n, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (CWorkspace: need   2*N*N+3*N, */
/*                                 prefer 2*N*N+2*N+2*N*NB) */
/*                    (RWorkspace: need   N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("U", n, n, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need 2*N*N+3*N, prefer 2*N*N+2*N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in WORK(IR) */
/*                    (CWorkspace: need   2*N*N+3*N-1, */
/*                                 prefer 2*N*N+2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in WORK(IR) */
/*                    (CWorkspace: need 2*N*N) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, n, &c__0, &s[1], &rwork[ie], &work[
				ir], &ldwrkr, &work[iu], &ldwrku, cdum, &c__1,
				 &rwork[irwork], info);

/*                    Multiply Q in A by left singular vectors of R in */
/*                    WORK(IU), storing result in U */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, n, &c_b2, &a[a_offset], lda, &
				work[iu], &ldwrku, &c_b1, &u[u_offset], ldu);

/*                    Copy right singular vectors of R to A */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zlacpy_("F", n, n, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, n, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left vectors bidiagonalizing R */
/*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;

/*                    Generate right vectors bidiagonalizing R in A */
/*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &a[a_offset], lda, &work[itaup],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in A */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, m, &c__0, &s[1], &rwork[ie], &a[
				a_offset], lda, &u[u_offset], ldu, cdum, &
				c__1, &rwork[irwork], info);

		    }

		} else if (wntvas) {

/*                 Path 6 (M much larger than N, JOBU='S', JOBVT='S' */
/*                         or 'A') */
/*                 N left singular vectors to be computed in U and */
/*                 N right singular vectors to be computed in VT */

		    if (*lwork >= *n * *n + *n * 3) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IU) is LDA by N */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is N by N */

			    ldwrku = *n;
			}
			itau = iu + ldwrku * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R */
/*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 1]
				, &ldwrku);

/*                    Generate Q in A */
/*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, n, n, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to VT */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("U", n, n, &work[iu], &ldwrku, &vt[vt_offset],
				 ldvt);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (CWorkspace: need   N*N+3*N-1, */
/*                                 prefer N*N+2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in VT */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, n, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &work[iu], &ldwrku, cdum, &
				c__1, &rwork[irwork], info);

/*                    Multiply Q in A by left singular vectors of R in */
/*                    WORK(IU), storing result in U */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, n, &c_b2, &a[a_offset], lda, &
				work[iu], &ldwrku, &c_b1, &u[u_offset], ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, n, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R to VT, zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);
			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &vt[
				    vt_dim1 + 2], ldvt);
			}
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in VT */
/*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &rwork[ie],
				 &work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in VT */
/*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("Q", "R", "N", m, n, n, &vt[vt_offset], ldvt, 
				&work[itauq], &u[u_offset], ldu, &work[iwork],
				 &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, m, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, cdum, &
				c__1, &rwork[irwork], info);

		    }

		}

	    } else if (wntua) {

		if (wntvn) {

/*                 Path 7 (M much larger than N, JOBU='A', JOBVT='N') */
/*                 M left singular vectors to be computed in U and */
/*                 no right singular vectors to be computed */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *n * 3;
		    if (*lwork >= *n * *n + f2cmax(i__2,i__3)) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IR) is LDA by N */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is N by N */

			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Copy R to WORK(IR), zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[ir + 1]
				, &ldwrkr);

/*                    Generate Q in U */
/*                    (CWorkspace: need N*N+N+M, prefer N*N+N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IR) */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in WORK(IR) */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IR) */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, &c__0, n, &c__0, &s[1], &rwork[ie], 
				cdum, &c__1, &work[ir], &ldwrkr, cdum, &c__1, 
				&rwork[irwork], info);

/*                    Multiply Q in U by left singular vectors of R in */
/*                    WORK(IR), storing result in A */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, n, &c_b2, &u[u_offset], ldu, &
				work[ir], &ldwrkr, &c_b1, &a[a_offset], lda);

/*                    Copy left singular vectors of A from A to U */

			zlacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need N+M, prefer N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in A */
/*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, &c__0, m, &c__0, &s[1], &rwork[ie], 
				cdum, &c__1, &u[u_offset], ldu, cdum, &c__1, &
				rwork[irwork], info);

		    }

		} else if (wntvo) {

/*                 Path 8 (M much larger than N, JOBU='A', JOBVT='O') */
/*                 M left singular vectors to be computed in U and */
/*                 N right singular vectors to be overwritten on A */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *n * 3;
		    if (*lwork >= (*n << 1) * *n + f2cmax(i__2,i__3)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is LDA by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *n) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is N by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			} else {

/*                       WORK(IU) is N by N and WORK(IR) is N by N */

			    ldwrku = *n;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need 2*N*N+N+M, prefer 2*N*N+N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 1]
				, &ldwrku);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (CWorkspace: need   2*N*N+3*N, */
/*                                 prefer 2*N*N+2*N+2*N*NB) */
/*                    (RWorkspace: need   N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("U", n, n, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need 2*N*N+3*N, prefer 2*N*N+2*N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in WORK(IR) */
/*                    (CWorkspace: need   2*N*N+3*N-1, */
/*                                 prefer 2*N*N+2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in WORK(IR) */
/*                    (CWorkspace: need 2*N*N) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, n, &c__0, &s[1], &rwork[ie], &work[
				ir], &ldwrkr, &work[iu], &ldwrku, cdum, &c__1,
				 &rwork[irwork], info);

/*                    Multiply Q in U by left singular vectors of R in */
/*                    WORK(IU), storing result in A */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, n, &c_b2, &u[u_offset], ldu, &
				work[iu], &ldwrku, &c_b1, &a[a_offset], lda);

/*                    Copy left singular vectors of A from A to U */

			zlacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Copy right singular vectors of R from WORK(IR) to A */

			zlacpy_("F", n, n, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need N+M, prefer N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in A */
/*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;

/*                    Generate right bidiagonalizing vectors in A */
/*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &a[a_offset], lda, &work[itaup],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in A */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, m, &c__0, &s[1], &rwork[ie], &a[
				a_offset], lda, &u[u_offset], ldu, cdum, &
				c__1, &rwork[irwork], info);

		    }

		} else if (wntvas) {

/*                 Path 9 (M much larger than N, JOBU='A', JOBVT='S' */
/*                         or 'A') */
/*                 M left singular vectors to be computed in U and */
/*                 N right singular vectors to be computed in VT */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *n * 3;
		    if (*lwork >= *n * *n + f2cmax(i__2,i__3)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IU) is LDA by N */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is N by N */

			    ldwrku = *n;
			}
			itau = iu + ldwrku * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need N*N+N+M, prefer N*N+N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 1]
				, &ldwrku);
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to VT */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("U", n, n, &work[iu], &ldwrku, &vt[vt_offset],
				 ldvt);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (CWorkspace: need   N*N+3*N-1, */
/*                                 prefer N*N+2*N+(N-1)*NB) */
/*                    (RWorkspace: need   0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in VT */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, n, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &work[iu], &ldwrku, cdum, &
				c__1, &rwork[irwork], info);

/*                    Multiply Q in U by left singular vectors of R in */
/*                    WORK(IU), storing result in A */
/*                    (CWorkspace: need N*N) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, n, &c_b2, &u[u_offset], ldu, &
				work[iu], &ldwrku, &c_b1, &a[a_offset], lda);

/*                    Copy left singular vectors of A from A to U */

			zlacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (CWorkspace: need 2*N, prefer N+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (CWorkspace: need N+M, prefer N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R from A to VT, zeroing out below it */

			zlacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);
			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &vt[
				    vt_dim1 + 2], ldvt);
			}
			ie = 1;
			itauq = itau;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in VT */
/*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB) */
/*                    (RWorkspace: need N) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &rwork[ie],
				 &work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in VT */
/*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("Q", "R", "N", m, n, n, &vt[vt_offset], ldvt, 
				&work[itauq], &u[u_offset], ldu, &work[iwork],
				 &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			irwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", n, n, m, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, cdum, &
				c__1, &rwork[irwork], info);

		    }

		}

	    }

	} else {

/*           M .LT. MNTHR */

/*           Path 10 (M at least N, but not much larger) */
/*           Reduce to bidiagonal form without QR decomposition */

	    ie = 1;
	    itauq = 1;
	    itaup = itauq + *n;
	    iwork = itaup + *n;

/*           Bidiagonalize A */
/*           (CWorkspace: need 2*N+M, prefer 2*N+(M+N)*NB) */
/*           (RWorkspace: need N) */

	    i__2 = *lwork - iwork + 1;
	    zgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[iwork], &i__2, &ierr);
	    if (wntuas) {

/*              If left singular vectors desired in U, copy result to U */
/*              and generate left bidiagonalizing vectors in U */
/*              (CWorkspace: need 2*N+NCU, prefer 2*N+NCU*NB) */
/*              (RWorkspace: 0) */

		zlacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], ldu);
		if (wntus) {
		    ncu = *n;
		}
		if (wntua) {
		    ncu = *m;
		}
		i__2 = *lwork - iwork + 1;
		zungbr_("Q", m, &ncu, n, &u[u_offset], ldu, &work[itauq], &
			work[iwork], &i__2, &ierr);
	    }
	    if (wntvas) {

/*              If right singular vectors desired in VT, copy result to */
/*              VT and generate right bidiagonalizing vectors in VT */
/*              (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*              (RWorkspace: 0) */

		zlacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__2 = *lwork - iwork + 1;
		zungbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], &
			work[iwork], &i__2, &ierr);
	    }
	    if (wntuo) {

/*              If left singular vectors desired in A, generate left */
/*              bidiagonalizing vectors in A */
/*              (CWorkspace: need 3*N, prefer 2*N+N*NB) */
/*              (RWorkspace: 0) */

		i__2 = *lwork - iwork + 1;
		zungbr_("Q", m, n, n, &a[a_offset], lda, &work[itauq], &work[
			iwork], &i__2, &ierr);
	    }
	    if (wntvo) {

/*              If right singular vectors desired in A, generate right */
/*              bidiagonalizing vectors in A */
/*              (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*              (RWorkspace: 0) */

		i__2 = *lwork - iwork + 1;
		zungbr_("P", n, n, n, &a[a_offset], lda, &work[itaup], &work[
			iwork], &i__2, &ierr);
	    }
	    irwork = ie + *n;
	    if (wntuas || wntuo) {
		nru = *m;
	    }
	    if (wntun) {
		nru = 0;
	    }
	    if (wntvas || wntvo) {
		ncvt = *n;
	    }
	    if (wntvn) {
		ncvt = 0;
	    }
	    if (! wntuo && ! wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in VT */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("U", n, &ncvt, &nru, &c__0, &s[1], &rwork[ie], &vt[
			vt_offset], ldvt, &u[u_offset], ldu, cdum, &c__1, &
			rwork[irwork], info);
	    } else if (! wntuo && wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in A */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("U", n, &ncvt, &nru, &c__0, &s[1], &rwork[ie], &a[
			a_offset], lda, &u[u_offset], ldu, cdum, &c__1, &
			rwork[irwork], info);
	    } else {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in A and computing right singular */
/*              vectors in VT */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("U", n, &ncvt, &nru, &c__0, &s[1], &rwork[ie], &vt[
			vt_offset], ldvt, &a[a_offset], lda, cdum, &c__1, &
			rwork[irwork], info);
	    }

	}

    } else {

/*        A has more columns than rows. If A has sufficiently more */
/*        columns than rows, first reduce using the LQ decomposition (if */
/*        sufficient workspace available) */

	if (*n >= mnthr) {

	    if (wntvn) {

/*              Path 1t(N much larger than M, JOBVT='N') */
/*              No right singular vectors to be computed */

		itau = 1;
		iwork = itau + *m;

/*              Compute A=L*Q */
/*              (CWorkspace: need 2*M, prefer M+M*NB) */
/*              (RWorkspace: 0) */

		i__2 = *lwork - iwork + 1;
		zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &
			i__2, &ierr);

/*              Zero out above L */

		i__2 = *m - 1;
		i__3 = *m - 1;
		zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &a[(a_dim1 << 1) + 1]
			, lda);
		ie = 1;
		itauq = 1;
		itaup = itauq + *m;
		iwork = itaup + *m;

/*              Bidiagonalize L in A */
/*              (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*              (RWorkspace: need M) */

		i__2 = *lwork - iwork + 1;
		zgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			itauq], &work[itaup], &work[iwork], &i__2, &ierr);
		if (wntuo || wntuas) {

/*                 If left singular vectors desired, generate Q */
/*                 (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungbr_("Q", m, m, m, &a[a_offset], lda, &work[itauq], &
			    work[iwork], &i__2, &ierr);
		}
		irwork = ie + *m;
		nru = 0;
		if (wntuo || wntuas) {
		    nru = *m;
		}

/*              Perform bidiagonal QR iteration, computing left singular */
/*              vectors of A in A if desired */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("U", m, &c__0, &nru, &c__0, &s[1], &rwork[ie], cdum, &
			c__1, &a[a_offset], lda, cdum, &c__1, &rwork[irwork], 
			info);

/*              If left singular vectors desired in U, copy them there */

		if (wntuas) {
		    zlacpy_("F", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		}

	    } else if (wntvo && wntun) {

/*              Path 2t(N much larger than M, JOBU='N', JOBVT='O') */
/*              M right singular vectors to be overwritten on A and */
/*              no left singular vectors to be computed */

		if (*lwork >= *m * *m + *m * 3) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *lda * *n;
		    if (*lwork >= f2cmax(i__2,i__3) + *lda * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is LDA by M */

			ldwrku = *lda;
			chunk = *n;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__2 = wrkbl, i__3 = *lda * *n;
			if (*lwork >= f2cmax(i__2,i__3) + *m * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is M by M */

			    ldwrku = *lda;
			    chunk = *n;
			    ldwrkr = *m;
			} else {

/*                    WORK(IU) is M by CHUNK and WORK(IR) is M by M */

			    ldwrku = *m;
			    chunk = (*lwork - *m * *m) / *m;
			    ldwrkr = *m;
			}
		    }
		    itau = ir + ldwrkr * *m;
		    iwork = itau + *m;

/*                 Compute A=L*Q */
/*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy L to WORK(IR) and zero out above it */

		    zlacpy_("L", m, m, &a[a_offset], lda, &work[ir], &ldwrkr);
		    i__2 = *m - 1;
		    i__3 = *m - 1;
		    zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[ir + 
			    ldwrkr], &ldwrkr);

/*                 Generate Q in A */
/*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zunglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = 1;
		    itauq = itau;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize L in WORK(IR) */
/*                 (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB) */
/*                 (RWorkspace: need M) */

		    i__2 = *lwork - iwork + 1;
		    zgebrd_(m, m, &work[ir], &ldwrkr, &s[1], &rwork[ie], &
			    work[itauq], &work[itaup], &work[iwork], &i__2, &
			    ierr);

/*                 Generate right vectors bidiagonalizing L */
/*                 (CWorkspace: need M*M+3*M-1, prefer M*M+2*M+(M-1)*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup], &
			    work[iwork], &i__2, &ierr);
		    irwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing right */
/*                 singular vectors of L in WORK(IR) */
/*                 (CWorkspace: need M*M) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", m, m, &c__0, &c__0, &s[1], &rwork[ie], &work[
			    ir], &ldwrkr, cdum, &c__1, cdum, &c__1, &rwork[
			    irwork], info);
		    iu = itauq;

/*                 Multiply right singular vectors of L in WORK(IR) by Q */
/*                 in A, storing result in WORK(IU) and copying to A */
/*                 (CWorkspace: need M*M+M, prefer M*M+M*N) */
/*                 (RWorkspace: 0) */

		    i__2 = *n;
		    i__3 = chunk;
		    for (i__ = 1; i__3 < 0 ? i__ >= i__2 : i__ <= i__2; i__ +=
			     i__3) {
/* Computing MIN */
			i__4 = *n - i__ + 1;
			blk = f2cmin(i__4,chunk);
			zgemm_("N", "N", m, &blk, m, &c_b2, &work[ir], &
				ldwrkr, &a[i__ * a_dim1 + 1], lda, &c_b1, &
				work[iu], &ldwrku);
			zlacpy_("F", m, &blk, &work[iu], &ldwrku, &a[i__ * 
				a_dim1 + 1], lda);
/* L30: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    ie = 1;
		    itauq = 1;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize A */
/*                 (CWorkspace: need 2*M+N, prefer 2*M+(M+N)*NB) */
/*                 (RWorkspace: need M) */

		    i__3 = *lwork - iwork + 1;
		    zgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__3, &ierr);

/*                 Generate right vectors bidiagonalizing A */
/*                 (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &
			    work[iwork], &i__3, &ierr);
		    irwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing right */
/*                 singular vectors of A in A */
/*                 (CWorkspace: 0) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("L", m, n, &c__0, &c__0, &s[1], &rwork[ie], &a[
			    a_offset], lda, cdum, &c__1, cdum, &c__1, &rwork[
			    irwork], info);

		}

	    } else if (wntvo && wntuas) {

/*              Path 3t(N much larger than M, JOBU='S' or 'A', JOBVT='O') */
/*              M right singular vectors to be overwritten on A and */
/*              M left singular vectors to be computed in U */

		if (*lwork >= *m * *m + *m * 3) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__3 = wrkbl, i__2 = *lda * *n;
		    if (*lwork >= f2cmax(i__3,i__2) + *lda * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is LDA by M */

			ldwrku = *lda;
			chunk = *n;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__3 = wrkbl, i__2 = *lda * *n;
			if (*lwork >= f2cmax(i__3,i__2) + *m * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is M by M */

			    ldwrku = *lda;
			    chunk = *n;
			    ldwrkr = *m;
			} else {

/*                    WORK(IU) is M by CHUNK and WORK(IR) is M by M */

			    ldwrku = *m;
			    chunk = (*lwork - *m * *m) / *m;
			    ldwrkr = *m;
			}
		    }
		    itau = ir + ldwrkr * *m;
		    iwork = itau + *m;

/*                 Compute A=L*Q */
/*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__3, &ierr);

/*                 Copy L to U, zeroing about above it */

		    zlacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		    i__3 = *m - 1;
		    i__2 = *m - 1;
		    zlaset_("U", &i__3, &i__2, &c_b1, &c_b1, &u[(u_dim1 << 1) 
			    + 1], ldu);

/*                 Generate Q in A */
/*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zunglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__3, &ierr);
		    ie = 1;
		    itauq = itau;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize L in U, copying result to WORK(IR) */
/*                 (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB) */
/*                 (RWorkspace: need M) */

		    i__3 = *lwork - iwork + 1;
		    zgebrd_(m, m, &u[u_offset], ldu, &s[1], &rwork[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__3, &ierr);
		    zlacpy_("U", m, m, &u[u_offset], ldu, &work[ir], &ldwrkr);

/*                 Generate right vectors bidiagonalizing L in WORK(IR) */
/*                 (CWorkspace: need M*M+3*M-1, prefer M*M+2*M+(M-1)*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup], &
			    work[iwork], &i__3, &ierr);

/*                 Generate left vectors bidiagonalizing L in U */
/*                 (CWorkspace: need M*M+3*M, prefer M*M+2*M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__3 = *lwork - iwork + 1;
		    zungbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq], &
			    work[iwork], &i__3, &ierr);
		    irwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of L in U, and computing right */
/*                 singular vectors of L in WORK(IR) */
/*                 (CWorkspace: need M*M) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", m, m, m, &c__0, &s[1], &rwork[ie], &work[ir],
			     &ldwrkr, &u[u_offset], ldu, cdum, &c__1, &rwork[
			    irwork], info);
		    iu = itauq;

/*                 Multiply right singular vectors of L in WORK(IR) by Q */
/*                 in A, storing result in WORK(IU) and copying to A */
/*                 (CWorkspace: need M*M+M, prefer M*M+M*N)) */
/*                 (RWorkspace: 0) */

		    i__3 = *n;
		    i__2 = chunk;
		    for (i__ = 1; i__2 < 0 ? i__ >= i__3 : i__ <= i__3; i__ +=
			     i__2) {
/* Computing MIN */
			i__4 = *n - i__ + 1;
			blk = f2cmin(i__4,chunk);
			zgemm_("N", "N", m, &blk, m, &c_b2, &work[ir], &
				ldwrkr, &a[i__ * a_dim1 + 1], lda, &c_b1, &
				work[iu], &ldwrku);
			zlacpy_("F", m, &blk, &work[iu], &ldwrku, &a[i__ * 
				a_dim1 + 1], lda);
/* L40: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    itau = 1;
		    iwork = itau + *m;

/*                 Compute A=L*Q */
/*                 (CWorkspace: need 2*M, prefer M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy L to U, zeroing out above it */

		    zlacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		    i__2 = *m - 1;
		    i__3 = *m - 1;
		    zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &u[(u_dim1 << 1) 
			    + 1], ldu);

/*                 Generate Q in A */
/*                 (CWorkspace: need 2*M, prefer M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zunglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = 1;
		    itauq = itau;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize L in U */
/*                 (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                 (RWorkspace: need M) */

		    i__2 = *lwork - iwork + 1;
		    zgebrd_(m, m, &u[u_offset], ldu, &s[1], &rwork[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__2, &ierr);

/*                 Multiply right vectors bidiagonalizing L by Q in A */
/*                 (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zunmbr_("P", "L", "C", m, n, m, &u[u_offset], ldu, &work[
			    itaup], &a[a_offset], lda, &work[iwork], &i__2, &
			    ierr);

/*                 Generate left vectors bidiagonalizing L in U */
/*                 (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                 (RWorkspace: 0) */

		    i__2 = *lwork - iwork + 1;
		    zungbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq], &
			    work[iwork], &i__2, &ierr);
		    irwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of A in U and computing right */
/*                 singular vectors of A in A */
/*                 (CWorkspace: 0) */
/*                 (RWorkspace: need BDSPAC) */

		    zbdsqr_("U", m, n, m, &c__0, &s[1], &rwork[ie], &a[
			    a_offset], lda, &u[u_offset], ldu, cdum, &c__1, &
			    rwork[irwork], info);

		}

	    } else if (wntvs) {

		if (wntun) {

/*                 Path 4t(N much larger than M, JOBU='N', JOBVT='S') */
/*                 M right singular vectors to be computed in VT and */
/*                 no left singular vectors to be computed */

		    if (*lwork >= *m * *m + *m * 3) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IR) is LDA by M */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is M by M */

			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy L to WORK(IR), zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[ir + 
				ldwrkr], &ldwrkr);

/*                    Generate Q in A */
/*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(m, n, m, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IR) */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &work[ir], &ldwrkr, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate right vectors bidiagonalizing L in */
/*                    WORK(IR) */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+(M-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of L in WORK(IR) */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, m, &c__0, &c__0, &s[1], &rwork[ie], &
				work[ir], &ldwrkr, cdum, &c__1, cdum, &c__1, &
				rwork[irwork], info);

/*                    Multiply right singular vectors of L in WORK(IR) by */
/*                    Q in A, storing result in VT */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, m, &c_b2, &work[ir], &ldwrkr, &
				a[a_offset], lda, &c_b1, &vt[vt_offset], ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy result to VT */

			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(m, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &a[(a_dim1 <<
				 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right vectors bidiagonalizing L by Q in VT */
/*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("P", "L", "C", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, n, &c__0, &c__0, &s[1], &rwork[ie], &
				vt[vt_offset], ldvt, cdum, &c__1, cdum, &c__1,
				 &rwork[irwork], info);

		    }

		} else if (wntuo) {

/*                 Path 5t(N much larger than M, JOBU='O', JOBVT='S') */
/*                 M right singular vectors to be computed in VT and */
/*                 M left singular vectors to be overwritten on A */

		    if (*lwork >= (*m << 1) * *m + *m * 3) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is LDA by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *m) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is M by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			} else {

/*                       WORK(IU) is M by M and WORK(IR) is M by M */

			    ldwrku = *m;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (CWorkspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out below it */

			zlacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 
				ldwrku], &ldwrku);

/*                    Generate Q in A */
/*                    (CWorkspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(m, n, m, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (CWorkspace: need   2*M*M+3*M, */
/*                                 prefer 2*M*M+2*M+2*M*NB) */
/*                    (RWorkspace: need   M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("L", m, m, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need   2*M*M+3*M-1, */
/*                                 prefer 2*M*M+2*M+(M-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in WORK(IR) */
/*                    (CWorkspace: need 2*M*M+3*M, prefer 2*M*M+2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in WORK(IR) and computing */
/*                    right singular vectors of L in WORK(IU) */
/*                    (CWorkspace: need 2*M*M) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, m, m, &c__0, &s[1], &rwork[ie], &work[
				iu], &ldwrku, &work[ir], &ldwrkr, cdum, &c__1,
				 &rwork[irwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in A, storing result in VT */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, m, &c_b2, &work[iu], &ldwrku, &
				a[a_offset], lda, &c_b1, &vt[vt_offset], ldvt);

/*                    Copy left singular vectors of L to A */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zlacpy_("F", m, m, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(m, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &a[(a_dim1 <<
				 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right vectors bidiagonalizing L by Q in VT */
/*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("P", "L", "C", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors of L in A */
/*                    (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &a[a_offset], lda, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in A and computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, n, m, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &a[a_offset], lda, cdum, &
				c__1, &rwork[irwork], info);

		    }

		} else if (wntuas) {

/*                 Path 6t(N much larger than M, JOBU='S' or 'A', */
/*                         JOBVT='S') */
/*                 M right singular vectors to be computed in VT and */
/*                 M left singular vectors to be computed in U */

		    if (*lwork >= *m * *m + *m * 3) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IU) is LDA by N */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is LDA by M */

			    ldwrku = *m;
			}
			itau = iu + ldwrku * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 
				ldwrku], &ldwrku);

/*                    Generate Q in A */
/*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(m, n, m, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to U */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("L", m, m, &work[iu], &ldwrku, &u[u_offset], 
				ldu);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need   M*M+3*M-1, */
/*                                 prefer M*M+2*M+(M-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in U and computing right */
/*                    singular vectors of L in WORK(IU) */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, m, m, &c__0, &s[1], &rwork[ie], &work[
				iu], &ldwrku, &u[u_offset], ldu, cdum, &c__1, 
				&rwork[irwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in A, storing result in VT */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, m, &c_b2, &work[iu], &ldwrku, &
				a[a_offset], lda, &c_b1, &vt[vt_offset], ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(m, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to U, zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], 
				ldu);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &u[(u_dim1 <<
				 1) + 1], ldu);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in U */
/*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &u[u_offset], ldu, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in U by Q */
/*                    in VT */
/*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("P", "L", "C", m, n, m, &u[u_offset], ldu, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, n, m, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, cdum, &
				c__1, &rwork[irwork], info);

		    }

		}

	    } else if (wntva) {

		if (wntun) {

/*                 Path 7t(N much larger than M, JOBU='N', JOBVT='A') */
/*                 N right singular vectors to be computed in VT and */
/*                 no left singular vectors to be computed */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *m * 3;
		    if (*lwork >= *m * *m + f2cmax(i__2,i__3)) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IR) is LDA by M */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is M by M */

			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Copy L to WORK(IR), zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[ir + 
				ldwrkr], &ldwrkr);

/*                    Generate Q in VT */
/*                    (CWorkspace: need M*M+M+N, prefer M*M+M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IR) */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &work[ir], &ldwrkr, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in WORK(IR) */
/*                    (CWorkspace: need   M*M+3*M-1, */
/*                                 prefer M*M+2*M+(M-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of L in WORK(IR) */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, m, &c__0, &c__0, &s[1], &rwork[ie], &
				work[ir], &ldwrkr, cdum, &c__1, cdum, &c__1, &
				rwork[irwork], info);

/*                    Multiply right singular vectors of L in WORK(IR) by */
/*                    Q in VT, storing result in A */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, m, &c_b2, &work[ir], &ldwrkr, &
				vt[vt_offset], ldvt, &c_b1, &a[a_offset], lda);

/*                    Copy right singular vectors of A from A to VT */

			zlacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need M+N, prefer M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &a[(a_dim1 <<
				 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in A by Q */
/*                    in VT */
/*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("P", "L", "C", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, n, &c__0, &c__0, &s[1], &rwork[ie], &
				vt[vt_offset], ldvt, cdum, &c__1, cdum, &c__1,
				 &rwork[irwork], info);

		    }

		} else if (wntuo) {

/*                 Path 8t(N much larger than M, JOBU='O', JOBVT='A') */
/*                 N right singular vectors to be computed in VT and */
/*                 M left singular vectors to be overwritten on A */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *m * 3;
		    if (*lwork >= (*m << 1) * *m + f2cmax(i__2,i__3)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is LDA by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *m) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is M by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			} else {

/*                       WORK(IU) is M by M and WORK(IR) is M by M */

			    ldwrku = *m;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need 2*M*M+M+N, prefer 2*M*M+M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 
				ldwrku], &ldwrku);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (CWorkspace: need   2*M*M+3*M, */
/*                                 prefer 2*M*M+2*M+2*M*NB) */
/*                    (RWorkspace: need   M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("L", m, m, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need   2*M*M+3*M-1, */
/*                                 prefer 2*M*M+2*M+(M-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in WORK(IR) */
/*                    (CWorkspace: need 2*M*M+3*M, prefer 2*M*M+2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in WORK(IR) and computing */
/*                    right singular vectors of L in WORK(IU) */
/*                    (CWorkspace: need 2*M*M) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, m, m, &c__0, &s[1], &rwork[ie], &work[
				iu], &ldwrku, &work[ir], &ldwrkr, cdum, &c__1,
				 &rwork[irwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in VT, storing result in A */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, m, &c_b2, &work[iu], &ldwrku, &
				vt[vt_offset], ldvt, &c_b1, &a[a_offset], lda);

/*                    Copy right singular vectors of A from A to VT */

			zlacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Copy left singular vectors of A from WORK(IR) to A */

			zlacpy_("F", m, m, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need M+N, prefer M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &a[(a_dim1 <<
				 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &a[a_offset], lda, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in A by Q */
/*                    in VT */
/*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("P", "L", "C", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in A */
/*                    (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &a[a_offset], lda, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in A and computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, n, m, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &a[a_offset], lda, cdum, &
				c__1, &rwork[irwork], info);

		    }

		} else if (wntuas) {

/*                 Path 9t(N much larger than M, JOBU='S' or 'A', */
/*                         JOBVT='A') */
/*                 N right singular vectors to be computed in VT and */
/*                 M left singular vectors to be computed in U */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *m * 3;
		    if (*lwork >= *m * *m + f2cmax(i__2,i__3)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IU) is LDA by M */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is M by M */

			    ldwrku = *m;
			}
			itau = iu + ldwrku * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need M*M+M+N, prefer M*M+M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[iu + 
				ldwrku], &ldwrku);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to U */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &work[iu], &ldwrku, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			zlacpy_("L", m, m, &work[iu], &ldwrku, &u[u_offset], 
				ldu);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+(M-1)*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in U and computing right */
/*                    singular vectors of L in WORK(IU) */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, m, m, &c__0, &s[1], &rwork[ie], &work[
				iu], &ldwrku, &u[u_offset], ldu, cdum, &c__1, 
				&rwork[irwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in VT, storing result in A */
/*                    (CWorkspace: need M*M) */
/*                    (RWorkspace: 0) */

			zgemm_("N", "N", m, n, m, &c_b2, &work[iu], &ldwrku, &
				vt[vt_offset], ldvt, &c_b1, &a[a_offset], lda);

/*                    Copy right singular vectors of A from A to VT */

			zlacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (CWorkspace: need 2*M, prefer M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (CWorkspace: need M+N, prefer M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to U, zeroing out above it */

			zlacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], 
				ldu);
			i__2 = *m - 1;
			i__3 = *m - 1;
			zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &u[(u_dim1 <<
				 1) + 1], ldu);
			ie = 1;
			itauq = itau;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in U */
/*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB) */
/*                    (RWorkspace: need M) */

			i__2 = *lwork - iwork + 1;
			zgebrd_(m, m, &u[u_offset], ldu, &s[1], &rwork[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in U by Q */
/*                    in VT */
/*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zunmbr_("P", "L", "C", m, n, m, &u[u_offset], ldu, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*                    (RWorkspace: 0) */

			i__2 = *lwork - iwork + 1;
			zungbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			irwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (CWorkspace: 0) */
/*                    (RWorkspace: need BDSPAC) */

			zbdsqr_("U", m, n, m, &c__0, &s[1], &rwork[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, cdum, &
				c__1, &rwork[irwork], info);

		    }

		}

	    }

	} else {

/*           N .LT. MNTHR */

/*           Path 10t(N greater than M, but not much larger) */
/*           Reduce to bidiagonal form without LQ decomposition */

	    ie = 1;
	    itauq = 1;
	    itaup = itauq + *m;
	    iwork = itaup + *m;

/*           Bidiagonalize A */
/*           (CWorkspace: need 2*M+N, prefer 2*M+(M+N)*NB) */
/*           (RWorkspace: M) */

	    i__2 = *lwork - iwork + 1;
	    zgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[iwork], &i__2, &ierr);
	    if (wntuas) {

/*              If left singular vectors desired in U, copy result to U */
/*              and generate left bidiagonalizing vectors in U */
/*              (CWorkspace: need 3*M-1, prefer 2*M+(M-1)*NB) */
/*              (RWorkspace: 0) */

		zlacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		i__2 = *lwork - iwork + 1;
		zungbr_("Q", m, m, n, &u[u_offset], ldu, &work[itauq], &work[
			iwork], &i__2, &ierr);
	    }
	    if (wntvas) {

/*              If right singular vectors desired in VT, copy result to */
/*              VT and generate right bidiagonalizing vectors in VT */
/*              (CWorkspace: need 2*M+NRVT, prefer 2*M+NRVT*NB) */
/*              (RWorkspace: 0) */

		zlacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		if (wntva) {
		    nrvt = *n;
		}
		if (wntvs) {
		    nrvt = *m;
		}
		i__2 = *lwork - iwork + 1;
		zungbr_("P", &nrvt, n, m, &vt[vt_offset], ldvt, &work[itaup], 
			&work[iwork], &i__2, &ierr);
	    }
	    if (wntuo) {

/*              If left singular vectors desired in A, generate left */
/*              bidiagonalizing vectors in A */
/*              (CWorkspace: need 3*M-1, prefer 2*M+(M-1)*NB) */
/*              (RWorkspace: 0) */

		i__2 = *lwork - iwork + 1;
		zungbr_("Q", m, m, n, &a[a_offset], lda, &work[itauq], &work[
			iwork], &i__2, &ierr);
	    }
	    if (wntvo) {

/*              If right singular vectors desired in A, generate right */
/*              bidiagonalizing vectors in A */
/*              (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*              (RWorkspace: 0) */

		i__2 = *lwork - iwork + 1;
		zungbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &work[
			iwork], &i__2, &ierr);
	    }
	    irwork = ie + *m;
	    if (wntuas || wntuo) {
		nru = *m;
	    }
	    if (wntun) {
		nru = 0;
	    }
	    if (wntvas || wntvo) {
		ncvt = *n;
	    }
	    if (wntvn) {
		ncvt = 0;
	    }
	    if (! wntuo && ! wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in VT */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("L", m, &ncvt, &nru, &c__0, &s[1], &rwork[ie], &vt[
			vt_offset], ldvt, &u[u_offset], ldu, cdum, &c__1, &
			rwork[irwork], info);
	    } else if (! wntuo && wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in A */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("L", m, &ncvt, &nru, &c__0, &s[1], &rwork[ie], &a[
			a_offset], lda, &u[u_offset], ldu, cdum, &c__1, &
			rwork[irwork], info);
	    } else {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in A and computing right singular */
/*              vectors in VT */
/*              (CWorkspace: 0) */
/*              (RWorkspace: need BDSPAC) */

		zbdsqr_("L", m, &ncvt, &nru, &c__0, &s[1], &rwork[ie], &vt[
			vt_offset], ldvt, &a[a_offset], lda, cdum, &c__1, &
			rwork[irwork], info);
	    }

	}

    }

/*     Undo scaling if necessary */

    if (iscl == 1) {
	if (anrm > bignum) {
	    dlascl_("G", &c__0, &c__0, &bignum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, &ierr);
	}
	if (*info != 0 && anrm > bignum) {
	    i__2 = minmn - 1;
	    dlascl_("G", &c__0, &c__0, &bignum, &anrm, &i__2, &c__1, &rwork[
		    ie], &minmn, &ierr);
	}
	if (anrm < smlnum) {
	    dlascl_("G", &c__0, &c__0, &smlnum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, &ierr);
	}
	if (*info != 0 && anrm < smlnum) {
	    i__2 = minmn - 1;
	    dlascl_("G", &c__0, &c__0, &smlnum, &anrm, &i__2, &c__1, &rwork[
		    ie], &minmn, &ierr);
	}
    }

/*     Return optimal workspace in WORK(1) */

    work[1].r = (doublereal) maxwrk, work[1].i = 0.;

    return;

/*     End of ZGESVD */

} /* zgesvd_ */


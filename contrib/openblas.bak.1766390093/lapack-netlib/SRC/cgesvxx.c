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




/* > \brief <b> CGESVXX computes the solution to system of linear equations A * X = B for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGESVXX + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgesvxx
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgesvxx
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgesvxx
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CGESVXX( FACT, TRANS, N, NRHS, A, LDA, AF, LDAF, IPIV, */
/*                           EQUED, R, C, B, LDB, X, LDX, RCOND, RPVGRW, */
/*                           BERR, N_ERR_BNDS, ERR_BNDS_NORM, */
/*                           ERR_BNDS_COMP, NPARAMS, PARAMS, WORK, RWORK, */
/*                           INFO ) */

/*       CHARACTER          EQUED, FACT, TRANS */
/*       INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS, NPARAMS, */
/*      $                   N_ERR_BNDS */
/*       REAL               RCOND, RPVGRW */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ), */
/*      $                   X( LDX , * ),WORK( * ) */
/*       REAL               R( * ), C( * ), PARAMS( * ), BERR( * ), */
/*      $                   ERR_BNDS_NORM( NRHS, * ), */
/*      $                   ERR_BNDS_COMP( NRHS, * ), RWORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    CGESVXX uses the LU factorization to compute the solution to a */
/* >    complex system of linear equations  A * X = B,  where A is an */
/* >    N-by-N matrix and X and B are N-by-NRHS matrices. */
/* > */
/* >    If requested, both normwise and maximum componentwise error bounds */
/* >    are returned. CGESVXX will return a solution with a tiny */
/* >    guaranteed error (O(eps) where eps is the working machine */
/* >    precision) unless the matrix is very ill-conditioned, in which */
/* >    case a warning is returned. Relevant condition numbers also are */
/* >    calculated and returned. */
/* > */
/* >    CGESVXX accepts user-provided factorizations and equilibration */
/* >    factors; see the definitions of the FACT and EQUED options. */
/* >    Solving with refinement and using a factorization from a previous */
/* >    CGESVXX call will also produce a solution with either O(eps) */
/* >    errors or warnings, but we cannot make that claim for general */
/* >    user-provided factorizations and equilibration factors if they */
/* >    differ from what CGESVXX would itself produce. */
/* > \endverbatim */

/* > \par Description: */
/*  ================= */
/* > */
/* > \verbatim */
/* > */
/* >    The following steps are performed: */
/* > */
/* >    1. If FACT = 'E', real scaling factors are computed to equilibrate */
/* >    the system: */
/* > */
/* >      TRANS = 'N':  diag(R)*A*diag(C)     *inv(diag(C))*X = diag(R)*B */
/* >      TRANS = 'T': (diag(R)*A*diag(C))**T *inv(diag(R))*X = diag(C)*B */
/* >      TRANS = 'C': (diag(R)*A*diag(C))**H *inv(diag(R))*X = diag(C)*B */
/* > */
/* >    Whether or not the system will be equilibrated depends on the */
/* >    scaling of the matrix A, but if equilibration is used, A is */
/* >    overwritten by diag(R)*A*diag(C) and B by diag(R)*B (if TRANS='N') */
/* >    or diag(C)*B (if TRANS = 'T' or 'C'). */
/* > */
/* >    2. If FACT = 'N' or 'E', the LU decomposition is used to factor */
/* >    the matrix A (after equilibration if FACT = 'E') as */
/* > */
/* >      A = P * L * U, */
/* > */
/* >    where P is a permutation matrix, L is a unit lower triangular */
/* >    matrix, and U is upper triangular. */
/* > */
/* >    3. If some U(i,i)=0, so that U is exactly singular, then the */
/* >    routine returns with INFO = i. Otherwise, the factored form of A */
/* >    is used to estimate the condition number of the matrix A (see */
/* >    argument RCOND). If the reciprocal of the condition number is less */
/* >    than machine precision, the routine still goes on to solve for X */
/* >    and compute error bounds as described below. */
/* > */
/* >    4. The system of equations is solved for X using the factored form */
/* >    of A. */
/* > */
/* >    5. By default (unless PARAMS(LA_LINRX_ITREF_I) is set to zero), */
/* >    the routine will use iterative refinement to try to get a small */
/* >    error and error bounds.  Refinement calculates the residual to at */
/* >    least twice the working precision. */
/* > */
/* >    6. If equilibration was used, the matrix X is premultiplied by */
/* >    diag(C) (if TRANS = 'N') or diag(R) (if TRANS = 'T' or 'C') so */
/* >    that it solves the original system before equilibration. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \verbatim */
/* >     Some optional parameters are bundled in the PARAMS array.  These */
/* >     settings determine how refinement is performed, but often the */
/* >     defaults are acceptable.  If the defaults are acceptable, users */
/* >     can pass NPARAMS = 0 which prevents the source code from accessing */
/* >     the PARAMS argument. */
/* > \endverbatim */
/* > */
/* > \param[in] FACT */
/* > \verbatim */
/* >          FACT is CHARACTER*1 */
/* >     Specifies whether or not the factored form of the matrix A is */
/* >     supplied on entry, and if not, whether the matrix A should be */
/* >     equilibrated before it is factored. */
/* >       = 'F':  On entry, AF and IPIV contain the factored form of A. */
/* >               If EQUED is not 'N', the matrix A has been */
/* >               equilibrated with scaling factors given by R and C. */
/* >               A, AF, and IPIV are not modified. */
/* >       = 'N':  The matrix A will be copied to AF and factored. */
/* >       = 'E':  The matrix A will be equilibrated if necessary, then */
/* >               copied to AF and factored. */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER*1 */
/* >     Specifies the form of the system of equations: */
/* >       = 'N':  A * X = B     (No transpose) */
/* >       = 'T':  A**T * X = B  (Transpose) */
/* >       = 'C':  A**H * X = B  (Conjugate Transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >     The number of linear equations, i.e., the order of the */
/* >     matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >     The number of right hand sides, i.e., the number of columns */
/* >     of the matrices B and X.  NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >     On entry, the N-by-N matrix A.  If FACT = 'F' and EQUED is */
/* >     not 'N', then A must have been equilibrated by the scaling */
/* >     factors in R and/or C.  A is not modified if FACT = 'F' or */
/* >     'N', or if FACT = 'E' and EQUED = 'N' on exit. */
/* > */
/* >     On exit, if EQUED .ne. 'N', A is scaled as follows: */
/* >     EQUED = 'R':  A := diag(R) * A */
/* >     EQUED = 'C':  A := A * diag(C) */
/* >     EQUED = 'B':  A := diag(R) * A * diag(C). */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >     The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] AF */
/* > \verbatim */
/* >          AF is COMPLEX array, dimension (LDAF,N) */
/* >     If FACT = 'F', then AF is an input argument and on entry */
/* >     contains the factors L and U from the factorization */
/* >     A = P*L*U as computed by CGETRF.  If EQUED .ne. 'N', then */
/* >     AF is the factored form of the equilibrated matrix A. */
/* > */
/* >     If FACT = 'N', then AF is an output argument and on exit */
/* >     returns the factors L and U from the factorization A = P*L*U */
/* >     of the original matrix A. */
/* > */
/* >     If FACT = 'E', then AF is an output argument and on exit */
/* >     returns the factors L and U from the factorization A = P*L*U */
/* >     of the equilibrated matrix A (see the description of A for */
/* >     the form of the equilibrated matrix). */
/* > \endverbatim */
/* > */
/* > \param[in] LDAF */
/* > \verbatim */
/* >          LDAF is INTEGER */
/* >     The leading dimension of the array AF.  LDAF >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >     If FACT = 'F', then IPIV is an input argument and on entry */
/* >     contains the pivot indices from the factorization A = P*L*U */
/* >     as computed by CGETRF; row i of the matrix was interchanged */
/* >     with row IPIV(i). */
/* > */
/* >     If FACT = 'N', then IPIV is an output argument and on exit */
/* >     contains the pivot indices from the factorization A = P*L*U */
/* >     of the original matrix A. */
/* > */
/* >     If FACT = 'E', then IPIV is an output argument and on exit */
/* >     contains the pivot indices from the factorization A = P*L*U */
/* >     of the equilibrated matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in,out] EQUED */
/* > \verbatim */
/* >          EQUED is CHARACTER*1 */
/* >     Specifies the form of equilibration that was done. */
/* >       = 'N':  No equilibration (always true if FACT = 'N'). */
/* >       = 'R':  Row equilibration, i.e., A has been premultiplied by */
/* >               diag(R). */
/* >       = 'C':  Column equilibration, i.e., A has been postmultiplied */
/* >               by diag(C). */
/* >       = 'B':  Both row and column equilibration, i.e., A has been */
/* >               replaced by diag(R) * A * diag(C). */
/* >     EQUED is an input argument if FACT = 'F'; otherwise, it is an */
/* >     output argument. */
/* > \endverbatim */
/* > */
/* > \param[in,out] R */
/* > \verbatim */
/* >          R is REAL array, dimension (N) */
/* >     The row scale factors for A.  If EQUED = 'R' or 'B', A is */
/* >     multiplied on the left by diag(R); if EQUED = 'N' or 'C', R */
/* >     is not accessed.  R is an input argument if FACT = 'F'; */
/* >     otherwise, R is an output argument.  If FACT = 'F' and */
/* >     EQUED = 'R' or 'B', each element of R must be positive. */
/* >     If R is output, each element of R is a power of the radix. */
/* >     If R is input, each element of R should be a power of the radix */
/* >     to ensure a reliable solution and error estimates. Scaling by */
/* >     powers of the radix does not cause rounding errors unless the */
/* >     result underflows or overflows. Rounding errors during scaling */
/* >     lead to refining with a matrix that is not equivalent to the */
/* >     input matrix, producing error estimates that may not be */
/* >     reliable. */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is REAL array, dimension (N) */
/* >     The column scale factors for A.  If EQUED = 'C' or 'B', A is */
/* >     multiplied on the right by diag(C); if EQUED = 'N' or 'R', C */
/* >     is not accessed.  C is an input argument if FACT = 'F'; */
/* >     otherwise, C is an output argument.  If FACT = 'F' and */
/* >     EQUED = 'C' or 'B', each element of C must be positive. */
/* >     If C is output, each element of C is a power of the radix. */
/* >     If C is input, each element of C should be a power of the radix */
/* >     to ensure a reliable solution and error estimates. Scaling by */
/* >     powers of the radix does not cause rounding errors unless the */
/* >     result underflows or overflows. Rounding errors during scaling */
/* >     lead to refining with a matrix that is not equivalent to the */
/* >     input matrix, producing error estimates that may not be */
/* >     reliable. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDB,NRHS) */
/* >     On entry, the N-by-NRHS right hand side matrix B. */
/* >     On exit, */
/* >     if EQUED = 'N', B is not modified; */
/* >     if TRANS = 'N' and EQUED = 'R' or 'B', B is overwritten by */
/* >        diag(R)*B; */
/* >     if TRANS = 'T' or 'C' and EQUED = 'C' or 'B', B is */
/* >        overwritten by diag(C)*B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >     The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] X */
/* > \verbatim */
/* >          X is COMPLEX array, dimension (LDX,NRHS) */
/* >     If INFO = 0, the N-by-NRHS solution matrix X to the original */
/* >     system of equations.  Note that A and B are modified on exit */
/* >     if EQUED .ne. 'N', and the solution to the equilibrated system is */
/* >     inv(diag(C))*X if TRANS = 'N' and EQUED = 'C' or 'B', or */
/* >     inv(diag(R))*X if TRANS = 'T' or 'C' and EQUED = 'R' or 'B'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >     The leading dimension of the array X.  LDX >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] RCOND */
/* > \verbatim */
/* >          RCOND is REAL */
/* >     Reciprocal scaled condition number.  This is an estimate of the */
/* >     reciprocal Skeel condition number of the matrix A after */
/* >     equilibration (if done).  If this is less than the machine */
/* >     precision (in particular, if it is zero), the matrix is singular */
/* >     to working precision.  Note that the error may still be small even */
/* >     if this number is very small and the matrix appears ill- */
/* >     conditioned. */
/* > \endverbatim */
/* > */
/* > \param[out] RPVGRW */
/* > \verbatim */
/* >          RPVGRW is REAL */
/* >     Reciprocal pivot growth.  On exit, this contains the reciprocal */
/* >     pivot growth factor norm(A)/norm(U). The "f2cmax absolute element" */
/* >     norm is used.  If this is much less than 1, then the stability of */
/* >     the LU factorization of the (equilibrated) matrix A could be poor. */
/* >     This also means that the solution X, estimated condition numbers, */
/* >     and error bounds could be unreliable. If factorization fails with */
/* >     0<INFO<=N, then this contains the reciprocal pivot growth factor */
/* >     for the leading INFO columns of A.  In CGESVX, this quantity is */
/* >     returned in WORK(1). */
/* > \endverbatim */
/* > */
/* > \param[out] BERR */
/* > \verbatim */
/* >          BERR is REAL array, dimension (NRHS) */
/* >     Componentwise relative backward error.  This is the */
/* >     componentwise relative backward error of each solution vector X(j) */
/* >     (i.e., the smallest relative change in any element of A or B that */
/* >     makes X(j) an exact solution). */
/* > \endverbatim */
/* > */
/* > \param[in] N_ERR_BNDS */
/* > \verbatim */
/* >          N_ERR_BNDS is INTEGER */
/* >     Number of error bounds to return for each right hand side */
/* >     and each type (normwise or componentwise).  See ERR_BNDS_NORM and */
/* >     ERR_BNDS_COMP below. */
/* > \endverbatim */
/* > */
/* > \param[out] ERR_BNDS_NORM */
/* > \verbatim */
/* >          ERR_BNDS_NORM is REAL array, dimension (NRHS, N_ERR_BNDS) */
/* >     For each right-hand side, this array contains information about */
/* >     various error bounds and condition numbers corresponding to the */
/* >     normwise relative error, which is defined as follows: */
/* > */
/* >     Normwise relative error in the ith solution vector: */
/* >             max_j (abs(XTRUE(j,i) - X(j,i))) */
/* >            ------------------------------ */
/* >                  max_j abs(X(j,i)) */
/* > */
/* >     The array is indexed by the type of error information as described */
/* >     below. There currently are up to three pieces of information */
/* >     returned. */
/* > */
/* >     The first index in ERR_BNDS_NORM(i,:) corresponds to the ith */
/* >     right-hand side. */
/* > */
/* >     The second index in ERR_BNDS_NORM(:,err) contains the following */
/* >     three fields: */
/* >     err = 1 "Trust/don't trust" boolean. Trust the answer if the */
/* >              reciprocal condition number is less than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). */
/* > */
/* >     err = 2 "Guaranteed" error bound: The estimated forward error, */
/* >              almost certainly within a factor of 10 of the true error */
/* >              so long as the next entry is greater than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). This error bound should only */
/* >              be trusted if the previous boolean is true. */
/* > */
/* >     err = 3  Reciprocal condition number: Estimated normwise */
/* >              reciprocal condition number.  Compared with the threshold */
/* >              sqrt(n) * slamch('Epsilon') to determine if the error */
/* >              estimate is "guaranteed". These reciprocal condition */
/* >              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some */
/* >              appropriately scaled matrix Z. */
/* >              Let Z = S*A, where S scales each row by a power of the */
/* >              radix so all absolute row sums of Z are approximately 1. */
/* > */
/* >     See Lapack Working Note 165 for further details and extra */
/* >     cautions. */
/* > \endverbatim */
/* > */
/* > \param[out] ERR_BNDS_COMP */
/* > \verbatim */
/* >          ERR_BNDS_COMP is REAL array, dimension (NRHS, N_ERR_BNDS) */
/* >     For each right-hand side, this array contains information about */
/* >     various error bounds and condition numbers corresponding to the */
/* >     componentwise relative error, which is defined as follows: */
/* > */
/* >     Componentwise relative error in the ith solution vector: */
/* >                    abs(XTRUE(j,i) - X(j,i)) */
/* >             max_j ---------------------- */
/* >                         abs(X(j,i)) */
/* > */
/* >     The array is indexed by the right-hand side i (on which the */
/* >     componentwise relative error depends), and the type of error */
/* >     information as described below. There currently are up to three */
/* >     pieces of information returned for each right-hand side. If */
/* >     componentwise accuracy is not requested (PARAMS(3) = 0.0), then */
/* >     ERR_BNDS_COMP is not accessed.  If N_ERR_BNDS < 3, then at most */
/* >     the first (:,N_ERR_BNDS) entries are returned. */
/* > */
/* >     The first index in ERR_BNDS_COMP(i,:) corresponds to the ith */
/* >     right-hand side. */
/* > */
/* >     The second index in ERR_BNDS_COMP(:,err) contains the following */
/* >     three fields: */
/* >     err = 1 "Trust/don't trust" boolean. Trust the answer if the */
/* >              reciprocal condition number is less than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). */
/* > */
/* >     err = 2 "Guaranteed" error bound: The estimated forward error, */
/* >              almost certainly within a factor of 10 of the true error */
/* >              so long as the next entry is greater than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). This error bound should only */
/* >              be trusted if the previous boolean is true. */
/* > */
/* >     err = 3  Reciprocal condition number: Estimated componentwise */
/* >              reciprocal condition number.  Compared with the threshold */
/* >              sqrt(n) * slamch('Epsilon') to determine if the error */
/* >              estimate is "guaranteed". These reciprocal condition */
/* >              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some */
/* >              appropriately scaled matrix Z. */
/* >              Let Z = S*(A*diag(x)), where x is the solution for the */
/* >              current right-hand side and S scales each row of */
/* >              A*diag(x) by a power of the radix so all absolute row */
/* >              sums of Z are approximately 1. */
/* > */
/* >     See Lapack Working Note 165 for further details and extra */
/* >     cautions. */
/* > \endverbatim */
/* > */
/* > \param[in] NPARAMS */
/* > \verbatim */
/* >          NPARAMS is INTEGER */
/* >     Specifies the number of parameters set in PARAMS.  If <= 0, the */
/* >     PARAMS array is never referenced and default values are used. */
/* > \endverbatim */
/* > */
/* > \param[in,out] PARAMS */
/* > \verbatim */
/* >          PARAMS is REAL array, dimension NPARAMS */
/* >     Specifies algorithm parameters.  If an entry is < 0.0, then */
/* >     that entry will be filled with default value used for that */
/* >     parameter.  Only positions up to NPARAMS are accessed; defaults */
/* >     are used for higher-numbered parameters. */
/* > */
/* >       PARAMS(LA_LINRX_ITREF_I = 1) : Whether to perform iterative */
/* >            refinement or not. */
/* >         Default: 1.0 */
/* >            = 0.0:  No refinement is performed, and no error bounds are */
/* >                    computed. */
/* >            = 1.0:  Use the double-precision refinement algorithm, */
/* >                    possibly with doubled-single computations if the */
/* >                    compilation environment does not support DOUBLE */
/* >                    PRECISION. */
/* >              (other values are reserved for future use) */
/* > */
/* >       PARAMS(LA_LINRX_ITHRESH_I = 2) : Maximum number of residual */
/* >            computations allowed for refinement. */
/* >         Default: 10 */
/* >         Aggressive: Set to 100 to permit convergence using approximate */
/* >                     factorizations or factorizations other than LU. If */
/* >                     the factorization uses a technique other than */
/* >                     Gaussian elimination, the guarantees in */
/* >                     err_bnds_norm and err_bnds_comp may no longer be */
/* >                     trustworthy. */
/* > */
/* >       PARAMS(LA_LINRX_CWISE_I = 3) : Flag determining if the code */
/* >            will attempt to find a solution with small componentwise */
/* >            relative error in the double-precision algorithm.  Positive */
/* >            is true, 0.0 is false. */
/* >         Default: 1.0 (attempt componentwise convergence) */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >       = 0:  Successful exit. The solution to every right-hand side is */
/* >         guaranteed. */
/* >       < 0:  If INFO = -i, the i-th argument had an illegal value */
/* >       > 0 and <= N:  U(INFO,INFO) is exactly zero.  The factorization */
/* >         has been completed, but the factor U is exactly singular, so */
/* >         the solution and error bounds could not be computed. RCOND = 0 */
/* >         is returned. */
/* >       = N+J: The solution corresponding to the Jth right-hand side is */
/* >         not guaranteed. The solutions corresponding to other right- */
/* >         hand sides K with K > J may not be guaranteed as well, but */
/* >         only the first such right-hand side is reported. If a small */
/* >         componentwise error is not requested (PARAMS(3) = 0.0) then */
/* >         the Jth right-hand side is the first with a normwise error */
/* >         bound that is not guaranteed (the smallest J such */
/* >         that ERR_BNDS_NORM(J,1) = 0.0). By default (PARAMS(3) = 1.0) */
/* >         the Jth right-hand side is the first with either a normwise or */
/* >         componentwise error bound that is not guaranteed (the smallest */
/* >         J such that either ERR_BNDS_NORM(J,1) = 0.0 or */
/* >         ERR_BNDS_COMP(J,1) = 0.0). See the definition of */
/* >         ERR_BNDS_NORM(:,1) and ERR_BNDS_COMP(:,1). To get information */
/* >         about all of the right-hand sides check ERR_BNDS_NORM or */
/* >         ERR_BNDS_COMP. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date April 2012 */

/* > \ingroup complexGEsolve */

/*  ===================================================================== */
/* Subroutine */ void cgesvxx_(char *fact, char *trans, integer *n, integer *
	nrhs, complex *a, integer *lda, complex *af, integer *ldaf, integer *
	ipiv, char *equed, real *r__, real *c__, complex *b, integer *ldb, 
	complex *x, integer *ldx, real *rcond, real *rpvgrw, real *berr, 
	integer *n_err_bnds__, real *err_bnds_norm__, real *err_bnds_comp__, 
	integer *nparams, real *params, complex *work, real *rwork, integer *
	info)
{
    /* System generated locals */
    integer a_dim1, a_offset, af_dim1, af_offset, b_dim1, b_offset, x_dim1, 
	    x_offset, err_bnds_norm_dim1, err_bnds_norm_offset, 
	    err_bnds_comp_dim1, err_bnds_comp_offset, i__1;
    real r__1, r__2;

    /* Local variables */
    real amax;
    extern real cla_gerpvgrw_(integer *, integer *, complex *, integer *, 
	    complex *, integer *);
    integer j;
    extern logical lsame_(char *, char *);
    real rcmin, rcmax;
    logical equil;
    extern /* Subroutine */ void claqge_(integer *, integer *, complex *, 
	    integer *, real *, real *, real *, real *, real *, char *)
	    ;
    real colcnd;
    extern real slamch_(char *);
    logical nofact;
    extern /* Subroutine */ void cgetrf_(integer *, integer *, complex *, 
	    integer *, integer *, integer *), clacpy_(char *, integer *, 
	    integer *, complex *, integer *, complex *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    real bignum;
    integer infequ;
    logical colequ;
    extern /* Subroutine */ void cgetrs_(char *, integer *, integer *, complex 
	    *, integer *, integer *, complex *, integer *, integer *);
    real rowcnd;
    logical notran;
    real smlnum;
    logical rowequ;
    extern /* Subroutine */ void clascl2_(integer *, integer *, real *, 
	    complex *, integer *), cgeequb_(integer *, integer *, complex *, 
	    integer *, real *, real *, real *, real *, real *, integer *), 
	    cgerfsx_(char *, char *, integer *, integer *, complex *, integer 
	    *, complex *, integer *, integer *, real *, real *, complex *, 
	    integer *, complex *, integer *, real *, real *, integer *, real *
	    , real *, integer *, real *, complex *, real *, integer *);


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     April 2012 */


/*  ================================================================== */


    /* Parameter adjustments */
    err_bnds_comp_dim1 = *nrhs;
    err_bnds_comp_offset = 1 + err_bnds_comp_dim1 * 1;
    err_bnds_comp__ -= err_bnds_comp_offset;
    err_bnds_norm_dim1 = *nrhs;
    err_bnds_norm_offset = 1 + err_bnds_norm_dim1 * 1;
    err_bnds_norm__ -= err_bnds_norm_offset;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    af_dim1 = *ldaf;
    af_offset = 1 + af_dim1 * 1;
    af -= af_offset;
    --ipiv;
    --r__;
    --c__;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    --berr;
    --params;
    --work;
    --rwork;

    /* Function Body */
    *info = 0;
    nofact = lsame_(fact, "N");
    equil = lsame_(fact, "E");
    notran = lsame_(trans, "N");
    smlnum = slamch_("Safe minimum");
    bignum = 1.f / smlnum;
    if (nofact || equil) {
	*(unsigned char *)equed = 'N';
	rowequ = FALSE_;
	colequ = FALSE_;
    } else {
	rowequ = lsame_(equed, "R") || lsame_(equed, 
		"B");
	colequ = lsame_(equed, "C") || lsame_(equed, 
		"B");
    }

/*     Default is failure.  If an input parameter is wrong or */
/*     factorization fails, make everything look horrible.  Only the */
/*     pivot growth is set here, the rest is initialized in CGERFSX. */

    *rpvgrw = 0.f;

/*     Test the input parameters.  PARAMS is not tested until CGERFSX. */

    if (! nofact && ! equil && ! lsame_(fact, "F")) {
	*info = -1;
    } else if (! notran && ! lsame_(trans, "T") && ! 
	    lsame_(trans, "C")) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*nrhs < 0) {
	*info = -4;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldaf < f2cmax(1,*n)) {
	*info = -8;
    } else if (lsame_(fact, "F") && ! (rowequ || colequ 
	    || lsame_(equed, "N"))) {
	*info = -10;
    } else {
	if (rowequ) {
	    rcmin = bignum;
	    rcmax = 0.f;
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
/* Computing MIN */
		r__1 = rcmin, r__2 = r__[j];
		rcmin = f2cmin(r__1,r__2);
/* Computing MAX */
		r__1 = rcmax, r__2 = r__[j];
		rcmax = f2cmax(r__1,r__2);
/* L10: */
	    }
	    if (rcmin <= 0.f) {
		*info = -11;
	    } else if (*n > 0) {
		rowcnd = f2cmax(rcmin,smlnum) / f2cmin(rcmax,bignum);
	    } else {
		rowcnd = 1.f;
	    }
	}
	if (colequ && *info == 0) {
	    rcmin = bignum;
	    rcmax = 0.f;
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
/* Computing MIN */
		r__1 = rcmin, r__2 = c__[j];
		rcmin = f2cmin(r__1,r__2);
/* Computing MAX */
		r__1 = rcmax, r__2 = c__[j];
		rcmax = f2cmax(r__1,r__2);
/* L20: */
	    }
	    if (rcmin <= 0.f) {
		*info = -12;
	    } else if (*n > 0) {
		colcnd = f2cmax(rcmin,smlnum) / f2cmin(rcmax,bignum);
	    } else {
		colcnd = 1.f;
	    }
	}
	if (*info == 0) {
	    if (*ldb < f2cmax(1,*n)) {
		*info = -14;
	    } else if (*ldx < f2cmax(1,*n)) {
		*info = -16;
	    }
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CGESVXX", &i__1, (ftnlen)7);
	return;
    }

    if (equil) {

/*     Compute row and column scalings to equilibrate the matrix A. */

	cgeequb_(n, n, &a[a_offset], lda, &r__[1], &c__[1], &rowcnd, &colcnd, 
		&amax, &infequ);
	if (infequ == 0) {

/*     Equilibrate the matrix. */

	    claqge_(n, n, &a[a_offset], lda, &r__[1], &c__[1], &rowcnd, &
		    colcnd, &amax, equed);
	    rowequ = lsame_(equed, "R") || lsame_(equed,
		     "B");
	    colequ = lsame_(equed, "C") || lsame_(equed,
		     "B");
	}

/*     If the scaling factors are not applied, set them to 1.0. */

	if (! rowequ) {
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		r__[j] = 1.f;
	    }
	}
	if (! colequ) {
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		c__[j] = 1.f;
	    }
	}
    }

/*     Scale the right-hand side. */

    if (notran) {
	if (rowequ) {
	    clascl2_(n, nrhs, &r__[1], &b[b_offset], ldb);
	}
    } else {
	if (colequ) {
	    clascl2_(n, nrhs, &c__[1], &b[b_offset], ldb);
	}
    }

    if (nofact || equil) {

/*        Compute the LU factorization of A. */

	clacpy_("Full", n, n, &a[a_offset], lda, &af[af_offset], ldaf);
	cgetrf_(n, n, &af[af_offset], ldaf, &ipiv[1], info);

/*        Return if INFO is non-zero. */

	if (*info > 0) {

/*           Pivot in column INFO is exactly 0 */
/*           Compute the reciprocal pivot growth factor of the */
/*           leading rank-deficient INFO columns of A. */

	    *rpvgrw = cla_gerpvgrw_(n, info, &a[a_offset], lda, &af[
		    af_offset], ldaf);
	    return;
	}
    }

/*     Compute the reciprocal pivot growth factor RPVGRW. */

    *rpvgrw = cla_gerpvgrw_(n, n, &a[a_offset], lda, &af[af_offset], ldaf);

/*     Compute the solution matrix X. */

    clacpy_("Full", n, nrhs, &b[b_offset], ldb, &x[x_offset], ldx);
    cgetrs_(trans, n, nrhs, &af[af_offset], ldaf, &ipiv[1], &x[x_offset], ldx,
	     info);

/*     Use iterative refinement to improve the computed solution and */
/*     compute error bounds and backward error estimates for it. */

    cgerfsx_(trans, equed, n, nrhs, &a[a_offset], lda, &af[af_offset], ldaf, &
	    ipiv[1], &r__[1], &c__[1], &b[b_offset], ldb, &x[x_offset], ldx, 
	    rcond, &berr[1], n_err_bnds__, &err_bnds_norm__[
	    err_bnds_norm_offset], &err_bnds_comp__[err_bnds_comp_offset], 
	    nparams, &params[1], &work[1], &rwork[1], info);

/*     Scale solutions. */

    if (colequ && notran) {
	clascl2_(n, nrhs, &c__[1], &x[x_offset], ldx);
    } else if (rowequ && ! notran) {
	clascl2_(n, nrhs, &r__[1], &x[x_offset], ldx);
    }

    return;

/*     End of CGESVXX */

} /* cgesvxx_ */


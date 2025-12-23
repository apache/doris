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

static doublereal c_b10 = 1.;
static doublereal c_b14 = -.125;
static integer c__1 = 1;
static doublereal c_b19 = 0.;
static integer c__2 = 2;

/* > \brief \b DBDSVDX */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DBDSVDX + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dbdsvdx
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dbdsvdx
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dbdsvdx
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*     SUBROUTINE DBDSVDX( UPLO, JOBZ, RANGE, N, D, E, VL, VU, IL, IU, */
/*    $                    NS, S, Z, LDZ, WORK, IWORK, INFO ) */

/*      CHARACTER          JOBZ, RANGE, UPLO */
/*      INTEGER            IL, INFO, IU, LDZ, N, NS */
/*      DOUBLE PRECISION   VL, VU */
/*      INTEGER            IWORK( * ) */
/*      DOUBLE PRECISION   D( * ), E( * ), S( * ), WORK( * ), */
/*                         Z( LDZ, * ) */

/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >  DBDSVDX computes the singular value decomposition (SVD) of a real */
/* >  N-by-N (upper or lower) bidiagonal matrix B, B = U * S * VT, */
/* >  where S is a diagonal matrix with non-negative diagonal elements */
/* >  (the singular values of B), and U and VT are orthogonal matrices */
/* >  of left and right singular vectors, respectively. */
/* > */
/* >  Given an upper bidiagonal B with diagonal D = [ d_1 d_2 ... d_N ] */
/* >  and superdiagonal E = [ e_1 e_2 ... e_N-1 ], DBDSVDX computes the */
/* >  singular value decompositon of B through the eigenvalues and */
/* >  eigenvectors of the N*2-by-N*2 tridiagonal matrix */
/* > */
/* >        |  0  d_1                | */
/* >        | d_1  0  e_1            | */
/* >  TGK = |     e_1  0  d_2        | */
/* >        |         d_2  .   .     | */
/* >        |              .   .   . | */
/* > */
/* >  If (s,u,v) is a singular triplet of B with ||u|| = ||v|| = 1, then */
/* >  (+/-s,q), ||q|| = 1, are eigenpairs of TGK, with q = P * ( u' +/-v' ) / */
/* >  sqrt(2) = ( v_1 u_1 v_2 u_2 ... v_n u_n ) / sqrt(2), and */
/* >  P = [ e_{n+1} e_{1} e_{n+2} e_{2} ... ]. */
/* > */
/* >  Given a TGK matrix, one can either a) compute -s,-v and change signs */
/* >  so that the singular values (and corresponding vectors) are already in */
/* >  descending order (as in DGESVD/DGESDD) or b) compute s,v and reorder */
/* >  the values (and corresponding vectors). DBDSVDX implements a) by */
/* >  calling DSTEVX (bisection plus inverse iteration, to be replaced */
/* >  with a version of the Multiple Relative Robust Representation */
/* >  algorithm. (See P. Willems and B. Lang, A framework for the MR^3 */
/* >  algorithm: theory and implementation, SIAM J. Sci. Comput., */
/* >  35:740-766, 2013.) */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          = 'U':  B is upper bidiagonal; */
/* >          = 'L':  B is lower bidiagonal. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBZ */
/* > \verbatim */
/* >          JOBZ is CHARACTER*1 */
/* >          = 'N':  Compute singular values only; */
/* >          = 'V':  Compute singular values and singular vectors. */
/* > \endverbatim */
/* > */
/* > \param[in] RANGE */
/* > \verbatim */
/* >          RANGE is CHARACTER*1 */
/* >          = 'A': all singular values will be found. */
/* >          = 'V': all singular values in the half-open interval [VL,VU) */
/* >                 will be found. */
/* >          = 'I': the IL-th through IU-th singular values will be found. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the bidiagonal matrix.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >          The n diagonal elements of the bidiagonal matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (f2cmax(1,N-1)) */
/* >          The (n-1) superdiagonal elements of the bidiagonal matrix */
/* >          B in elements 1 to N-1. */
/* > \endverbatim */
/* > */
/* > \param[in] VL */
/* > \verbatim */
/* >         VL is DOUBLE PRECISION */
/* >          If RANGE='V', the lower bound of the interval to */
/* >          be searched for singular values. VU > VL. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] VU */
/* > \verbatim */
/* >         VU is DOUBLE PRECISION */
/* >          If RANGE='V', the upper bound of the interval to */
/* >          be searched for singular values. VU > VL. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] IL */
/* > \verbatim */
/* >          IL is INTEGER */
/* >          If RANGE='I', the index of the */
/* >          smallest singular value to be returned. */
/* >          1 <= IL <= IU <= f2cmin(M,N), if f2cmin(M,N) > 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[in] IU */
/* > \verbatim */
/* >          IU is INTEGER */
/* >          If RANGE='I', the index of the */
/* >          largest singular value to be returned. */
/* >          1 <= IL <= IU <= f2cmin(M,N), if f2cmin(M,N) > 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[out] NS */
/* > \verbatim */
/* >          NS is INTEGER */
/* >          The total number of singular values found.  0 <= NS <= N. */
/* >          If RANGE = 'A', NS = N, and if RANGE = 'I', NS = IU-IL+1. */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is DOUBLE PRECISION array, dimension (N) */
/* >          The first NS elements contain the selected singular values in */
/* >          ascending order. */
/* > \endverbatim */
/* > */
/* > \param[out] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension (2*N,K) */
/* >          If JOBZ = 'V', then if INFO = 0 the first NS columns of Z */
/* >          contain the singular vectors of the matrix B corresponding to */
/* >          the selected singular values, with U in rows 1 to N and V */
/* >          in rows N+1 to N*2, i.e. */
/* >          Z = [ U ] */
/* >              [ V ] */
/* >          If JOBZ = 'N', then Z is not referenced. */
/* >          Note: The user must ensure that at least K = NS+1 columns are */
/* >          supplied in the array Z; if RANGE = 'V', the exact value of */
/* >          NS is not known in advance and an upper bound must be used. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z. LDZ >= 1, and if */
/* >          JOBZ = 'V', LDZ >= f2cmax(2,N*2). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (14*N) */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (12*N) */
/* >          If JOBZ = 'V', then if INFO = 0, the first NS elements of */
/* >          IWORK are zero. If INFO > 0, then IWORK contains the indices */
/* >          of the eigenvectors that failed to converge in DSTEVX. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* >          > 0:  if INFO = i, then i eigenvectors failed to converge */
/* >                   in DSTEVX. The indices of the eigenvectors */
/* >                   (as returned by DSTEVX) are stored in the */
/* >                   array IWORK. */
/* >                if INFO = N*2 + 1, an internal error occurred. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup doubleOTHEReigen */

/*  ===================================================================== */
/* Subroutine */ void dbdsvdx_(char *uplo, char *jobz, char *range, integer *n,
	 doublereal *d__, doublereal *e, doublereal *vl, doublereal *vu, 
	integer *il, integer *iu, integer *ns, doublereal *s, doublereal *z__,
	 integer *ldz, doublereal *work, integer *iwork, integer *info)
{
    /* System generated locals */
    integer z_dim1, z_offset, i__1, i__2, i__3, i__4, i__5;
    doublereal d__1, d__2, d__3, d__4;

    /* Local variables */
    doublereal emin;
    extern doublereal ddot_(integer *, doublereal *, integer *, doublereal *, 
	    integer *);
    integer ntgk;
    doublereal smin, smax, nrmu, nrmv;
    extern doublereal dnrm2_(integer *, doublereal *, integer *);
    logical sveq0;
    integer i__, idbeg, j, k;
    doublereal sqrt2;
    integer idend;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    integer isbeg;
    extern logical lsame_(char *, char *);
    integer idtgk, ietgk, iltgk, itemp;
    extern /* Subroutine */ void dcopy_(integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer icolz;
    logical allsv;
    integer idptr;
    logical indsv;
    integer ieptr, iutgk;
    extern /* Subroutine */ void daxpy_(integer *, doublereal *, doublereal *, 
	    integer *, doublereal *, integer *);
    doublereal vltgk;
    logical lower;
    extern /* Subroutine */ void dswap_(integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    doublereal zjtji;
    logical split, valsv;
    integer isplt;
    doublereal ortol, vutgk;
    logical wantz;
    char rngvx[1];
    integer irowu, irowv, irowz;
    extern doublereal dlamch_(char *);
    integer iifail;
    doublereal mu;
    extern integer idamax_(integer *, doublereal *, integer *);
    extern /* Subroutine */ void dlaset_(char *, integer *, integer *, 
	    doublereal *, doublereal *, doublereal *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    doublereal abstol, thresh;
    integer iiwork;
    extern /* Subroutine */ void dstevx_(char *, char *, integer *, doublereal 
	    *, doublereal *, doublereal *, doublereal *, integer *, integer *,
	     doublereal *, integer *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *, integer *, integer *), 
	    mecago_();
    doublereal eps;
    integer nsl;
    doublereal tol, ulp;
    integer nru, nrv;


/*  -- LAPACK driver routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    --e;
    --s;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;
    --iwork;

    /* Function Body */
    allsv = lsame_(range, "A");
    valsv = lsame_(range, "V");
    indsv = lsame_(range, "I");
    wantz = lsame_(jobz, "V");
    lower = lsame_(uplo, "L");

    *info = 0;
    if (! lsame_(uplo, "U") && ! lower) {
	*info = -1;
    } else if (! (wantz || lsame_(jobz, "N"))) {
	*info = -2;
    } else if (! (allsv || valsv || indsv)) {
	*info = -3;
    } else if (*n < 0) {
	*info = -4;
    } else if (*n > 0) {
	if (valsv) {
	    if (*vl < 0.) {
		*info = -7;
	    } else if (*vu <= *vl) {
		*info = -8;
	    }
	} else if (indsv) {
	    if (*il < 1 || *il > f2cmax(1,*n)) {
		*info = -9;
	    } else if (*iu < f2cmin(*n,*il) || *iu > *n) {
		*info = -10;
	    }
	}
    }
    if (*info == 0) {
	if (*ldz < 1 || wantz && *ldz < *n << 1) {
	    *info = -14;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DBDSVDX", &i__1, (ftnlen)7);
	return;
    }

/*     Quick return if possible (N.LE.1) */

    *ns = 0;
    if (*n == 0) {
	return;
    }

    if (*n == 1) {
	if (allsv || indsv) {
	    *ns = 1;
	    s[1] = abs(d__[1]);
	} else {
	    if (*vl < abs(d__[1]) && *vu >= abs(d__[1])) {
		*ns = 1;
		s[1] = abs(d__[1]);
	    }
	}
	if (wantz) {
	    z__[z_dim1 + 1] = d_sign(&c_b10, &d__[1]);
	    z__[z_dim1 + 2] = 1.;
	}
	return;
    }

    abstol = dlamch_("Safe Minimum") * 2;
    ulp = dlamch_("Precision");
    eps = dlamch_("Epsilon");
    sqrt2 = sqrt(2.);
    ortol = sqrt(ulp);

/*     Criterion for splitting is taken from DBDSQR when singular */
/*     values are computed to relative accuracy TOL. (See J. Demmel and */
/*     W. Kahan, Accurate singular values of bidiagonal matrices, SIAM */
/*     J. Sci. and Stat. Comput., 11:873–912, 1990.) */

/* Computing MAX */
/* Computing MIN */
    d__3 = 100., d__4 = pow_dd(&eps, &c_b14);
    d__1 = 10., d__2 = f2cmin(d__3,d__4);
    tol = f2cmax(d__1,d__2) * eps;

/*     Compute approximate maximum, minimum singular values. */

    i__ = idamax_(n, &d__[1], &c__1);
    smax = (d__1 = d__[i__], abs(d__1));
    i__1 = *n - 1;
    i__ = idamax_(&i__1, &e[1], &c__1);
/* Computing MAX */
    d__2 = smax, d__3 = (d__1 = e[i__], abs(d__1));
    smax = f2cmax(d__2,d__3);

/*     Compute threshold for neglecting D's and E's. */

    smin = abs(d__[1]);
    if (smin != 0.) {
	mu = smin;
	i__1 = *n;
	for (i__ = 2; i__ <= i__1; ++i__) {
	    mu = (d__2 = d__[i__], abs(d__2)) * (mu / (mu + (d__1 = e[i__ - 1]
		    , abs(d__1))));
	    smin = f2cmin(smin,mu);
	    if (smin == 0.) {
		myexit_();
	    }
	}
    }
    smin /= sqrt((doublereal) (*n));
    thresh = tol * smin;

/*     Check for zeros in D and E (splits), i.e. submatrices. */

    i__1 = *n - 1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if ((d__1 = d__[i__], abs(d__1)) <= thresh) {
	    d__[i__] = 0.;
	}
	if ((d__1 = e[i__], abs(d__1)) <= thresh) {
	    e[i__] = 0.;
	}
    }
    if ((d__1 = d__[*n], abs(d__1)) <= thresh) {
	d__[*n] = 0.;
    }

/*     Pointers for arrays used by DSTEVX. */

    idtgk = 1;
    ietgk = idtgk + (*n << 1);
    itemp = ietgk + (*n << 1);
    iifail = 1;
    iiwork = iifail + (*n << 1);

/*     Set RNGVX, which corresponds to RANGE for DSTEVX in TGK mode. */
/*     VL,VU or IL,IU are redefined to conform to implementation a) */
/*     described in the leading comments. */

    iltgk = 0;
    iutgk = 0;
    vltgk = 0.;
    vutgk = 0.;

    if (allsv) {

/*        All singular values will be found. We aim at -s (see */
/*        leading comments) with RNGVX = 'I'. IL and IU are set */
/*        later (as ILTGK and IUTGK) according to the dimension */
/*        of the active submatrix. */

	*(unsigned char *)rngvx = 'I';
	if (wantz) {
	    i__1 = *n << 1;
	    i__2 = *n + 1;
	    dlaset_("F", &i__1, &i__2, &c_b19, &c_b19, &z__[z_offset], ldz);
	}
    } else if (valsv) {

/*        Find singular values in a half-open interval. We aim */
/*        at -s (see leading comments) and we swap VL and VU */
/*        (as VUTGK and VLTGK), changing their signs. */

	*(unsigned char *)rngvx = 'V';
	vltgk = -(*vu);
	vutgk = -(*vl);
	i__1 = idtgk + (*n << 1) - 1;
	for (i__ = idtgk; i__ <= i__1; ++i__) {
	    work[i__] = 0.;
	}
/*         WORK( IDTGK:IDTGK+2*N-1 ) = ZERO */
	dcopy_(n, &d__[1], &c__1, &work[ietgk], &c__2);
	i__1 = *n - 1;
	dcopy_(&i__1, &e[1], &c__1, &work[ietgk + 1], &c__2);
	i__1 = *n << 1;
	dstevx_("N", "V", &i__1, &work[idtgk], &work[ietgk], &vltgk, &vutgk, &
		iltgk, &iltgk, &abstol, ns, &s[1], &z__[z_offset], ldz, &work[
		itemp], &iwork[iiwork], &iwork[iifail], info);
	if (*ns == 0) {
	    return;
	} else {
	    if (wantz) {
		i__1 = *n << 1;
		dlaset_("F", &i__1, ns, &c_b19, &c_b19, &z__[z_offset], ldz);
	    }
	}
    } else if (indsv) {

/*        Find the IL-th through the IU-th singular values. We aim */
/*        at -s (see leading comments) and indices are mapped into */
/*        values, therefore mimicking DSTEBZ, where */

/*        GL = GL - FUDGE*TNORM*ULP*N - FUDGE*TWO*PIVMIN */
/*        GU = GU + FUDGE*TNORM*ULP*N + FUDGE*PIVMIN */

	iltgk = *il;
	iutgk = *iu;
	*(unsigned char *)rngvx = 'V';
	i__1 = idtgk + (*n << 1) - 1;
	for (i__ = idtgk; i__ <= i__1; ++i__) {
	    work[i__] = 0.;
	}
/*         WORK( IDTGK:IDTGK+2*N-1 ) = ZERO */
	dcopy_(n, &d__[1], &c__1, &work[ietgk], &c__2);
	i__1 = *n - 1;
	dcopy_(&i__1, &e[1], &c__1, &work[ietgk + 1], &c__2);
	i__1 = *n << 1;
	dstevx_("N", "I", &i__1, &work[idtgk], &work[ietgk], &vltgk, &vltgk, &
		iltgk, &iltgk, &abstol, ns, &s[1], &z__[z_offset], ldz, &work[
		itemp], &iwork[iiwork], &iwork[iifail], info);
	vltgk = s[1] - smax * 2. * ulp * *n;
	i__1 = idtgk + (*n << 1) - 1;
	for (i__ = idtgk; i__ <= i__1; ++i__) {
	    work[i__] = 0.;
	}
/*         WORK( IDTGK:IDTGK+2*N-1 ) = ZERO */
	dcopy_(n, &d__[1], &c__1, &work[ietgk], &c__2);
	i__1 = *n - 1;
	dcopy_(&i__1, &e[1], &c__1, &work[ietgk + 1], &c__2);
	i__1 = *n << 1;
	dstevx_("N", "I", &i__1, &work[idtgk], &work[ietgk], &vutgk, &vutgk, &
		iutgk, &iutgk, &abstol, ns, &s[1], &z__[z_offset], ldz, &work[
		itemp], &iwork[iiwork], &iwork[iifail], info);
	vutgk = s[1] + smax * 2. * ulp * *n;
	vutgk = f2cmin(vutgk,0.);

/*        If VLTGK=VUTGK, DSTEVX returns an error message, */
/*        so if needed we change VUTGK slightly. */

	if (vltgk == vutgk) {
	    vltgk -= tol;
	}

	if (wantz) {
	    i__1 = *n << 1;
	    i__2 = *iu - *il + 1;
	    dlaset_("F", &i__1, &i__2, &c_b19, &c_b19, &z__[z_offset], ldz);
	}
    }

/*     Initialize variables and pointers for S, Z, and WORK. */

/*     NRU, NRV: number of rows in U and V for the active submatrix */
/*     IDBEG, ISBEG: offsets for the entries of D and S */
/*     IROWZ, ICOLZ: offsets for the rows and columns of Z */
/*     IROWU, IROWV: offsets for the rows of U and V */

    *ns = 0;
    nru = 0;
    nrv = 0;
    idbeg = 1;
    isbeg = 1;
    irowz = 1;
    icolz = 1;
    irowu = 2;
    irowv = 1;
    split = FALSE_;
    sveq0 = FALSE_;

/*     Form the tridiagonal TGK matrix. */

    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	s[i__] = 0.;
    }
/*      S( 1:N ) = ZERO */
    work[ietgk + (*n << 1) - 1] = 0.;
    i__1 = idtgk + (*n << 1) - 1;
    for (i__ = idtgk; i__ <= i__1; ++i__) {
	work[i__] = 0.;
    }
/*      WORK( IDTGK:IDTGK+2*N-1 ) = ZERO */
    dcopy_(n, &d__[1], &c__1, &work[ietgk], &c__2);
    i__1 = *n - 1;
    dcopy_(&i__1, &e[1], &c__1, &work[ietgk + 1], &c__2);


/*     Check for splits in two levels, outer level */
/*     in E and inner level in D. */

    i__1 = *n << 1;
    for (ieptr = 2; ieptr <= i__1; ieptr += 2) {
	if (work[ietgk + ieptr - 1] == 0.) {

/*           Split in E (this piece of B is square) or bottom */
/*           of the (input bidiagonal) matrix. */

	    isplt = idbeg;
	    idend = ieptr - 1;
	    i__2 = idend;
	    for (idptr = idbeg; idptr <= i__2; idptr += 2) {
		if (work[ietgk + idptr - 1] == 0.) {

/*                 Split in D (rectangular submatrix). Set the number */
/*                 of rows in U and V (NRU and NRV) accordingly. */

		    if (idptr == idbeg) {

/*                    D=0 at the top. */

			sveq0 = TRUE_;
			if (idbeg == idend) {
			    nru = 1;
			    nrv = 1;
			}
		    } else if (idptr == idend) {

/*                    D=0 at the bottom. */

			sveq0 = TRUE_;
			nru = (idend - isplt) / 2 + 1;
			nrv = nru;
			if (isplt != idbeg) {
			    ++nru;
			}
		    } else {
			if (isplt == idbeg) {

/*                       Split: top rectangular submatrix. */

			    nru = (idptr - idbeg) / 2;
			    nrv = nru + 1;
			} else {

/*                       Split: middle square submatrix. */

			    nru = (idptr - isplt) / 2 + 1;
			    nrv = nru;
			}
		    }
		} else if (idptr == idend) {

/*                 Last entry of D in the active submatrix. */

		    if (isplt == idbeg) {

/*                    No split (trivial case). */

			nru = (idend - idbeg) / 2 + 1;
			nrv = nru;
		    } else {

/*                    Split: bottom rectangular submatrix. */

			nrv = (idend - isplt) / 2 + 1;
			nru = nrv + 1;
		    }
		}

		ntgk = nru + nrv;

		if (ntgk > 0) {

/*                 Compute eigenvalues/vectors of the active */
/*                 submatrix according to RANGE: */
/*                 if RANGE='A' (ALLSV) then RNGVX = 'I' */
/*                 if RANGE='V' (VALSV) then RNGVX = 'V' */
/*                 if RANGE='I' (INDSV) then RNGVX = 'V' */

		    iltgk = 1;
		    iutgk = ntgk / 2;
		    if (allsv || vutgk == 0.) {
			if (sveq0 || smin < eps || ntgk % 2 > 0) {
/*                        Special case: eigenvalue equal to zero or very */
/*                        small, additional eigenvector is needed. */
			    ++iutgk;
			}
		    }

/*                 Workspace needed by DSTEVX: */
/*                 WORK( ITEMP: ): 2*5*NTGK */
/*                 IWORK( 1: ): 2*6*NTGK */

		    dstevx_(jobz, rngvx, &ntgk, &work[idtgk + isplt - 1], &
			    work[ietgk + isplt - 1], &vltgk, &vutgk, &iltgk, &
			    iutgk, &abstol, &nsl, &s[isbeg], &z__[irowz + 
			    icolz * z_dim1], ldz, &work[itemp], &iwork[iiwork]
			    , &iwork[iifail], info);
		    if (*info != 0) {
/*                    Exit with the error code from DSTEVX. */
			return;
		    }
		    emin = (d__1 = s[isbeg], abs(d__1));
		    i__3 = isbeg + nsl - 1;
		    for (i__ = isbeg; i__ <= i__3; ++i__) {
			if ((d__1 = s[i__], abs(d__1)) > emin) {
			    emin = s[i__];
			}
		    }
/*                  EMIN = ABS( MAXVAL( S( ISBEG:ISBEG+NSL-1 ) ) ) */

		    if (nsl > 0 && wantz) {

/*                    Normalize u=Z([2,4,...],:) and v=Z([1,3,...],:), */
/*                    changing the sign of v as discussed in the leading */
/*                    comments. The norms of u and v may be (slightly) */
/*                    different from 1/sqrt(2) if the corresponding */
/*                    eigenvalues are very small or too close. We check */
/*                    those norms and, if needed, reorthogonalize the */
/*                    vectors. */

			if (nsl > 1 && vutgk == 0. && ntgk % 2 == 0 && emin ==
				 0. && ! split) {

/*                       D=0 at the top or bottom of the active submatrix: */
/*                       one eigenvalue is equal to zero; concatenate the */
/*                       eigenvectors corresponding to the two smallest */
/*                       eigenvalues. */

			    i__3 = irowz + ntgk - 1;
			    for (i__ = irowz; i__ <= i__3; ++i__) {
				z__[i__ + (icolz + nsl - 2) * z_dim1] += z__[
					i__ + (icolz + nsl - 1) * z_dim1];
				z__[i__ + (icolz + nsl - 1) * z_dim1] = 0.;
			    }
/*                        Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-2 ) = */
/*     $                  Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-2 ) + */
/*     $                  Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-1 ) */
/*                        Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-1 ) = */
/*     $                  ZERO */
/*                       IF( IUTGK*2.GT.NTGK ) THEN */
/*                          Eigenvalue equal to zero or very small. */
/*                          NSL = NSL - 1 */
/*                       END IF */
			}

/* Computing MIN */
			i__4 = nsl - 1, i__5 = nru - 1;
			i__3 = f2cmin(i__4,i__5);
			for (i__ = 0; i__ <= i__3; ++i__) {
			    nrmu = dnrm2_(&nru, &z__[irowu + (icolz + i__) * 
				    z_dim1], &c__2);
			    if (nrmu == 0.) {
				*info = (*n << 1) + 1;
				return;
			    }
			    d__1 = 1. / nrmu;
			    dscal_(&nru, &d__1, &z__[irowu + (icolz + i__) * 
				    z_dim1], &c__2);
			    if (nrmu != 1. && (d__1 = nrmu - ortol, abs(d__1))
				     * sqrt2 > 1.) {
				i__4 = i__ - 1;
				for (j = 0; j <= i__4; ++j) {
				    zjtji = -ddot_(&nru, &z__[irowu + (icolz 
					    + j) * z_dim1], &c__2, &z__[irowu 
					    + (icolz + i__) * z_dim1], &c__2);
				    daxpy_(&nru, &zjtji, &z__[irowu + (icolz 
					    + j) * z_dim1], &c__2, &z__[irowu 
					    + (icolz + i__) * z_dim1], &c__2);
				}
				nrmu = dnrm2_(&nru, &z__[irowu + (icolz + i__)
					 * z_dim1], &c__2);
				d__1 = 1. / nrmu;
				dscal_(&nru, &d__1, &z__[irowu + (icolz + i__)
					 * z_dim1], &c__2);
			    }
			}
/* Computing MIN */
			i__4 = nsl - 1, i__5 = nrv - 1;
			i__3 = f2cmin(i__4,i__5);
			for (i__ = 0; i__ <= i__3; ++i__) {
			    nrmv = dnrm2_(&nrv, &z__[irowv + (icolz + i__) * 
				    z_dim1], &c__2);
			    if (nrmv == 0.) {
				*info = (*n << 1) + 1;
				return;
			    }
			    d__1 = -1. / nrmv;
			    dscal_(&nrv, &d__1, &z__[irowv + (icolz + i__) * 
				    z_dim1], &c__2);
			    if (nrmv != 1. && (d__1 = nrmv - ortol, abs(d__1))
				     * sqrt2 > 1.) {
				i__4 = i__ - 1;
				for (j = 0; j <= i__4; ++j) {
				    zjtji = -ddot_(&nrv, &z__[irowv + (icolz 
					    + j) * z_dim1], &c__2, &z__[irowv 
					    + (icolz + i__) * z_dim1], &c__2);
				    daxpy_(&nru, &zjtji, &z__[irowv + (icolz 
					    + j) * z_dim1], &c__2, &z__[irowv 
					    + (icolz + i__) * z_dim1], &c__2);
				}
				nrmv = dnrm2_(&nrv, &z__[irowv + (icolz + i__)
					 * z_dim1], &c__2);
				d__1 = 1. / nrmv;
				dscal_(&nrv, &d__1, &z__[irowv + (icolz + i__)
					 * z_dim1], &c__2);
			    }
			}
			if (vutgk == 0. && idptr < idend && ntgk % 2 > 0) {

/*                       D=0 in the middle of the active submatrix (one */
/*                       eigenvalue is equal to zero): save the corresponding */
/*                       eigenvector for later use (when bottom of the */
/*                       active submatrix is reached). */

			    split = TRUE_;
			    i__3 = irowz + ntgk - 1;
			    for (i__ = irowz; i__ <= i__3; ++i__) {
				z__[i__ + (*n + 1) * z_dim1] = z__[i__ + (*ns 
					+ nsl) * z_dim1];
				z__[i__ + (*ns + nsl) * z_dim1] = 0.;
			    }
/*                        Z( IROWZ:IROWZ+NTGK-1,N+1 ) = */
/*     $                     Z( IROWZ:IROWZ+NTGK-1,NS+NSL ) */
/*                        Z( IROWZ:IROWZ+NTGK-1,NS+NSL ) = */
/*     $                     ZERO */
			}
		    }

/* ** WANTZ **! */
		    nsl = f2cmin(nsl,nru);
		    sveq0 = FALSE_;

/*                 Absolute values of the eigenvalues of TGK. */

		    i__3 = nsl - 1;
		    for (i__ = 0; i__ <= i__3; ++i__) {
			s[isbeg + i__] = (d__1 = s[isbeg + i__], abs(d__1));
		    }

/*                 Update pointers for TGK, S and Z. */

		    isbeg += nsl;
		    irowz += ntgk;
		    icolz += nsl;
		    irowu = irowz;
		    irowv = irowz + 1;
		    isplt = idptr + 1;
		    *ns += nsl;
		    nru = 0;
		    nrv = 0;
		}
/* ** NTGK.GT.0 **! */
		if (irowz < *n << 1 && wantz) {
		    i__3 = irowz - 1;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			z__[i__ + icolz * z_dim1] = 0.;
		    }
/*                       Z( 1:IROWZ-1, ICOLZ ) = ZERO */
		}
	    }
/* ** IDPTR loop **! */
	    if (split && wantz) {

/*              Bring back eigenvector corresponding */
/*              to eigenvalue equal to zero. */

		i__2 = idend - ntgk + 1;
		for (i__ = idbeg; i__ <= i__2; ++i__) {
		    z__[i__ + (isbeg - 1) * z_dim1] += z__[i__ + (*n + 1) * 
			    z_dim1];
		    z__[i__ + (*n + 1) * z_dim1] = 0.;
		}
/*               Z( IDBEG:IDEND-NTGK+1,ISBEG-1 ) = */
/*     $         Z( IDBEG:IDEND-NTGK+1,ISBEG-1 ) + */
/*     $         Z( IDBEG:IDEND-NTGK+1,N+1 ) */
/*               Z( IDBEG:IDEND-NTGK+1,N+1 ) = 0 */
	    }
	    --irowv;
	    ++irowu;
	    idbeg = ieptr + 1;
	    sveq0 = FALSE_;
	    split = FALSE_;
	}
/* ** Check for split in E **! */
    }

/*     Sort the singular values into decreasing order (insertion sort on */
/*     singular values, but only one transposition per singular vector) */

/* ** IEPTR loop **! */
    i__1 = *ns - 1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	k = 1;
	smin = s[1];
	i__2 = *ns + 1 - i__;
	for (j = 2; j <= i__2; ++j) {
	    if (s[j] <= smin) {
		k = j;
		smin = s[j];
	    }
	}
	if (k != *ns + 1 - i__) {
	    s[k] = s[*ns + 1 - i__];
	    s[*ns + 1 - i__] = smin;
	    if (wantz) {
		i__2 = *n << 1;
		dswap_(&i__2, &z__[k * z_dim1 + 1], &c__1, &z__[(*ns + 1 - 
			i__) * z_dim1 + 1], &c__1);
	    }
	}
    }

/*     If RANGE=I, check for singular values/vectors to be discarded. */

    if (indsv) {
	k = *iu - *il + 1;
	if (k < *ns) {
	    i__1 = *ns;
	    for (i__ = k + 1; i__ <= i__1; ++i__) {
		s[i__] = 0.;
	    }
/*            S( K+1:NS ) = ZERO */
	    if (wantz) {
		i__1 = *n << 1;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    i__2 = *ns;
		    for (j = k + 1; j <= i__2; ++j) {
			z__[i__ + j * z_dim1] = 0.;
		    }
		}
/*           Z( 1:N*2,K+1:NS ) = ZERO */
	    }
	    *ns = k;
	}
    }

/*     Reorder Z: U = Z( 1:N,1:NS ), V = Z( N+1:N*2,1:NS ). */
/*     If B is a lower diagonal, swap U and V. */

    if (wantz) {
	i__1 = *ns;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n << 1;
	    dcopy_(&i__2, &z__[i__ * z_dim1 + 1], &c__1, &work[1], &c__1);
	    if (lower) {
		dcopy_(n, &work[2], &c__2, &z__[*n + 1 + i__ * z_dim1], &c__1)
			;
		dcopy_(n, &work[1], &c__2, &z__[i__ * z_dim1 + 1], &c__1);
	    } else {
		dcopy_(n, &work[2], &c__2, &z__[i__ * z_dim1 + 1], &c__1);
		dcopy_(n, &work[1], &c__2, &z__[*n + 1 + i__ * z_dim1], &c__1)
			;
	    }
	}
    }

    return;

/*     End of DBDSVDX */

} /* dbdsvdx_ */


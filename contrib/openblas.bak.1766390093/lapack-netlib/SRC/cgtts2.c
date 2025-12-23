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




/* > \brief \b CGTTS2 solves a system of linear equations with a tridiagonal matrix using the LU factorization
 computed by sgttrf. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGTTS2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgtts2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgtts2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgtts2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CGTTS2( ITRANS, N, NRHS, DL, D, DU, DU2, IPIV, B, LDB ) */

/*       INTEGER            ITRANS, LDB, N, NRHS */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            B( LDB, * ), D( * ), DL( * ), DU( * ), DU2( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CGTTS2 solves one of the systems of equations */
/* >    A * X = B,  A**T * X = B,  or  A**H * X = B, */
/* > with a tridiagonal matrix A using the LU factorization computed */
/* > by CGTTRF. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] ITRANS */
/* > \verbatim */
/* >          ITRANS is INTEGER */
/* >          Specifies the form of the system of equations. */
/* >          = 0:  A * X = B     (No transpose) */
/* >          = 1:  A**T * X = B  (Transpose) */
/* >          = 2:  A**H * X = B  (Conjugate transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >          The number of right hand sides, i.e., the number of columns */
/* >          of the matrix B.  NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] DL */
/* > \verbatim */
/* >          DL is COMPLEX array, dimension (N-1) */
/* >          The (n-1) multipliers that define the matrix L from the */
/* >          LU factorization of A. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is COMPLEX array, dimension (N) */
/* >          The n diagonal elements of the upper triangular matrix U from */
/* >          the LU factorization of A. */
/* > \endverbatim */
/* > */
/* > \param[in] DU */
/* > \verbatim */
/* >          DU is COMPLEX array, dimension (N-1) */
/* >          The (n-1) elements of the first super-diagonal of U. */
/* > \endverbatim */
/* > */
/* > \param[in] DU2 */
/* > \verbatim */
/* >          DU2 is COMPLEX array, dimension (N-2) */
/* >          The (n-2) elements of the second super-diagonal of U. */
/* > \endverbatim */
/* > */
/* > \param[in] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >          The pivot indices; for 1 <= i <= n, row i of the matrix was */
/* >          interchanged with row IPIV(i).  IPIV(i) will always be either */
/* >          i or i+1; IPIV(i) = i indicates a row interchange was not */
/* >          required. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDB,NRHS) */
/* >          On entry, the matrix of right hand side vectors B. */
/* >          On exit, B is overwritten by the solution vectors X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complexGTcomputational */

/*  ===================================================================== */
/* Subroutine */ void cgtts2_(integer *itrans, integer *n, integer *nrhs, 
	complex *dl, complex *d__, complex *du, complex *du2, integer *ipiv, 
	complex *b, integer *ldb)
{
    /* System generated locals */
    integer b_dim1, b_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8;
    complex q__1, q__2, q__3, q__4, q__5, q__6, q__7, q__8;

    /* Local variables */
    complex temp;
    integer i__, j;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Quick return if possible */

    /* Parameter adjustments */
    --dl;
    --d__;
    --du;
    --du2;
    --ipiv;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;

    /* Function Body */
    if (*n == 0 || *nrhs == 0) {
	return;
    }

    if (*itrans == 0) {

/*        Solve A*X = B using the LU factorization of A, */
/*        overwriting each right hand side vector with its solution. */

	if (*nrhs <= 1) {
	    j = 1;
L10:

/*           Solve L*x = b. */

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		if (ipiv[i__] == i__) {
		    i__2 = i__ + 1 + j * b_dim1;
		    i__3 = i__ + 1 + j * b_dim1;
		    i__4 = i__;
		    i__5 = i__ + j * b_dim1;
		    q__2.r = dl[i__4].r * b[i__5].r - dl[i__4].i * b[i__5].i, 
			    q__2.i = dl[i__4].r * b[i__5].i + dl[i__4].i * b[
			    i__5].r;
		    q__1.r = b[i__3].r - q__2.r, q__1.i = b[i__3].i - q__2.i;
		    b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		} else {
		    i__2 = i__ + j * b_dim1;
		    temp.r = b[i__2].r, temp.i = b[i__2].i;
		    i__2 = i__ + j * b_dim1;
		    i__3 = i__ + 1 + j * b_dim1;
		    b[i__2].r = b[i__3].r, b[i__2].i = b[i__3].i;
		    i__2 = i__ + 1 + j * b_dim1;
		    i__3 = i__;
		    i__4 = i__ + j * b_dim1;
		    q__2.r = dl[i__3].r * b[i__4].r - dl[i__3].i * b[i__4].i, 
			    q__2.i = dl[i__3].r * b[i__4].i + dl[i__3].i * b[
			    i__4].r;
		    q__1.r = temp.r - q__2.r, q__1.i = temp.i - q__2.i;
		    b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		}
/* L20: */
	    }

/*           Solve U*x = b. */

	    i__1 = *n + j * b_dim1;
	    c_div(&q__1, &b[*n + j * b_dim1], &d__[*n]);
	    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
	    if (*n > 1) {
		i__1 = *n - 1 + j * b_dim1;
		i__2 = *n - 1 + j * b_dim1;
		i__3 = *n - 1;
		i__4 = *n + j * b_dim1;
		q__3.r = du[i__3].r * b[i__4].r - du[i__3].i * b[i__4].i, 
			q__3.i = du[i__3].r * b[i__4].i + du[i__3].i * b[i__4]
			.r;
		q__2.r = b[i__2].r - q__3.r, q__2.i = b[i__2].i - q__3.i;
		c_div(&q__1, &q__2, &d__[*n - 1]);
		b[i__1].r = q__1.r, b[i__1].i = q__1.i;
	    }
	    for (i__ = *n - 2; i__ >= 1; --i__) {
		i__1 = i__ + j * b_dim1;
		i__2 = i__ + j * b_dim1;
		i__3 = i__;
		i__4 = i__ + 1 + j * b_dim1;
		q__4.r = du[i__3].r * b[i__4].r - du[i__3].i * b[i__4].i, 
			q__4.i = du[i__3].r * b[i__4].i + du[i__3].i * b[i__4]
			.r;
		q__3.r = b[i__2].r - q__4.r, q__3.i = b[i__2].i - q__4.i;
		i__5 = i__;
		i__6 = i__ + 2 + j * b_dim1;
		q__5.r = du2[i__5].r * b[i__6].r - du2[i__5].i * b[i__6].i, 
			q__5.i = du2[i__5].r * b[i__6].i + du2[i__5].i * b[
			i__6].r;
		q__2.r = q__3.r - q__5.r, q__2.i = q__3.i - q__5.i;
		c_div(&q__1, &q__2, &d__[i__]);
		b[i__1].r = q__1.r, b[i__1].i = q__1.i;
/* L30: */
	    }
	    if (j < *nrhs) {
		++j;
		goto L10;
	    }
	} else {
	    i__1 = *nrhs;
	    for (j = 1; j <= i__1; ++j) {

/*           Solve L*x = b. */

		i__2 = *n - 1;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    if (ipiv[i__] == i__) {
			i__3 = i__ + 1 + j * b_dim1;
			i__4 = i__ + 1 + j * b_dim1;
			i__5 = i__;
			i__6 = i__ + j * b_dim1;
			q__2.r = dl[i__5].r * b[i__6].r - dl[i__5].i * b[i__6]
				.i, q__2.i = dl[i__5].r * b[i__6].i + dl[i__5]
				.i * b[i__6].r;
			q__1.r = b[i__4].r - q__2.r, q__1.i = b[i__4].i - 
				q__2.i;
			b[i__3].r = q__1.r, b[i__3].i = q__1.i;
		    } else {
			i__3 = i__ + j * b_dim1;
			temp.r = b[i__3].r, temp.i = b[i__3].i;
			i__3 = i__ + j * b_dim1;
			i__4 = i__ + 1 + j * b_dim1;
			b[i__3].r = b[i__4].r, b[i__3].i = b[i__4].i;
			i__3 = i__ + 1 + j * b_dim1;
			i__4 = i__;
			i__5 = i__ + j * b_dim1;
			q__2.r = dl[i__4].r * b[i__5].r - dl[i__4].i * b[i__5]
				.i, q__2.i = dl[i__4].r * b[i__5].i + dl[i__4]
				.i * b[i__5].r;
			q__1.r = temp.r - q__2.r, q__1.i = temp.i - q__2.i;
			b[i__3].r = q__1.r, b[i__3].i = q__1.i;
		    }
/* L40: */
		}

/*           Solve U*x = b. */

		i__2 = *n + j * b_dim1;
		c_div(&q__1, &b[*n + j * b_dim1], &d__[*n]);
		b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		if (*n > 1) {
		    i__2 = *n - 1 + j * b_dim1;
		    i__3 = *n - 1 + j * b_dim1;
		    i__4 = *n - 1;
		    i__5 = *n + j * b_dim1;
		    q__3.r = du[i__4].r * b[i__5].r - du[i__4].i * b[i__5].i, 
			    q__3.i = du[i__4].r * b[i__5].i + du[i__4].i * b[
			    i__5].r;
		    q__2.r = b[i__3].r - q__3.r, q__2.i = b[i__3].i - q__3.i;
		    c_div(&q__1, &q__2, &d__[*n - 1]);
		    b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		}
		for (i__ = *n - 2; i__ >= 1; --i__) {
		    i__2 = i__ + j * b_dim1;
		    i__3 = i__ + j * b_dim1;
		    i__4 = i__;
		    i__5 = i__ + 1 + j * b_dim1;
		    q__4.r = du[i__4].r * b[i__5].r - du[i__4].i * b[i__5].i, 
			    q__4.i = du[i__4].r * b[i__5].i + du[i__4].i * b[
			    i__5].r;
		    q__3.r = b[i__3].r - q__4.r, q__3.i = b[i__3].i - q__4.i;
		    i__6 = i__;
		    i__7 = i__ + 2 + j * b_dim1;
		    q__5.r = du2[i__6].r * b[i__7].r - du2[i__6].i * b[i__7]
			    .i, q__5.i = du2[i__6].r * b[i__7].i + du2[i__6]
			    .i * b[i__7].r;
		    q__2.r = q__3.r - q__5.r, q__2.i = q__3.i - q__5.i;
		    c_div(&q__1, &q__2, &d__[i__]);
		    b[i__2].r = q__1.r, b[i__2].i = q__1.i;
/* L50: */
		}
/* L60: */
	    }
	}
    } else if (*itrans == 1) {

/*        Solve A**T * X = B. */

	if (*nrhs <= 1) {
	    j = 1;
L70:

/*           Solve U**T * x = b. */

	    i__1 = j * b_dim1 + 1;
	    c_div(&q__1, &b[j * b_dim1 + 1], &d__[1]);
	    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
	    if (*n > 1) {
		i__1 = j * b_dim1 + 2;
		i__2 = j * b_dim1 + 2;
		i__3 = j * b_dim1 + 1;
		q__3.r = du[1].r * b[i__3].r - du[1].i * b[i__3].i, q__3.i = 
			du[1].r * b[i__3].i + du[1].i * b[i__3].r;
		q__2.r = b[i__2].r - q__3.r, q__2.i = b[i__2].i - q__3.i;
		c_div(&q__1, &q__2, &d__[2]);
		b[i__1].r = q__1.r, b[i__1].i = q__1.i;
	    }
	    i__1 = *n;
	    for (i__ = 3; i__ <= i__1; ++i__) {
		i__2 = i__ + j * b_dim1;
		i__3 = i__ + j * b_dim1;
		i__4 = i__ - 1;
		i__5 = i__ - 1 + j * b_dim1;
		q__4.r = du[i__4].r * b[i__5].r - du[i__4].i * b[i__5].i, 
			q__4.i = du[i__4].r * b[i__5].i + du[i__4].i * b[i__5]
			.r;
		q__3.r = b[i__3].r - q__4.r, q__3.i = b[i__3].i - q__4.i;
		i__6 = i__ - 2;
		i__7 = i__ - 2 + j * b_dim1;
		q__5.r = du2[i__6].r * b[i__7].r - du2[i__6].i * b[i__7].i, 
			q__5.i = du2[i__6].r * b[i__7].i + du2[i__6].i * b[
			i__7].r;
		q__2.r = q__3.r - q__5.r, q__2.i = q__3.i - q__5.i;
		c_div(&q__1, &q__2, &d__[i__]);
		b[i__2].r = q__1.r, b[i__2].i = q__1.i;
/* L80: */
	    }

/*           Solve L**T * x = b. */

	    for (i__ = *n - 1; i__ >= 1; --i__) {
		if (ipiv[i__] == i__) {
		    i__1 = i__ + j * b_dim1;
		    i__2 = i__ + j * b_dim1;
		    i__3 = i__;
		    i__4 = i__ + 1 + j * b_dim1;
		    q__2.r = dl[i__3].r * b[i__4].r - dl[i__3].i * b[i__4].i, 
			    q__2.i = dl[i__3].r * b[i__4].i + dl[i__3].i * b[
			    i__4].r;
		    q__1.r = b[i__2].r - q__2.r, q__1.i = b[i__2].i - q__2.i;
		    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
		} else {
		    i__1 = i__ + 1 + j * b_dim1;
		    temp.r = b[i__1].r, temp.i = b[i__1].i;
		    i__1 = i__ + 1 + j * b_dim1;
		    i__2 = i__ + j * b_dim1;
		    i__3 = i__;
		    q__2.r = dl[i__3].r * temp.r - dl[i__3].i * temp.i, 
			    q__2.i = dl[i__3].r * temp.i + dl[i__3].i * 
			    temp.r;
		    q__1.r = b[i__2].r - q__2.r, q__1.i = b[i__2].i - q__2.i;
		    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
		    i__1 = i__ + j * b_dim1;
		    b[i__1].r = temp.r, b[i__1].i = temp.i;
		}
/* L90: */
	    }
	    if (j < *nrhs) {
		++j;
		goto L70;
	    }
	} else {
	    i__1 = *nrhs;
	    for (j = 1; j <= i__1; ++j) {

/*           Solve U**T * x = b. */

		i__2 = j * b_dim1 + 1;
		c_div(&q__1, &b[j * b_dim1 + 1], &d__[1]);
		b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		if (*n > 1) {
		    i__2 = j * b_dim1 + 2;
		    i__3 = j * b_dim1 + 2;
		    i__4 = j * b_dim1 + 1;
		    q__3.r = du[1].r * b[i__4].r - du[1].i * b[i__4].i, 
			    q__3.i = du[1].r * b[i__4].i + du[1].i * b[i__4]
			    .r;
		    q__2.r = b[i__3].r - q__3.r, q__2.i = b[i__3].i - q__3.i;
		    c_div(&q__1, &q__2, &d__[2]);
		    b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		}
		i__2 = *n;
		for (i__ = 3; i__ <= i__2; ++i__) {
		    i__3 = i__ + j * b_dim1;
		    i__4 = i__ + j * b_dim1;
		    i__5 = i__ - 1;
		    i__6 = i__ - 1 + j * b_dim1;
		    q__4.r = du[i__5].r * b[i__6].r - du[i__5].i * b[i__6].i, 
			    q__4.i = du[i__5].r * b[i__6].i + du[i__5].i * b[
			    i__6].r;
		    q__3.r = b[i__4].r - q__4.r, q__3.i = b[i__4].i - q__4.i;
		    i__7 = i__ - 2;
		    i__8 = i__ - 2 + j * b_dim1;
		    q__5.r = du2[i__7].r * b[i__8].r - du2[i__7].i * b[i__8]
			    .i, q__5.i = du2[i__7].r * b[i__8].i + du2[i__7]
			    .i * b[i__8].r;
		    q__2.r = q__3.r - q__5.r, q__2.i = q__3.i - q__5.i;
		    c_div(&q__1, &q__2, &d__[i__]);
		    b[i__3].r = q__1.r, b[i__3].i = q__1.i;
/* L100: */
		}

/*           Solve L**T * x = b. */

		for (i__ = *n - 1; i__ >= 1; --i__) {
		    if (ipiv[i__] == i__) {
			i__2 = i__ + j * b_dim1;
			i__3 = i__ + j * b_dim1;
			i__4 = i__;
			i__5 = i__ + 1 + j * b_dim1;
			q__2.r = dl[i__4].r * b[i__5].r - dl[i__4].i * b[i__5]
				.i, q__2.i = dl[i__4].r * b[i__5].i + dl[i__4]
				.i * b[i__5].r;
			q__1.r = b[i__3].r - q__2.r, q__1.i = b[i__3].i - 
				q__2.i;
			b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		    } else {
			i__2 = i__ + 1 + j * b_dim1;
			temp.r = b[i__2].r, temp.i = b[i__2].i;
			i__2 = i__ + 1 + j * b_dim1;
			i__3 = i__ + j * b_dim1;
			i__4 = i__;
			q__2.r = dl[i__4].r * temp.r - dl[i__4].i * temp.i, 
				q__2.i = dl[i__4].r * temp.i + dl[i__4].i * 
				temp.r;
			q__1.r = b[i__3].r - q__2.r, q__1.i = b[i__3].i - 
				q__2.i;
			b[i__2].r = q__1.r, b[i__2].i = q__1.i;
			i__2 = i__ + j * b_dim1;
			b[i__2].r = temp.r, b[i__2].i = temp.i;
		    }
/* L110: */
		}
/* L120: */
	    }
	}
    } else {

/*        Solve A**H * X = B. */

	if (*nrhs <= 1) {
	    j = 1;
L130:

/*           Solve U**H * x = b. */

	    i__1 = j * b_dim1 + 1;
	    r_cnjg(&q__2, &d__[1]);
	    c_div(&q__1, &b[j * b_dim1 + 1], &q__2);
	    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
	    if (*n > 1) {
		i__1 = j * b_dim1 + 2;
		i__2 = j * b_dim1 + 2;
		r_cnjg(&q__4, &du[1]);
		i__3 = j * b_dim1 + 1;
		q__3.r = q__4.r * b[i__3].r - q__4.i * b[i__3].i, q__3.i = 
			q__4.r * b[i__3].i + q__4.i * b[i__3].r;
		q__2.r = b[i__2].r - q__3.r, q__2.i = b[i__2].i - q__3.i;
		r_cnjg(&q__5, &d__[2]);
		c_div(&q__1, &q__2, &q__5);
		b[i__1].r = q__1.r, b[i__1].i = q__1.i;
	    }
	    i__1 = *n;
	    for (i__ = 3; i__ <= i__1; ++i__) {
		i__2 = i__ + j * b_dim1;
		i__3 = i__ + j * b_dim1;
		r_cnjg(&q__5, &du[i__ - 1]);
		i__4 = i__ - 1 + j * b_dim1;
		q__4.r = q__5.r * b[i__4].r - q__5.i * b[i__4].i, q__4.i = 
			q__5.r * b[i__4].i + q__5.i * b[i__4].r;
		q__3.r = b[i__3].r - q__4.r, q__3.i = b[i__3].i - q__4.i;
		r_cnjg(&q__7, &du2[i__ - 2]);
		i__5 = i__ - 2 + j * b_dim1;
		q__6.r = q__7.r * b[i__5].r - q__7.i * b[i__5].i, q__6.i = 
			q__7.r * b[i__5].i + q__7.i * b[i__5].r;
		q__2.r = q__3.r - q__6.r, q__2.i = q__3.i - q__6.i;
		r_cnjg(&q__8, &d__[i__]);
		c_div(&q__1, &q__2, &q__8);
		b[i__2].r = q__1.r, b[i__2].i = q__1.i;
/* L140: */
	    }

/*           Solve L**H * x = b. */

	    for (i__ = *n - 1; i__ >= 1; --i__) {
		if (ipiv[i__] == i__) {
		    i__1 = i__ + j * b_dim1;
		    i__2 = i__ + j * b_dim1;
		    r_cnjg(&q__3, &dl[i__]);
		    i__3 = i__ + 1 + j * b_dim1;
		    q__2.r = q__3.r * b[i__3].r - q__3.i * b[i__3].i, q__2.i =
			     q__3.r * b[i__3].i + q__3.i * b[i__3].r;
		    q__1.r = b[i__2].r - q__2.r, q__1.i = b[i__2].i - q__2.i;
		    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
		} else {
		    i__1 = i__ + 1 + j * b_dim1;
		    temp.r = b[i__1].r, temp.i = b[i__1].i;
		    i__1 = i__ + 1 + j * b_dim1;
		    i__2 = i__ + j * b_dim1;
		    r_cnjg(&q__3, &dl[i__]);
		    q__2.r = q__3.r * temp.r - q__3.i * temp.i, q__2.i = 
			    q__3.r * temp.i + q__3.i * temp.r;
		    q__1.r = b[i__2].r - q__2.r, q__1.i = b[i__2].i - q__2.i;
		    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
		    i__1 = i__ + j * b_dim1;
		    b[i__1].r = temp.r, b[i__1].i = temp.i;
		}
/* L150: */
	    }
	    if (j < *nrhs) {
		++j;
		goto L130;
	    }
	} else {
	    i__1 = *nrhs;
	    for (j = 1; j <= i__1; ++j) {

/*           Solve U**H * x = b. */

		i__2 = j * b_dim1 + 1;
		r_cnjg(&q__2, &d__[1]);
		c_div(&q__1, &b[j * b_dim1 + 1], &q__2);
		b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		if (*n > 1) {
		    i__2 = j * b_dim1 + 2;
		    i__3 = j * b_dim1 + 2;
		    r_cnjg(&q__4, &du[1]);
		    i__4 = j * b_dim1 + 1;
		    q__3.r = q__4.r * b[i__4].r - q__4.i * b[i__4].i, q__3.i =
			     q__4.r * b[i__4].i + q__4.i * b[i__4].r;
		    q__2.r = b[i__3].r - q__3.r, q__2.i = b[i__3].i - q__3.i;
		    r_cnjg(&q__5, &d__[2]);
		    c_div(&q__1, &q__2, &q__5);
		    b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		}
		i__2 = *n;
		for (i__ = 3; i__ <= i__2; ++i__) {
		    i__3 = i__ + j * b_dim1;
		    i__4 = i__ + j * b_dim1;
		    r_cnjg(&q__5, &du[i__ - 1]);
		    i__5 = i__ - 1 + j * b_dim1;
		    q__4.r = q__5.r * b[i__5].r - q__5.i * b[i__5].i, q__4.i =
			     q__5.r * b[i__5].i + q__5.i * b[i__5].r;
		    q__3.r = b[i__4].r - q__4.r, q__3.i = b[i__4].i - q__4.i;
		    r_cnjg(&q__7, &du2[i__ - 2]);
		    i__6 = i__ - 2 + j * b_dim1;
		    q__6.r = q__7.r * b[i__6].r - q__7.i * b[i__6].i, q__6.i =
			     q__7.r * b[i__6].i + q__7.i * b[i__6].r;
		    q__2.r = q__3.r - q__6.r, q__2.i = q__3.i - q__6.i;
		    r_cnjg(&q__8, &d__[i__]);
		    c_div(&q__1, &q__2, &q__8);
		    b[i__3].r = q__1.r, b[i__3].i = q__1.i;
/* L160: */
		}

/*           Solve L**H * x = b. */

		for (i__ = *n - 1; i__ >= 1; --i__) {
		    if (ipiv[i__] == i__) {
			i__2 = i__ + j * b_dim1;
			i__3 = i__ + j * b_dim1;
			r_cnjg(&q__3, &dl[i__]);
			i__4 = i__ + 1 + j * b_dim1;
			q__2.r = q__3.r * b[i__4].r - q__3.i * b[i__4].i, 
				q__2.i = q__3.r * b[i__4].i + q__3.i * b[i__4]
				.r;
			q__1.r = b[i__3].r - q__2.r, q__1.i = b[i__3].i - 
				q__2.i;
			b[i__2].r = q__1.r, b[i__2].i = q__1.i;
		    } else {
			i__2 = i__ + 1 + j * b_dim1;
			temp.r = b[i__2].r, temp.i = b[i__2].i;
			i__2 = i__ + 1 + j * b_dim1;
			i__3 = i__ + j * b_dim1;
			r_cnjg(&q__3, &dl[i__]);
			q__2.r = q__3.r * temp.r - q__3.i * temp.i, q__2.i = 
				q__3.r * temp.i + q__3.i * temp.r;
			q__1.r = b[i__3].r - q__2.r, q__1.i = b[i__3].i - 
				q__2.i;
			b[i__2].r = q__1.r, b[i__2].i = q__1.i;
			i__2 = i__ + j * b_dim1;
			b[i__2].r = temp.r, b[i__2].i = temp.i;
		    }
/* L170: */
		}
/* L180: */
	    }
	}
    }

/*     End of CGTTS2 */

    return;
} /* cgtts2_ */


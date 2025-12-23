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
static integer c__65 = 65;
static doublereal c_b18 = -1.;
static doublereal c_b31 = 1.;

/* > \brief \b DGBTRF */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DGBTRF + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dgbtrf.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dgbtrf.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dgbtrf.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DGBTRF( M, N, KL, KU, AB, LDAB, IPIV, INFO ) */

/*       INTEGER            INFO, KL, KU, LDAB, M, N */
/*       INTEGER            IPIV( * ) */
/*       DOUBLE PRECISION   AB( LDAB, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DGBTRF computes an LU factorization of a real m-by-n band matrix A */
/* > using partial pivoting with row interchanges. */
/* > */
/* > This is the blocked version of the algorithm, calling Level 3 BLAS. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >          The number of subdiagonals within the band of A.  KL >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >          The number of superdiagonals within the band of A.  KU >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is DOUBLE PRECISION array, dimension (LDAB,N) */
/* >          On entry, the matrix A in band storage, in rows KL+1 to */
/* >          2*KL+KU+1; rows 1 to KL of the array need not be set. */
/* >          The j-th column of A is stored in the j-th column of the */
/* >          array AB as follows: */
/* >          AB(kl+ku+1+i-j,j) = A(i,j) for f2cmax(1,j-ku)<=i<=f2cmin(m,j+kl) */
/* > */
/* >          On exit, details of the factorization: U is stored as an */
/* >          upper triangular band matrix with KL+KU superdiagonals in */
/* >          rows 1 to KL+KU+1, and the multipliers used during the */
/* >          factorization are stored in rows KL+KU+2 to 2*KL+KU+1. */
/* >          See below for further details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >          The leading dimension of the array AB.  LDAB >= 2*KL+KU+1. */
/* > \endverbatim */
/* > */
/* > \param[out] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (f2cmin(M,N)) */
/* >          The pivot indices; for 1 <= i <= f2cmin(M,N), row i of the */
/* >          matrix was interchanged with row IPIV(i). */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          > 0: if INFO = +i, U(i,i) is exactly zero. The factorization */
/* >               has been completed, but the factor U is exactly */
/* >               singular, and division by zero will occur if it is used */
/* >               to solve a system of equations. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleGBcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The band storage scheme is illustrated by the following example, when */
/* >  M = N = 6, KL = 2, KU = 1: */
/* > */
/* >  On entry:                       On exit: */
/* > */
/* >      *    *    *    +    +    +       *    *    *   u14  u25  u36 */
/* >      *    *    +    +    +    +       *    *   u13  u24  u35  u46 */
/* >      *   a12  a23  a34  a45  a56      *   u12  u23  u34  u45  u56 */
/* >     a11  a22  a33  a44  a55  a66     u11  u22  u33  u44  u55  u66 */
/* >     a21  a32  a43  a54  a65   *      m21  m32  m43  m54  m65   * */
/* >     a31  a42  a53  a64   *    *      m31  m42  m53  m64   *    * */
/* > */
/* >  Array elements marked * are not used by the routine; elements marked */
/* >  + need not be set on entry, but are required by the routine to store */
/* >  elements of U because of fill-in resulting from the row interchanges. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dgbtrf_(integer *m, integer *n, integer *kl, integer *ku,
	 doublereal *ab, integer *ldab, integer *ipiv, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, i__1, i__2, i__3, i__4, i__5, i__6;
    doublereal d__1;

    /* Local variables */
    extern /* Subroutine */ void dger_(integer *, integer *, doublereal *, 
	    doublereal *, integer *, doublereal *, integer *, doublereal *, 
	    integer *);
    doublereal temp;
    integer i__, j;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *), dgemm_(char *, char *, integer *, integer *, integer *
	    , doublereal *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, integer *), dcopy_(
	    integer *, doublereal *, integer *, doublereal *, integer *), 
	    dswap_(integer *, doublereal *, integer *, doublereal *, integer *
	    );
    doublereal work13[4160]	/* was [65][64] */, work31[4160]	/* 
	    was [65][64] */;
    extern /* Subroutine */ void dtrsm_(char *, char *, char *, char *, 
	    integer *, integer *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer i2, i3, j2, j3, k2;
    extern /* Subroutine */ void dgbtf2_(integer *, integer *, integer *, 
	    integer *, doublereal *, integer *, integer *, integer *);
    integer jb, nb, ii, jj, jm, ip, jp, km, ju, kv;
    extern integer idamax_(integer *, doublereal *, integer *);
    integer nw;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ int dlaswp_(integer *, doublereal *, integer *, 
	    integer *, integer *, integer *, integer *);


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     KV is the number of superdiagonals in the factor U, allowing for */
/*     fill-in */

    /* Parameter adjustments */
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    --ipiv;

    /* Function Body */
    kv = *ku + *kl;

/*     Test the input parameters. */

    *info = 0;
    if (*m < 0) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*kl < 0) {
	*info = -3;
    } else if (*ku < 0) {
	*info = -4;
    } else if (*ldab < *kl + kv + 1) {
	*info = -6;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DGBTRF", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Determine the block size for this environment */

    nb = ilaenv_(&c__1, "DGBTRF", " ", m, n, kl, ku, (ftnlen)6, (ftnlen)1);

/*     The block size must not exceed the limit set by the size of the */
/*     local arrays WORK13 and WORK31. */

    nb = f2cmin(nb,64);

    if (nb <= 1 || nb > *kl) {

/*        Use unblocked code */

	dgbtf2_(m, n, kl, ku, &ab[ab_offset], ldab, &ipiv[1], info);
    } else {

/*        Use blocked code */

/*        Zero the superdiagonal elements of the work array WORK13 */

	i__1 = nb;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		work13[i__ + j * 65 - 66] = 0.;
/* L10: */
	    }
/* L20: */
	}

/*        Zero the subdiagonal elements of the work array WORK31 */

	i__1 = nb;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = nb;
	    for (i__ = j + 1; i__ <= i__2; ++i__) {
		work31[i__ + j * 65 - 66] = 0.;
/* L30: */
	    }
/* L40: */
	}

/*        Gaussian elimination with partial pivoting */

/*        Set fill-in elements in columns KU+2 to KV to zero */

	i__1 = f2cmin(kv,*n);
	for (j = *ku + 2; j <= i__1; ++j) {
	    i__2 = *kl;
	    for (i__ = kv - j + 2; i__ <= i__2; ++i__) {
		ab[i__ + j * ab_dim1] = 0.;
/* L50: */
	    }
/* L60: */
	}

/*        JU is the index of the last column affected by the current */
/*        stage of the factorization */

	ju = 1;

	i__1 = f2cmin(*m,*n);
	i__2 = nb;
	for (j = 1; i__2 < 0 ? j >= i__1 : j <= i__1; j += i__2) {
/* Computing MIN */
	    i__3 = nb, i__4 = f2cmin(*m,*n) - j + 1;
	    jb = f2cmin(i__3,i__4);

/*           The active part of the matrix is partitioned */

/*              A11   A12   A13 */
/*              A21   A22   A23 */
/*              A31   A32   A33 */

/*           Here A11, A21 and A31 denote the current block of JB columns */
/*           which is about to be factorized. The number of rows in the */
/*           partitioning are JB, I2, I3 respectively, and the numbers */
/*           of columns are JB, J2, J3. The superdiagonal elements of A13 */
/*           and the subdiagonal elements of A31 lie outside the band. */

/* Computing MIN */
	    i__3 = *kl - jb, i__4 = *m - j - jb + 1;
	    i2 = f2cmin(i__3,i__4);
/* Computing MIN */
	    i__3 = jb, i__4 = *m - j - *kl + 1;
	    i3 = f2cmin(i__3,i__4);

/*           J2 and J3 are computed after JU has been updated. */

/*           Factorize the current block of JB columns */

	    i__3 = j + jb - 1;
	    for (jj = j; jj <= i__3; ++jj) {

/*              Set fill-in elements in column JJ+KV to zero */

		if (jj + kv <= *n) {
		    i__4 = *kl;
		    for (i__ = 1; i__ <= i__4; ++i__) {
			ab[i__ + (jj + kv) * ab_dim1] = 0.;
/* L70: */
		    }
		}

/*              Find pivot and test for singularity. KM is the number of */
/*              subdiagonal elements in the current column. */

/* Computing MIN */
		i__4 = *kl, i__5 = *m - jj;
		km = f2cmin(i__4,i__5);
		i__4 = km + 1;
		jp = idamax_(&i__4, &ab[kv + 1 + jj * ab_dim1], &c__1);
		ipiv[jj] = jp + jj - j;
		if (ab[kv + jp + jj * ab_dim1] != 0.) {
/* Computing MAX */
/* Computing MIN */
		    i__6 = jj + *ku + jp - 1;
		    i__4 = ju, i__5 = f2cmin(i__6,*n);
		    ju = f2cmax(i__4,i__5);
		    if (jp != 1) {

/*                    Apply interchange to columns J to J+JB-1 */

			if (jp + jj - 1 < j + *kl) {

			    i__4 = *ldab - 1;
			    i__5 = *ldab - 1;
			    dswap_(&jb, &ab[kv + 1 + jj - j + j * ab_dim1], &
				    i__4, &ab[kv + jp + jj - j + j * ab_dim1],
				     &i__5);
			} else {

/*                       The interchange affects columns J to JJ-1 of A31 */
/*                       which are stored in the work array WORK31 */

			    i__4 = jj - j;
			    i__5 = *ldab - 1;
			    dswap_(&i__4, &ab[kv + 1 + jj - j + j * ab_dim1], 
				    &i__5, &work31[jp + jj - j - *kl - 1], &
				    c__65);
			    i__4 = j + jb - jj;
			    i__5 = *ldab - 1;
			    i__6 = *ldab - 1;
			    dswap_(&i__4, &ab[kv + 1 + jj * ab_dim1], &i__5, &
				    ab[kv + jp + jj * ab_dim1], &i__6);
			}
		    }

/*                 Compute multipliers */

		    d__1 = 1. / ab[kv + 1 + jj * ab_dim1];
		    dscal_(&km, &d__1, &ab[kv + 2 + jj * ab_dim1], &c__1);

/*                 Update trailing submatrix within the band and within */
/*                 the current block. JM is the index of the last column */
/*                 which needs to be updated. */

/* Computing MIN */
		    i__4 = ju, i__5 = j + jb - 1;
		    jm = f2cmin(i__4,i__5);
		    if (jm > jj) {
			i__4 = jm - jj;
			i__5 = *ldab - 1;
			i__6 = *ldab - 1;
			dger_(&km, &i__4, &c_b18, &ab[kv + 2 + jj * ab_dim1], 
				&c__1, &ab[kv + (jj + 1) * ab_dim1], &i__5, &
				ab[kv + 1 + (jj + 1) * ab_dim1], &i__6);
		    }
		} else {

/*                 If pivot is zero, set INFO to the index of the pivot */
/*                 unless a zero pivot has already been found. */

		    if (*info == 0) {
			*info = jj;
		    }
		}

/*              Copy current column of A31 into the work array WORK31 */

/* Computing MIN */
		i__4 = jj - j + 1;
		nw = f2cmin(i__4,i3);
		if (nw > 0) {
		    dcopy_(&nw, &ab[kv + *kl + 1 - jj + j + jj * ab_dim1], &
			    c__1, &work31[(jj - j + 1) * 65 - 65], &c__1);
		}
/* L80: */
	    }
	    if (j + jb <= *n) {

/*              Apply the row interchanges to the other blocks. */

/* Computing MIN */
		i__3 = ju - j + 1;
		j2 = f2cmin(i__3,kv) - jb;
/* Computing MAX */
		i__3 = 0, i__4 = ju - j - kv + 1;
		j3 = f2cmax(i__3,i__4);

/*              Use DLASWP to apply the row interchanges to A12, A22, and */
/*              A32. */

		i__3 = *ldab - 1;
		dlaswp_(&j2, &ab[kv + 1 - jb + (j + jb) * ab_dim1], &i__3, &
			c__1, &jb, &ipiv[j], &c__1);

/*              Adjust the pivot indices. */

		i__3 = j + jb - 1;
		for (i__ = j; i__ <= i__3; ++i__) {
		    ipiv[i__] = ipiv[i__] + j - 1;
/* L90: */
		}

/*              Apply the row interchanges to A13, A23, and A33 */
/*              columnwise. */

		k2 = j - 1 + jb + j2;
		i__3 = j3;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    jj = k2 + i__;
		    i__4 = j + jb - 1;
		    for (ii = j + i__ - 1; ii <= i__4; ++ii) {
			ip = ipiv[ii];
			if (ip != ii) {
			    temp = ab[kv + 1 + ii - jj + jj * ab_dim1];
			    ab[kv + 1 + ii - jj + jj * ab_dim1] = ab[kv + 1 + 
				    ip - jj + jj * ab_dim1];
			    ab[kv + 1 + ip - jj + jj * ab_dim1] = temp;
			}
/* L100: */
		    }
/* L110: */
		}

/*              Update the relevant part of the trailing submatrix */

		if (j2 > 0) {

/*                 Update A12 */

		    i__3 = *ldab - 1;
		    i__4 = *ldab - 1;
		    dtrsm_("Left", "Lower", "No transpose", "Unit", &jb, &j2, 
			    &c_b31, &ab[kv + 1 + j * ab_dim1], &i__3, &ab[kv 
			    + 1 - jb + (j + jb) * ab_dim1], &i__4);

		    if (i2 > 0) {

/*                    Update A22 */

			i__3 = *ldab - 1;
			i__4 = *ldab - 1;
			i__5 = *ldab - 1;
			dgemm_("No transpose", "No transpose", &i2, &j2, &jb, 
				&c_b18, &ab[kv + 1 + jb + j * ab_dim1], &i__3,
				 &ab[kv + 1 - jb + (j + jb) * ab_dim1], &i__4,
				 &c_b31, &ab[kv + 1 + (j + jb) * ab_dim1], &
				i__5);
		    }

		    if (i3 > 0) {

/*                    Update A32 */

			i__3 = *ldab - 1;
			i__4 = *ldab - 1;
			dgemm_("No transpose", "No transpose", &i3, &j2, &jb, 
				&c_b18, work31, &c__65, &ab[kv + 1 - jb + (j 
				+ jb) * ab_dim1], &i__3, &c_b31, &ab[kv + *kl 
				+ 1 - jb + (j + jb) * ab_dim1], &i__4);
		    }
		}

		if (j3 > 0) {

/*                 Copy the lower triangle of A13 into the work array */
/*                 WORK13 */

		    i__3 = j3;
		    for (jj = 1; jj <= i__3; ++jj) {
			i__4 = jb;
			for (ii = jj; ii <= i__4; ++ii) {
			    work13[ii + jj * 65 - 66] = ab[ii - jj + 1 + (jj 
				    + j + kv - 1) * ab_dim1];
/* L120: */
			}
/* L130: */
		    }

/*                 Update A13 in the work array */

		    i__3 = *ldab - 1;
		    dtrsm_("Left", "Lower", "No transpose", "Unit", &jb, &j3, 
			    &c_b31, &ab[kv + 1 + j * ab_dim1], &i__3, work13, 
			    &c__65);

		    if (i2 > 0) {

/*                    Update A23 */

			i__3 = *ldab - 1;
			i__4 = *ldab - 1;
			dgemm_("No transpose", "No transpose", &i2, &j3, &jb, 
				&c_b18, &ab[kv + 1 + jb + j * ab_dim1], &i__3,
				 work13, &c__65, &c_b31, &ab[jb + 1 + (j + kv)
				 * ab_dim1], &i__4);
		    }

		    if (i3 > 0) {

/*                    Update A33 */

			i__3 = *ldab - 1;
			dgemm_("No transpose", "No transpose", &i3, &j3, &jb, 
				&c_b18, work31, &c__65, work13, &c__65, &
				c_b31, &ab[*kl + 1 + (j + kv) * ab_dim1], &
				i__3);
		    }

/*                 Copy the lower triangle of A13 back into place */

		    i__3 = j3;
		    for (jj = 1; jj <= i__3; ++jj) {
			i__4 = jb;
			for (ii = jj; ii <= i__4; ++ii) {
			    ab[ii - jj + 1 + (jj + j + kv - 1) * ab_dim1] = 
				    work13[ii + jj * 65 - 66];
/* L140: */
			}
/* L150: */
		    }
		}
	    } else {

/*              Adjust the pivot indices. */

		i__3 = j + jb - 1;
		for (i__ = j; i__ <= i__3; ++i__) {
		    ipiv[i__] = ipiv[i__] + j - 1;
/* L160: */
		}
	    }

/*           Partially undo the interchanges in the current block to */
/*           restore the upper triangular form of A31 and copy the upper */
/*           triangle of A31 back into place */

	    i__3 = j;
	    for (jj = j + jb - 1; jj >= i__3; --jj) {
		jp = ipiv[jj] - jj + 1;
		if (jp != 1) {

/*                 Apply interchange to columns J to JJ-1 */

		    if (jp + jj - 1 < j + *kl) {

/*                    The interchange does not affect A31 */

			i__4 = jj - j;
			i__5 = *ldab - 1;
			i__6 = *ldab - 1;
			dswap_(&i__4, &ab[kv + 1 + jj - j + j * ab_dim1], &
				i__5, &ab[kv + jp + jj - j + j * ab_dim1], &
				i__6);
		    } else {

/*                    The interchange does affect A31 */

			i__4 = jj - j;
			i__5 = *ldab - 1;
			dswap_(&i__4, &ab[kv + 1 + jj - j + j * ab_dim1], &
				i__5, &work31[jp + jj - j - *kl - 1], &c__65);
		    }
		}

/*              Copy the current column of A31 back into place */

/* Computing MIN */
		i__4 = i3, i__5 = jj - j + 1;
		nw = f2cmin(i__4,i__5);
		if (nw > 0) {
		    dcopy_(&nw, &work31[(jj - j + 1) * 65 - 65], &c__1, &ab[
			    kv + *kl + 1 - jj + j + jj * ab_dim1], &c__1);
		}
/* L170: */
	    }
/* L180: */
	}
    }

    return;

/*     End of DGBTRF */

} /* dgbtrf_ */


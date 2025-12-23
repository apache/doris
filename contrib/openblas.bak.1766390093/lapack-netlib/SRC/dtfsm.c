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

static doublereal c_b23 = -1.;
static doublereal c_b27 = 1.;

/* > \brief \b DTFSM solves a matrix equation (one operand is a triangular matrix in RFP format). */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DTFSM + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtfsm.f
"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtfsm.f
"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtfsm.f
"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DTFSM( TRANSR, SIDE, UPLO, TRANS, DIAG, M, N, ALPHA, A, */
/*                         B, LDB ) */

/*       CHARACTER          TRANSR, DIAG, SIDE, TRANS, UPLO */
/*       INTEGER            LDB, M, N */
/*       DOUBLE PRECISION   ALPHA */
/*       DOUBLE PRECISION   A( 0: * ), B( 0: LDB-1, 0: * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > Level 3 BLAS like routine for A in RFP Format. */
/* > */
/* > DTFSM  solves the matrix equation */
/* > */
/* >    op( A )*X = alpha*B  or  X*op( A ) = alpha*B */
/* > */
/* > where alpha is a scalar, X and B are m by n matrices, A is a unit, or */
/* > non-unit,  upper or lower triangular matrix  and  op( A )  is one  of */
/* > */
/* >    op( A ) = A   or   op( A ) = A**T. */
/* > */
/* > A is in Rectangular Full Packed (RFP) Format. */
/* > */
/* > The matrix X is overwritten on B. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] TRANSR */
/* > \verbatim */
/* >          TRANSR is CHARACTER*1 */
/* >          = 'N':  The Normal Form of RFP A is stored; */
/* >          = 'T':  The Transpose Form of RFP A is stored. */
/* > \endverbatim */
/* > */
/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >           On entry, SIDE specifies whether op( A ) appears on the left */
/* >           or right of X as follows: */
/* > */
/* >              SIDE = 'L' or 'l'   op( A )*X = alpha*B. */
/* > */
/* >              SIDE = 'R' or 'r'   X*op( A ) = alpha*B. */
/* > */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >           On entry, UPLO specifies whether the RFP matrix A came from */
/* >           an upper or lower triangular matrix as follows: */
/* >           UPLO = 'U' or 'u' RFP A came from an upper triangular matrix */
/* >           UPLO = 'L' or 'l' RFP A came from a  lower triangular matrix */
/* > */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER*1 */
/* >           On entry, TRANS  specifies the form of op( A ) to be used */
/* >           in the matrix multiplication as follows: */
/* > */
/* >              TRANS  = 'N' or 'n'   op( A ) = A. */
/* > */
/* >              TRANS  = 'T' or 't'   op( A ) = A'. */
/* > */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] DIAG */
/* > \verbatim */
/* >          DIAG is CHARACTER*1 */
/* >           On entry, DIAG specifies whether or not RFP A is unit */
/* >           triangular as follows: */
/* > */
/* >              DIAG = 'U' or 'u'   A is assumed to be unit triangular. */
/* > */
/* >              DIAG = 'N' or 'n'   A is not assumed to be unit */
/* >                                  triangular. */
/* > */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >           On entry, M specifies the number of rows of B. M must be at */
/* >           least zero. */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           On entry, N specifies the number of columns of B.  N must be */
/* >           at least zero. */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] ALPHA */
/* > \verbatim */
/* >          ALPHA is DOUBLE PRECISION */
/* >           On entry,  ALPHA specifies the scalar  alpha. When  alpha is */
/* >           zero then  A is not referenced and  B need not be set before */
/* >           entry. */
/* >           Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (NT) */
/* >           NT = N*(N+1)/2. On entry, the matrix A in RFP Format. */
/* >           RFP Format is described by TRANSR, UPLO and N as follows: */
/* >           If TRANSR='N' then RFP A is (0:N,0:K-1) when N is even; */
/* >           K=N/2. RFP A is (0:N-1,0:K) when N is odd; K=N/2. If */
/* >           TRANSR = 'T' then RFP is the transpose of RFP A as */
/* >           defined when TRANSR = 'N'. The contents of RFP A are defined */
/* >           by UPLO as follows: If UPLO = 'U' the RFP A contains the NT */
/* >           elements of upper packed A either in normal or */
/* >           transpose Format. If UPLO = 'L' the RFP A contains */
/* >           the NT elements of lower packed A either in normal or */
/* >           transpose Format. The LDA of RFP A is (N+1)/2 when */
/* >           TRANSR = 'T'. When TRANSR is 'N' the LDA is N+1 when N is */
/* >           even and is N when is odd. */
/* >           See the Note below for more details. Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB,N) */
/* >           Before entry,  the leading  m by n part of the array  B must */
/* >           contain  the  right-hand  side  matrix  B,  and  on exit  is */
/* >           overwritten by the solution matrix  X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >           On entry, LDB specifies the first dimension of B as declared */
/* >           in  the  calling  (sub)  program.   LDB  must  be  at  least */
/* >           f2cmax( 1, m ). */
/* >           Unchanged on exit. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  We first consider Rectangular Full Packed (RFP) Format when N is */
/* >  even. We give an example where N = 6. */
/* > */
/* >      AP is Upper             AP is Lower */
/* > */
/* >   00 01 02 03 04 05       00 */
/* >      11 12 13 14 15       10 11 */
/* >         22 23 24 25       20 21 22 */
/* >            33 34 35       30 31 32 33 */
/* >               44 45       40 41 42 43 44 */
/* >                  55       50 51 52 53 54 55 */
/* > */
/* > */
/* >  Let TRANSR = 'N'. RFP holds AP as follows: */
/* >  For UPLO = 'U' the upper trapezoid A(0:5,0:2) consists of the last */
/* >  three columns of AP upper. The lower triangle A(4:6,0:2) consists of */
/* >  the transpose of the first three columns of AP upper. */
/* >  For UPLO = 'L' the lower trapezoid A(1:6,0:2) consists of the first */
/* >  three columns of AP lower. The upper triangle A(0:2,0:2) consists of */
/* >  the transpose of the last three columns of AP lower. */
/* >  This covers the case N even and TRANSR = 'N'. */
/* > */
/* >         RFP A                   RFP A */
/* > */
/* >        03 04 05                33 43 53 */
/* >        13 14 15                00 44 54 */
/* >        23 24 25                10 11 55 */
/* >        33 34 35                20 21 22 */
/* >        00 44 45                30 31 32 */
/* >        01 11 55                40 41 42 */
/* >        02 12 22                50 51 52 */
/* > */
/* >  Now let TRANSR = 'T'. RFP A in both UPLO cases is just the */
/* >  transpose of RFP A above. One therefore gets: */
/* > */
/* > */
/* >           RFP A                   RFP A */
/* > */
/* >     03 13 23 33 00 01 02    33 00 10 20 30 40 50 */
/* >     04 14 24 34 44 11 12    43 44 11 21 31 41 51 */
/* >     05 15 25 35 45 55 22    53 54 55 22 32 42 52 */
/* > */
/* > */
/* >  We then consider Rectangular Full Packed (RFP) Format when N is */
/* >  odd. We give an example where N = 5. */
/* > */
/* >     AP is Upper                 AP is Lower */
/* > */
/* >   00 01 02 03 04              00 */
/* >      11 12 13 14              10 11 */
/* >         22 23 24              20 21 22 */
/* >            33 34              30 31 32 33 */
/* >               44              40 41 42 43 44 */
/* > */
/* > */
/* >  Let TRANSR = 'N'. RFP holds AP as follows: */
/* >  For UPLO = 'U' the upper trapezoid A(0:4,0:2) consists of the last */
/* >  three columns of AP upper. The lower triangle A(3:4,0:1) consists of */
/* >  the transpose of the first two columns of AP upper. */
/* >  For UPLO = 'L' the lower trapezoid A(0:4,0:2) consists of the first */
/* >  three columns of AP lower. The upper triangle A(0:1,1:2) consists of */
/* >  the transpose of the last two columns of AP lower. */
/* >  This covers the case N odd and TRANSR = 'N'. */
/* > */
/* >         RFP A                   RFP A */
/* > */
/* >        02 03 04                00 33 43 */
/* >        12 13 14                10 11 44 */
/* >        22 23 24                20 21 22 */
/* >        00 33 34                30 31 32 */
/* >        01 11 44                40 41 42 */
/* > */
/* >  Now let TRANSR = 'T'. RFP A in both UPLO cases is just the */
/* >  transpose of RFP A above. One therefore gets: */
/* > */
/* >           RFP A                   RFP A */
/* > */
/* >     02 12 22 00 01             00 10 20 30 40 50 */
/* >     03 13 23 33 11             33 11 21 31 41 51 */
/* >     04 14 24 34 44             43 44 22 32 42 52 */
/* > \endverbatim */

/*  ===================================================================== */
/* Subroutine */ void dtfsm_(char *transr, char *side, char *uplo, char *trans,
	 char *diag, integer *m, integer *n, doublereal *alpha, doublereal *a,
	 doublereal *b, integer *ldb)
{
    /* System generated locals */
    integer b_dim1, b_offset, i__1, i__2;

    /* Local variables */
    integer info, i__, j, k;
    logical normaltransr;
    extern /* Subroutine */ void dgemm_(char *, char *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, integer *);
    logical lside;
    extern logical lsame_(char *, char *);
    logical lower;
    extern /* Subroutine */ void dtrsm_(char *, char *, char *, char *, 
	    integer *, integer *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer m1, m2, n1, n2;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    logical misodd, nisodd, notrans;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    b_dim1 = *ldb - 1 - 0 + 1;
    b_offset = 0 + b_dim1 * 0;
    b -= b_offset;

    /* Function Body */
    info = 0;
    normaltransr = lsame_(transr, "N");
    lside = lsame_(side, "L");
    lower = lsame_(uplo, "L");
    notrans = lsame_(trans, "N");
    if (! normaltransr && ! lsame_(transr, "T")) {
	info = -1;
    } else if (! lside && ! lsame_(side, "R")) {
	info = -2;
    } else if (! lower && ! lsame_(uplo, "U")) {
	info = -3;
    } else if (! notrans && ! lsame_(trans, "T")) {
	info = -4;
    } else if (! lsame_(diag, "N") && ! lsame_(diag, 
	    "U")) {
	info = -5;
    } else if (*m < 0) {
	info = -6;
    } else if (*n < 0) {
	info = -7;
    } else if (*ldb < f2cmax(1,*m)) {
	info = -11;
    }
    if (info != 0) {
	i__1 = -info;
	xerbla_("DTFSM ", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return when ( (N.EQ.0).OR.(M.EQ.0) ) */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Quick return when ALPHA.EQ.(0D+0) */

    if (*alpha == 0.) {
	i__1 = *n - 1;
	for (j = 0; j <= i__1; ++j) {
	    i__2 = *m - 1;
	    for (i__ = 0; i__ <= i__2; ++i__) {
		b[i__ + j * b_dim1] = 0.;
/* L10: */
	    }
/* L20: */
	}
	return;
    }

    if (lside) {

/*        SIDE = 'L' */

/*        A is M-by-M. */
/*        If M is odd, set NISODD = .TRUE., and M1 and M2. */
/*        If M is even, NISODD = .FALSE., and M. */

	if (*m % 2 == 0) {
	    misodd = FALSE_;
	    k = *m / 2;
	} else {
	    misodd = TRUE_;
	    if (lower) {
		m2 = *m / 2;
		m1 = *m - m2;
	    } else {
		m1 = *m / 2;
		m2 = *m - m1;
	    }
	}


	if (misodd) {

/*           SIDE = 'L' and N is odd */

	    if (normaltransr) {

/*              SIDE = 'L', N is odd, and TRANSR = 'N' */

		if (lower) {

/*                 SIDE  ='L', N is odd, TRANSR = 'N', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='L', N is odd, TRANSR = 'N', UPLO = 'L', and */
/*                    TRANS = 'N' */

			if (*m == 1) {
			    dtrsm_("L", "L", "N", diag, &m1, n, alpha, a, m, &
				    b[b_offset], ldb);
			} else {
			    dtrsm_("L", "L", "N", diag, &m1, n, alpha, a, m, &
				    b[b_offset], ldb);
			    dgemm_("N", "N", &m2, n, &m1, &c_b23, &a[m1], m, &
				    b[b_offset], ldb, alpha, &b[m1], ldb);
			    dtrsm_("L", "U", "T", diag, &m2, n, &c_b27, &a[*m]
				    , m, &b[m1], ldb);
			}

		    } else {

/*                    SIDE  ='L', N is odd, TRANSR = 'N', UPLO = 'L', and */
/*                    TRANS = 'T' */

			if (*m == 1) {
			    dtrsm_("L", "L", "T", diag, &m1, n, alpha, a, m, &
				    b[b_offset], ldb);
			} else {
			    dtrsm_("L", "U", "N", diag, &m2, n, alpha, &a[*m],
				     m, &b[m1], ldb);
			    dgemm_("T", "N", &m1, n, &m2, &c_b23, &a[m1], m, &
				    b[m1], ldb, alpha, &b[b_offset], ldb);
			    dtrsm_("L", "L", "T", diag, &m1, n, &c_b27, a, m, 
				    &b[b_offset], ldb);
			}

		    }

		} else {

/*                 SIDE  ='L', N is odd, TRANSR = 'N', and UPLO = 'U' */

		    if (! notrans) {

/*                    SIDE  ='L', N is odd, TRANSR = 'N', UPLO = 'U', and */
/*                    TRANS = 'N' */

			dtrsm_("L", "L", "N", diag, &m1, n, alpha, &a[m2], m, 
				&b[b_offset], ldb);
			dgemm_("T", "N", &m2, n, &m1, &c_b23, a, m, &b[
				b_offset], ldb, alpha, &b[m1], ldb);
			dtrsm_("L", "U", "T", diag, &m2, n, &c_b27, &a[m1], m,
				 &b[m1], ldb);

		    } else {

/*                    SIDE  ='L', N is odd, TRANSR = 'N', UPLO = 'U', and */
/*                    TRANS = 'T' */

			dtrsm_("L", "U", "N", diag, &m2, n, alpha, &a[m1], m, 
				&b[m1], ldb);
			dgemm_("N", "N", &m1, n, &m2, &c_b23, a, m, &b[m1], 
				ldb, alpha, &b[b_offset], ldb);
			dtrsm_("L", "L", "T", diag, &m1, n, &c_b27, &a[m2], m,
				 &b[b_offset], ldb);

		    }

		}

	    } else {

/*              SIDE = 'L', N is odd, and TRANSR = 'T' */

		if (lower) {

/*                 SIDE  ='L', N is odd, TRANSR = 'T', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='L', N is odd, TRANSR = 'T', UPLO = 'L', and */
/*                    TRANS = 'N' */

			if (*m == 1) {
			    dtrsm_("L", "U", "T", diag, &m1, n, alpha, a, &m1,
				     &b[b_offset], ldb);
			} else {
			    dtrsm_("L", "U", "T", diag, &m1, n, alpha, a, &m1,
				     &b[b_offset], ldb);
			    dgemm_("T", "N", &m2, n, &m1, &c_b23, &a[m1 * m1],
				     &m1, &b[b_offset], ldb, alpha, &b[m1], 
				    ldb);
			    dtrsm_("L", "L", "N", diag, &m2, n, &c_b27, &a[1],
				     &m1, &b[m1], ldb);
			}

		    } else {

/*                    SIDE  ='L', N is odd, TRANSR = 'T', UPLO = 'L', and */
/*                    TRANS = 'T' */

			if (*m == 1) {
			    dtrsm_("L", "U", "N", diag, &m1, n, alpha, a, &m1,
				     &b[b_offset], ldb);
			} else {
			    dtrsm_("L", "L", "T", diag, &m2, n, alpha, &a[1], 
				    &m1, &b[m1], ldb);
			    dgemm_("N", "N", &m1, n, &m2, &c_b23, &a[m1 * m1],
				     &m1, &b[m1], ldb, alpha, &b[b_offset], 
				    ldb);
			    dtrsm_("L", "U", "N", diag, &m1, n, &c_b27, a, &
				    m1, &b[b_offset], ldb);
			}

		    }

		} else {

/*                 SIDE  ='L', N is odd, TRANSR = 'T', and UPLO = 'U' */

		    if (! notrans) {

/*                    SIDE  ='L', N is odd, TRANSR = 'T', UPLO = 'U', and */
/*                    TRANS = 'N' */

			dtrsm_("L", "U", "T", diag, &m1, n, alpha, &a[m2 * m2]
				, &m2, &b[b_offset], ldb);
			dgemm_("N", "N", &m2, n, &m1, &c_b23, a, &m2, &b[
				b_offset], ldb, alpha, &b[m1], ldb);
			dtrsm_("L", "L", "N", diag, &m2, n, &c_b27, &a[m1 * 
				m2], &m2, &b[m1], ldb);

		    } else {

/*                    SIDE  ='L', N is odd, TRANSR = 'T', UPLO = 'U', and */
/*                    TRANS = 'T' */

			dtrsm_("L", "L", "T", diag, &m2, n, alpha, &a[m1 * m2]
				, &m2, &b[m1], ldb);
			dgemm_("T", "N", &m1, n, &m2, &c_b23, a, &m2, &b[m1], 
				ldb, alpha, &b[b_offset], ldb);
			dtrsm_("L", "U", "N", diag, &m1, n, &c_b27, &a[m2 * 
				m2], &m2, &b[b_offset], ldb);

		    }

		}

	    }

	} else {

/*           SIDE = 'L' and N is even */

	    if (normaltransr) {

/*              SIDE = 'L', N is even, and TRANSR = 'N' */

		if (lower) {

/*                 SIDE  ='L', N is even, TRANSR = 'N', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='L', N is even, TRANSR = 'N', UPLO = 'L', */
/*                    and TRANS = 'N' */

			i__1 = *m + 1;
			dtrsm_("L", "L", "N", diag, &k, n, alpha, &a[1], &
				i__1, &b[b_offset], ldb);
			i__1 = *m + 1;
			dgemm_("N", "N", &k, n, &k, &c_b23, &a[k + 1], &i__1, 
				&b[b_offset], ldb, alpha, &b[k], ldb);
			i__1 = *m + 1;
			dtrsm_("L", "U", "T", diag, &k, n, &c_b27, a, &i__1, &
				b[k], ldb);

		    } else {

/*                    SIDE  ='L', N is even, TRANSR = 'N', UPLO = 'L', */
/*                    and TRANS = 'T' */

			i__1 = *m + 1;
			dtrsm_("L", "U", "N", diag, &k, n, alpha, a, &i__1, &
				b[k], ldb);
			i__1 = *m + 1;
			dgemm_("T", "N", &k, n, &k, &c_b23, &a[k + 1], &i__1, 
				&b[k], ldb, alpha, &b[b_offset], ldb);
			i__1 = *m + 1;
			dtrsm_("L", "L", "T", diag, &k, n, &c_b27, &a[1], &
				i__1, &b[b_offset], ldb);

		    }

		} else {

/*                 SIDE  ='L', N is even, TRANSR = 'N', and UPLO = 'U' */

		    if (! notrans) {

/*                    SIDE  ='L', N is even, TRANSR = 'N', UPLO = 'U', */
/*                    and TRANS = 'N' */

			i__1 = *m + 1;
			dtrsm_("L", "L", "N", diag, &k, n, alpha, &a[k + 1], &
				i__1, &b[b_offset], ldb);
			i__1 = *m + 1;
			dgemm_("T", "N", &k, n, &k, &c_b23, a, &i__1, &b[
				b_offset], ldb, alpha, &b[k], ldb);
			i__1 = *m + 1;
			dtrsm_("L", "U", "T", diag, &k, n, &c_b27, &a[k], &
				i__1, &b[k], ldb);

		    } else {

/*                    SIDE  ='L', N is even, TRANSR = 'N', UPLO = 'U', */
/*                    and TRANS = 'T' */
			i__1 = *m + 1;
			dtrsm_("L", "U", "N", diag, &k, n, alpha, &a[k], &
				i__1, &b[k], ldb);
			i__1 = *m + 1;
			dgemm_("N", "N", &k, n, &k, &c_b23, a, &i__1, &b[k], 
				ldb, alpha, &b[b_offset], ldb);
			i__1 = *m + 1;
			dtrsm_("L", "L", "T", diag, &k, n, &c_b27, &a[k + 1], 
				&i__1, &b[b_offset], ldb);

		    }

		}

	    } else {

/*              SIDE = 'L', N is even, and TRANSR = 'T' */

		if (lower) {

/*                 SIDE  ='L', N is even, TRANSR = 'T', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='L', N is even, TRANSR = 'T', UPLO = 'L', */
/*                    and TRANS = 'N' */

			dtrsm_("L", "U", "T", diag, &k, n, alpha, &a[k], &k, &
				b[b_offset], ldb);
			dgemm_("T", "N", &k, n, &k, &c_b23, &a[k * (k + 1)], &
				k, &b[b_offset], ldb, alpha, &b[k], ldb);
			dtrsm_("L", "L", "N", diag, &k, n, &c_b27, a, &k, &b[
				k], ldb);

		    } else {

/*                    SIDE  ='L', N is even, TRANSR = 'T', UPLO = 'L', */
/*                    and TRANS = 'T' */

			dtrsm_("L", "L", "T", diag, &k, n, alpha, a, &k, &b[k]
				, ldb);
			dgemm_("N", "N", &k, n, &k, &c_b23, &a[k * (k + 1)], &
				k, &b[k], ldb, alpha, &b[b_offset], ldb);
			dtrsm_("L", "U", "N", diag, &k, n, &c_b27, &a[k], &k, 
				&b[b_offset], ldb);

		    }

		} else {

/*                 SIDE  ='L', N is even, TRANSR = 'T', and UPLO = 'U' */

		    if (! notrans) {

/*                    SIDE  ='L', N is even, TRANSR = 'T', UPLO = 'U', */
/*                    and TRANS = 'N' */

			dtrsm_("L", "U", "T", diag, &k, n, alpha, &a[k * (k + 
				1)], &k, &b[b_offset], ldb);
			dgemm_("N", "N", &k, n, &k, &c_b23, a, &k, &b[
				b_offset], ldb, alpha, &b[k], ldb);
			dtrsm_("L", "L", "N", diag, &k, n, &c_b27, &a[k * k], 
				&k, &b[k], ldb);

		    } else {

/*                    SIDE  ='L', N is even, TRANSR = 'T', UPLO = 'U', */
/*                    and TRANS = 'T' */

			dtrsm_("L", "L", "T", diag, &k, n, alpha, &a[k * k], &
				k, &b[k], ldb);
			dgemm_("T", "N", &k, n, &k, &c_b23, a, &k, &b[k], ldb,
				 alpha, &b[b_offset], ldb);
			dtrsm_("L", "U", "N", diag, &k, n, &c_b27, &a[k * (k 
				+ 1)], &k, &b[b_offset], ldb);

		    }

		}

	    }

	}

    } else {

/*        SIDE = 'R' */

/*        A is N-by-N. */
/*        If N is odd, set NISODD = .TRUE., and N1 and N2. */
/*        If N is even, NISODD = .FALSE., and K. */

	if (*n % 2 == 0) {
	    nisodd = FALSE_;
	    k = *n / 2;
	} else {
	    nisodd = TRUE_;
	    if (lower) {
		n2 = *n / 2;
		n1 = *n - n2;
	    } else {
		n1 = *n / 2;
		n2 = *n - n1;
	    }
	}

	if (nisodd) {

/*           SIDE = 'R' and N is odd */

	    if (normaltransr) {

/*              SIDE = 'R', N is odd, and TRANSR = 'N' */

		if (lower) {

/*                 SIDE  ='R', N is odd, TRANSR = 'N', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='R', N is odd, TRANSR = 'N', UPLO = 'L', and */
/*                    TRANS = 'N' */

			dtrsm_("R", "U", "T", diag, m, &n2, alpha, &a[*n], n, 
				&b[n1 * b_dim1], ldb);
			dgemm_("N", "N", m, &n1, &n2, &c_b23, &b[n1 * b_dim1],
				 ldb, &a[n1], n, alpha, b, ldb);
			dtrsm_("R", "L", "N", diag, m, &n1, &c_b27, a, n, b, 
				ldb);

		    } else {

/*                    SIDE  ='R', N is odd, TRANSR = 'N', UPLO = 'L', and */
/*                    TRANS = 'T' */

			dtrsm_("R", "L", "T", diag, m, &n1, alpha, a, n, b, 
				ldb);
			dgemm_("N", "T", m, &n2, &n1, &c_b23, b, ldb, &a[n1], 
				n, alpha, &b[n1 * b_dim1], ldb);
			dtrsm_("R", "U", "N", diag, m, &n2, &c_b27, &a[*n], n,
				 &b[n1 * b_dim1], ldb);

		    }

		} else {

/*                 SIDE  ='R', N is odd, TRANSR = 'N', and UPLO = 'U' */

		    if (notrans) {

/*                    SIDE  ='R', N is odd, TRANSR = 'N', UPLO = 'U', and */
/*                    TRANS = 'N' */

			dtrsm_("R", "L", "T", diag, m, &n1, alpha, &a[n2], n, 
				b, ldb);
			dgemm_("N", "N", m, &n2, &n1, &c_b23, b, ldb, a, n, 
				alpha, &b[n1 * b_dim1], ldb);
			dtrsm_("R", "U", "N", diag, m, &n2, &c_b27, &a[n1], n,
				 &b[n1 * b_dim1], ldb);

		    } else {

/*                    SIDE  ='R', N is odd, TRANSR = 'N', UPLO = 'U', and */
/*                    TRANS = 'T' */

			dtrsm_("R", "U", "T", diag, m, &n2, alpha, &a[n1], n, 
				&b[n1 * b_dim1], ldb);
			dgemm_("N", "T", m, &n1, &n2, &c_b23, &b[n1 * b_dim1],
				 ldb, a, n, alpha, b, ldb);
			dtrsm_("R", "L", "N", diag, m, &n1, &c_b27, &a[n2], n,
				 b, ldb);

		    }

		}

	    } else {

/*              SIDE = 'R', N is odd, and TRANSR = 'T' */

		if (lower) {

/*                 SIDE  ='R', N is odd, TRANSR = 'T', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='R', N is odd, TRANSR = 'T', UPLO = 'L', and */
/*                    TRANS = 'N' */

			dtrsm_("R", "L", "N", diag, m, &n2, alpha, &a[1], &n1,
				 &b[n1 * b_dim1], ldb);
			dgemm_("N", "T", m, &n1, &n2, &c_b23, &b[n1 * b_dim1],
				 ldb, &a[n1 * n1], &n1, alpha, b, ldb);
			dtrsm_("R", "U", "T", diag, m, &n1, &c_b27, a, &n1, b,
				 ldb);

		    } else {

/*                    SIDE  ='R', N is odd, TRANSR = 'T', UPLO = 'L', and */
/*                    TRANS = 'T' */

			dtrsm_("R", "U", "N", diag, m, &n1, alpha, a, &n1, b, 
				ldb);
			dgemm_("N", "N", m, &n2, &n1, &c_b23, b, ldb, &a[n1 * 
				n1], &n1, alpha, &b[n1 * b_dim1], ldb);
			dtrsm_("R", "L", "T", diag, m, &n2, &c_b27, &a[1], &
				n1, &b[n1 * b_dim1], ldb);

		    }

		} else {

/*                 SIDE  ='R', N is odd, TRANSR = 'T', and UPLO = 'U' */

		    if (notrans) {

/*                    SIDE  ='R', N is odd, TRANSR = 'T', UPLO = 'U', and */
/*                    TRANS = 'N' */

			dtrsm_("R", "U", "N", diag, m, &n1, alpha, &a[n2 * n2]
				, &n2, b, ldb);
			dgemm_("N", "T", m, &n2, &n1, &c_b23, b, ldb, a, &n2, 
				alpha, &b[n1 * b_dim1], ldb);
			dtrsm_("R", "L", "T", diag, m, &n2, &c_b27, &a[n1 * 
				n2], &n2, &b[n1 * b_dim1], ldb);

		    } else {

/*                    SIDE  ='R', N is odd, TRANSR = 'T', UPLO = 'U', and */
/*                    TRANS = 'T' */

			dtrsm_("R", "L", "N", diag, m, &n2, alpha, &a[n1 * n2]
				, &n2, &b[n1 * b_dim1], ldb);
			dgemm_("N", "N", m, &n1, &n2, &c_b23, &b[n1 * b_dim1],
				 ldb, a, &n2, alpha, b, ldb);
			dtrsm_("R", "U", "T", diag, m, &n1, &c_b27, &a[n2 * 
				n2], &n2, b, ldb);

		    }

		}

	    }

	} else {

/*           SIDE = 'R' and N is even */

	    if (normaltransr) {

/*              SIDE = 'R', N is even, and TRANSR = 'N' */

		if (lower) {

/*                 SIDE  ='R', N is even, TRANSR = 'N', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='R', N is even, TRANSR = 'N', UPLO = 'L', */
/*                    and TRANS = 'N' */

			i__1 = *n + 1;
			dtrsm_("R", "U", "T", diag, m, &k, alpha, a, &i__1, &
				b[k * b_dim1], ldb);
			i__1 = *n + 1;
			dgemm_("N", "N", m, &k, &k, &c_b23, &b[k * b_dim1], 
				ldb, &a[k + 1], &i__1, alpha, b, ldb);
			i__1 = *n + 1;
			dtrsm_("R", "L", "N", diag, m, &k, &c_b27, &a[1], &
				i__1, b, ldb);

		    } else {

/*                    SIDE  ='R', N is even, TRANSR = 'N', UPLO = 'L', */
/*                    and TRANS = 'T' */

			i__1 = *n + 1;
			dtrsm_("R", "L", "T", diag, m, &k, alpha, &a[1], &
				i__1, b, ldb);
			i__1 = *n + 1;
			dgemm_("N", "T", m, &k, &k, &c_b23, b, ldb, &a[k + 1],
				 &i__1, alpha, &b[k * b_dim1], ldb);
			i__1 = *n + 1;
			dtrsm_("R", "U", "N", diag, m, &k, &c_b27, a, &i__1, &
				b[k * b_dim1], ldb);

		    }

		} else {

/*                 SIDE  ='R', N is even, TRANSR = 'N', and UPLO = 'U' */

		    if (notrans) {

/*                    SIDE  ='R', N is even, TRANSR = 'N', UPLO = 'U', */
/*                    and TRANS = 'N' */

			i__1 = *n + 1;
			dtrsm_("R", "L", "T", diag, m, &k, alpha, &a[k + 1], &
				i__1, b, ldb);
			i__1 = *n + 1;
			dgemm_("N", "N", m, &k, &k, &c_b23, b, ldb, a, &i__1, 
				alpha, &b[k * b_dim1], ldb);
			i__1 = *n + 1;
			dtrsm_("R", "U", "N", diag, m, &k, &c_b27, &a[k], &
				i__1, &b[k * b_dim1], ldb);

		    } else {

/*                    SIDE  ='R', N is even, TRANSR = 'N', UPLO = 'U', */
/*                    and TRANS = 'T' */

			i__1 = *n + 1;
			dtrsm_("R", "U", "T", diag, m, &k, alpha, &a[k], &
				i__1, &b[k * b_dim1], ldb);
			i__1 = *n + 1;
			dgemm_("N", "T", m, &k, &k, &c_b23, &b[k * b_dim1], 
				ldb, a, &i__1, alpha, b, ldb);
			i__1 = *n + 1;
			dtrsm_("R", "L", "N", diag, m, &k, &c_b27, &a[k + 1], 
				&i__1, b, ldb);

		    }

		}

	    } else {

/*              SIDE = 'R', N is even, and TRANSR = 'T' */

		if (lower) {

/*                 SIDE  ='R', N is even, TRANSR = 'T', and UPLO = 'L' */

		    if (notrans) {

/*                    SIDE  ='R', N is even, TRANSR = 'T', UPLO = 'L', */
/*                    and TRANS = 'N' */

			dtrsm_("R", "L", "N", diag, m, &k, alpha, a, &k, &b[k 
				* b_dim1], ldb);
			dgemm_("N", "T", m, &k, &k, &c_b23, &b[k * b_dim1], 
				ldb, &a[(k + 1) * k], &k, alpha, b, ldb);
			dtrsm_("R", "U", "T", diag, m, &k, &c_b27, &a[k], &k, 
				b, ldb);

		    } else {

/*                    SIDE  ='R', N is even, TRANSR = 'T', UPLO = 'L', */
/*                    and TRANS = 'T' */

			dtrsm_("R", "U", "N", diag, m, &k, alpha, &a[k], &k, 
				b, ldb);
			dgemm_("N", "N", m, &k, &k, &c_b23, b, ldb, &a[(k + 1)
				 * k], &k, alpha, &b[k * b_dim1], ldb);
			dtrsm_("R", "L", "T", diag, m, &k, &c_b27, a, &k, &b[
				k * b_dim1], ldb);

		    }

		} else {

/*                 SIDE  ='R', N is even, TRANSR = 'T', and UPLO = 'U' */

		    if (notrans) {

/*                    SIDE  ='R', N is even, TRANSR = 'T', UPLO = 'U', */
/*                    and TRANS = 'N' */

			dtrsm_("R", "U", "N", diag, m, &k, alpha, &a[(k + 1) *
				 k], &k, b, ldb);
			dgemm_("N", "T", m, &k, &k, &c_b23, b, ldb, a, &k, 
				alpha, &b[k * b_dim1], ldb);
			dtrsm_("R", "L", "T", diag, m, &k, &c_b27, &a[k * k], 
				&k, &b[k * b_dim1], ldb);

		    } else {

/*                    SIDE  ='R', N is even, TRANSR = 'T', UPLO = 'U', */
/*                    and TRANS = 'T' */

			dtrsm_("R", "L", "N", diag, m, &k, alpha, &a[k * k], &
				k, &b[k * b_dim1], ldb);
			dgemm_("N", "N", m, &k, &k, &c_b23, &b[k * b_dim1], 
				ldb, a, &k, alpha, b, ldb);
			dtrsm_("R", "U", "T", diag, m, &k, &c_b27, &a[(k + 1) 
				* k], &k, b, ldb);

		    }

		}

	    }

	}
    }

    return;

/*     End of DTFSM */

} /* dtfsm_ */


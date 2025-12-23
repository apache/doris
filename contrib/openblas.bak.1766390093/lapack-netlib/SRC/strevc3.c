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
static integer c_n1 = -1;
static integer c__2 = 2;
static real c_b17 = 0.f;
static logical c_false = FALSE_;
static real c_b29 = 1.f;
static logical c_true = TRUE_;

/* > \brief \b STREVC3 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download STREVC3 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/strevc3
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/strevc3
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/strevc3
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE STREVC3( SIDE, HOWMNY, SELECT, N, T, LDT, VL, LDVL, */
/*                           VR, LDVR, MM, M, WORK, LWORK, INFO ) */

/*       CHARACTER          HOWMNY, SIDE */
/*       INTEGER            INFO, LDT, LDVL, LDVR, LWORK, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       REAL               T( LDT, * ), VL( LDVL, * ), VR( LDVR, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > STREVC3 computes some or all of the right and/or left eigenvectors of */
/* > a real upper quasi-triangular matrix T. */
/* > Matrices of this type are produced by the Schur factorization of */
/* > a real general matrix:  A = Q*T*Q**T, as computed by SHSEQR. */
/* > */
/* > The right eigenvector x and the left eigenvector y of T corresponding */
/* > to an eigenvalue w are defined by: */
/* > */
/* >    T*x = w*x,     (y**T)*T = w*(y**T) */
/* > */
/* > where y**T denotes the transpose of the vector y. */
/* > The eigenvalues are not input to this routine, but are read directly */
/* > from the diagonal blocks of T. */
/* > */
/* > This routine returns the matrices X and/or Y of right and left */
/* > eigenvectors of T, or the products Q*X and/or Q*Y, where Q is an */
/* > input matrix. If Q is the orthogonal factor that reduces a matrix */
/* > A to Schur form T, then Q*X and Q*Y are the matrices of right and */
/* > left eigenvectors of A. */
/* > */
/* > This uses a Level 3 BLAS version of the back transformation. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >          = 'R':  compute right eigenvectors only; */
/* >          = 'L':  compute left eigenvectors only; */
/* >          = 'B':  compute both right and left eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] HOWMNY */
/* > \verbatim */
/* >          HOWMNY is CHARACTER*1 */
/* >          = 'A':  compute all right and/or left eigenvectors; */
/* >          = 'B':  compute all right and/or left eigenvectors, */
/* >                  backtransformed by the matrices in VR and/or VL; */
/* >          = 'S':  compute selected right and/or left eigenvectors, */
/* >                  as indicated by the logical array SELECT. */
/* > \endverbatim */
/* > */
/* > \param[in,out] SELECT */
/* > \verbatim */
/* >          SELECT is LOGICAL array, dimension (N) */
/* >          If HOWMNY = 'S', SELECT specifies the eigenvectors to be */
/* >          computed. */
/* >          If w(j) is a real eigenvalue, the corresponding real */
/* >          eigenvector is computed if SELECT(j) is .TRUE.. */
/* >          If w(j) and w(j+1) are the real and imaginary parts of a */
/* >          complex eigenvalue, the corresponding complex eigenvector is */
/* >          computed if either SELECT(j) or SELECT(j+1) is .TRUE., and */
/* >          on exit SELECT(j) is set to .TRUE. and SELECT(j+1) is set to */
/* >          .FALSE.. */
/* >          Not referenced if HOWMNY = 'A' or 'B'. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix T. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] T */
/* > \verbatim */
/* >          T is REAL array, dimension (LDT,N) */
/* >          The upper quasi-triangular matrix T in Schur canonical form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T. LDT >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] VL */
/* > \verbatim */
/* >          VL is REAL array, dimension (LDVL,MM) */
/* >          On entry, if SIDE = 'L' or 'B' and HOWMNY = 'B', VL must */
/* >          contain an N-by-N matrix Q (usually the orthogonal matrix Q */
/* >          of Schur vectors returned by SHSEQR). */
/* >          On exit, if SIDE = 'L' or 'B', VL contains: */
/* >          if HOWMNY = 'A', the matrix Y of left eigenvectors of T; */
/* >          if HOWMNY = 'B', the matrix Q*Y; */
/* >          if HOWMNY = 'S', the left eigenvectors of T specified by */
/* >                           SELECT, stored consecutively in the columns */
/* >                           of VL, in the same order as their */
/* >                           eigenvalues. */
/* >          A complex eigenvector corresponding to a complex eigenvalue */
/* >          is stored in two consecutive columns, the first holding the */
/* >          real part, and the second the imaginary part. */
/* >          Not referenced if SIDE = 'R'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of the array VL. */
/* >          LDVL >= 1, and if SIDE = 'L' or 'B', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VR */
/* > \verbatim */
/* >          VR is REAL array, dimension (LDVR,MM) */
/* >          On entry, if SIDE = 'R' or 'B' and HOWMNY = 'B', VR must */
/* >          contain an N-by-N matrix Q (usually the orthogonal matrix Q */
/* >          of Schur vectors returned by SHSEQR). */
/* >          On exit, if SIDE = 'R' or 'B', VR contains: */
/* >          if HOWMNY = 'A', the matrix X of right eigenvectors of T; */
/* >          if HOWMNY = 'B', the matrix Q*X; */
/* >          if HOWMNY = 'S', the right eigenvectors of T specified by */
/* >                           SELECT, stored consecutively in the columns */
/* >                           of VR, in the same order as their */
/* >                           eigenvalues. */
/* >          A complex eigenvector corresponding to a complex eigenvalue */
/* >          is stored in two consecutive columns, the first holding the */
/* >          real part and the second the imaginary part. */
/* >          Not referenced if SIDE = 'L'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the array VR. */
/* >          LDVR >= 1, and if SIDE = 'R' or 'B', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] MM */
/* > \verbatim */
/* >          MM is INTEGER */
/* >          The number of columns in the arrays VL and/or VR. MM >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of columns in the arrays VL and/or VR actually */
/* >          used to store the eigenvectors. */
/* >          If HOWMNY = 'A' or 'B', M is set to N. */
/* >          Each selected real eigenvector occupies one column and each */
/* >          selected complex eigenvector occupies two columns. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)) */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of array WORK. LWORK >= f2cmax(1,3*N). */
/* >          For optimum performance, LWORK >= N + 2*N*NB, where NB is */
/* >          the optimal blocksize. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2017 */

/*  @generated from dtrevc3.f, fortran d -> s, Tue Apr 19 01:47:44 2016 */

/* > \ingroup realOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The algorithm used in this program is basically backward (forward) */
/* >  substitution, with scaling to make the the code robust against */
/* >  possible overflow. */
/* > */
/* >  Each eigenvector is normalized so that the element of largest */
/* >  magnitude has magnitude 1; here the magnitude of a complex number */
/* >  (x,y) is taken to be |x| + |y|. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void strevc3_(char *side, char *howmny, logical *select, 
	integer *n, real *t, integer *ldt, real *vl, integer *ldvl, real *vr, 
	integer *ldvr, integer *mm, integer *m, real *work, integer *lwork, 
	integer *info)
{
    /* System generated locals */
    address a__1[2];
    integer t_dim1, t_offset, vl_dim1, vl_offset, vr_dim1, vr_offset, i__1[2],
	     i__2, i__3, i__4;
    real r__1, r__2, r__3, r__4;
    char ch__1[2];

    /* Local variables */
    real beta, emax;
    logical pair, allv;
    integer ierr;
    real unfl, ovfl, smin;
    extern real sdot_(integer *, real *, integer *, real *, integer *);
    logical over;
    real vmax;
    integer jnxt, i__, j, k;
    real scale, x[4]	/* was [2][2] */;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *), 
	    sgemm_(char *, char *, integer *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *);
    real remax;
    logical leftv;
    extern /* Subroutine */ void sgemv_(char *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *);
    logical bothv;
    real vcrit;
    logical somev;
    integer j1, j2;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *);
    real xnorm;
    extern /* Subroutine */ void saxpy_(integer *, real *, real *, integer *, 
	    real *, integer *);
    integer iscomplex[128];
    extern /* Subroutine */ void slaln2_(logical *, integer *, integer *, real 
	    *, real *, real *, integer *, real *, real *, real *, integer *, 
	    real *, real *, real *, integer *, real *, real *, integer *);
    integer nb, ii, ki;
    extern /* Subroutine */ void slabad_(real *, real *);
    integer ip, is, iv;
    real wi;
    extern real slamch_(char *);
    real wr;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    real bignum;
    extern integer isamax_(integer *, real *, integer *);
    extern /* Subroutine */ void slacpy_(char *, integer *, integer *, real *, 
	    integer *, real *, integer *), slaset_(char *, integer *, 
	    integer *, real *, real *, real *, integer *);
    logical rightv;
    integer ki2, maxwrk;
    real smlnum;
    logical lquery;
    real rec, ulp;


/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */


/*  ===================================================================== */


/*     Decode and test the input parameters */

    /* Parameter adjustments */
    --select;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --work;

    /* Function Body */
    bothv = lsame_(side, "B");
    rightv = lsame_(side, "R") || bothv;
    leftv = lsame_(side, "L") || bothv;

    allv = lsame_(howmny, "A");
    over = lsame_(howmny, "B");
    somev = lsame_(howmny, "S");

    *info = 0;
/* Writing concatenation */
    i__1[0] = 1, a__1[0] = side;
    i__1[1] = 1, a__1[1] = howmny;
    s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
    nb = ilaenv_(&c__1, "STREVC", ch__1, n, &c_n1, &c_n1, &c_n1, (ftnlen)6, (
	    ftnlen)2);
    maxwrk = *n + (*n << 1) * nb;
    work[1] = (real) maxwrk;
    lquery = *lwork == -1;
    if (! rightv && ! leftv) {
	*info = -1;
    } else if (! allv && ! over && ! somev) {
	*info = -2;
    } else if (*n < 0) {
	*info = -4;
    } else if (*ldt < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldvl < 1 || leftv && *ldvl < *n) {
	*info = -8;
    } else if (*ldvr < 1 || rightv && *ldvr < *n) {
	*info = -10;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__2 = 1, i__3 = *n * 3;
	if (*lwork < f2cmax(i__2,i__3) && ! lquery) {
	    *info = -14;
	} else {

/*        Set M to the number of columns required to store the selected */
/*        eigenvectors, standardize the array SELECT if necessary, and */
/*        test MM. */

	    if (somev) {
		*m = 0;
		pair = FALSE_;
		i__2 = *n;
		for (j = 1; j <= i__2; ++j) {
		    if (pair) {
			pair = FALSE_;
			select[j] = FALSE_;
		    } else {
			if (j < *n) {
			    if (t[j + 1 + j * t_dim1] == 0.f) {
				if (select[j]) {
				    ++(*m);
				}
			    } else {
				pair = TRUE_;
				if (select[j] || select[j + 1]) {
				    select[j] = TRUE_;
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

	    if (*mm < *m) {
		*info = -11;
	    }
	}
    }
    if (*info != 0) {
	i__2 = -(*info);
	xerbla_("STREVC3", &i__2, (ftnlen)7);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible. */

    if (*n == 0) {
	return;
    }

/*     Use blocked version of back-transformation if sufficient workspace. */
/*     Zero-out the workspace to avoid potential NaN propagation. */

    if (over && *lwork >= *n + (*n << 4)) {
	nb = (*lwork - *n) / (*n << 1);
	nb = f2cmin(nb,128);
	i__2 = (nb << 1) + 1;
	slaset_("F", n, &i__2, &c_b17, &c_b17, &work[1], n);
    } else {
	nb = 1;
    }

/*     Set the constants to control overflow. */

    unfl = slamch_("Safe minimum");
    ovfl = 1.f / unfl;
    slabad_(&unfl, &ovfl);
    ulp = slamch_("Precision");
    smlnum = unfl * (*n / ulp);
    bignum = (1.f - ulp) / smlnum;

/*     Compute 1-norm of each column of strictly upper triangular */
/*     part of T to control overflow in triangular solver. */

    work[1] = 0.f;
    i__2 = *n;
    for (j = 2; j <= i__2; ++j) {
	work[j] = 0.f;
	i__3 = j - 1;
	for (i__ = 1; i__ <= i__3; ++i__) {
	    work[j] += (r__1 = t[i__ + j * t_dim1], abs(r__1));
/* L20: */
	}
/* L30: */
    }

/*     Index IP is used to specify the real or complex eigenvalue: */
/*       IP = 0, real eigenvalue, */
/*            1, first  of conjugate complex pair: (wr,wi) */
/*           -1, second of conjugate complex pair: (wr,wi) */
/*       ISCOMPLEX array stores IP for each column in current block. */

    if (rightv) {

/*        ============================================================ */
/*        Compute right eigenvectors. */

/*        IV is index of column in current block. */
/*        For complex right vector, uses IV-1 for real part and IV for complex part. */
/*        Non-blocked version always uses IV=2; */
/*        blocked     version starts with IV=NB, goes down to 1 or 2. */
/*        (Note the "0-th" column is used for 1-norms computed above.) */
	iv = 2;
	if (nb > 2) {
	    iv = nb;
	}
	ip = 0;
	is = *m;
	for (ki = *n; ki >= 1; --ki) {
	    if (ip == -1) {
/*              previous iteration (ki+1) was second of conjugate pair, */
/*              so this ki is first of conjugate pair; skip to end of loop */
		ip = 1;
		goto L140;
	    } else if (ki == 1) {
/*              last column, so this ki must be real eigenvalue */
		ip = 0;
	    } else if (t[ki + (ki - 1) * t_dim1] == 0.f) {
/*              zero on sub-diagonal, so this ki is real eigenvalue */
		ip = 0;
	    } else {
/*              non-zero on sub-diagonal, so this ki is second of conjugate pair */
		ip = -1;
	    }
	    if (somev) {
		if (ip == 0) {
		    if (! select[ki]) {
			goto L140;
		    }
		} else {
		    if (! select[ki - 1]) {
			goto L140;
		    }
		}
	    }

/*           Compute the KI-th eigenvalue (WR,WI). */

	    wr = t[ki + ki * t_dim1];
	    wi = 0.f;
	    if (ip != 0) {
		wi = sqrt((r__1 = t[ki + (ki - 1) * t_dim1], abs(r__1))) * 
			sqrt((r__2 = t[ki - 1 + ki * t_dim1], abs(r__2)));
	    }
/* Computing MAX */
	    r__1 = ulp * (abs(wr) + abs(wi));
	    smin = f2cmax(r__1,smlnum);

	    if (ip == 0) {

/*              -------------------------------------------------------- */
/*              Real right eigenvector */

		work[ki + iv * *n] = 1.f;

/*              Form right-hand side. */

		i__2 = ki - 1;
		for (k = 1; k <= i__2; ++k) {
		    work[k + iv * *n] = -t[k + ki * t_dim1];
/* L50: */
		}

/*              Solve upper quasi-triangular system: */
/*              [ T(1:KI-1,1:KI-1) - WR ]*X = SCALE*WORK. */

		jnxt = ki - 1;
		for (j = ki - 1; j >= 1; --j) {
		    if (j > jnxt) {
			goto L60;
		    }
		    j1 = j;
		    j2 = j;
		    jnxt = j - 1;
		    if (j > 1) {
			if (t[j + (j - 1) * t_dim1] != 0.f) {
			    j1 = j - 1;
			    jnxt = j - 2;
			}
		    }

		    if (j1 == j2) {

/*                    1-by-1 diagonal block */

			slaln2_(&c_false, &c__1, &c__1, &smin, &c_b29, &t[j + 
				j * t_dim1], ldt, &c_b29, &c_b29, &work[j + 
				iv * *n], n, &wr, &c_b17, x, &c__2, &scale, &
				xnorm, &ierr);

/*                    Scale X(1,1) to avoid overflow when updating */
/*                    the right-hand side. */

			if (xnorm > 1.f) {
			    if (work[j] > bignum / xnorm) {
				x[0] /= xnorm;
				scale /= xnorm;
			    }
			}

/*                    Scale if necessary */

			if (scale != 1.f) {
			    sscal_(&ki, &scale, &work[iv * *n + 1], &c__1);
			}
			work[j + iv * *n] = x[0];

/*                    Update right-hand side */

			i__2 = j - 1;
			r__1 = -x[0];
			saxpy_(&i__2, &r__1, &t[j * t_dim1 + 1], &c__1, &work[
				iv * *n + 1], &c__1);

		    } else {

/*                    2-by-2 diagonal block */

			slaln2_(&c_false, &c__2, &c__1, &smin, &c_b29, &t[j - 
				1 + (j - 1) * t_dim1], ldt, &c_b29, &c_b29, &
				work[j - 1 + iv * *n], n, &wr, &c_b17, x, &
				c__2, &scale, &xnorm, &ierr);

/*                    Scale X(1,1) and X(2,1) to avoid overflow when */
/*                    updating the right-hand side. */

			if (xnorm > 1.f) {
/* Computing MAX */
			    r__1 = work[j - 1], r__2 = work[j];
			    beta = f2cmax(r__1,r__2);
			    if (beta > bignum / xnorm) {
				x[0] /= xnorm;
				x[1] /= xnorm;
				scale /= xnorm;
			    }
			}

/*                    Scale if necessary */

			if (scale != 1.f) {
			    sscal_(&ki, &scale, &work[iv * *n + 1], &c__1);
			}
			work[j - 1 + iv * *n] = x[0];
			work[j + iv * *n] = x[1];

/*                    Update right-hand side */

			i__2 = j - 2;
			r__1 = -x[0];
			saxpy_(&i__2, &r__1, &t[(j - 1) * t_dim1 + 1], &c__1, 
				&work[iv * *n + 1], &c__1);
			i__2 = j - 2;
			r__1 = -x[1];
			saxpy_(&i__2, &r__1, &t[j * t_dim1 + 1], &c__1, &work[
				iv * *n + 1], &c__1);
		    }
L60:
		    ;
		}

/*              Copy the vector x or Q*x to VR and normalize. */

		if (! over) {
/*                 ------------------------------ */
/*                 no back-transform: copy x to VR and normalize. */
		    scopy_(&ki, &work[iv * *n + 1], &c__1, &vr[is * vr_dim1 + 
			    1], &c__1);

		    ii = isamax_(&ki, &vr[is * vr_dim1 + 1], &c__1);
		    remax = 1.f / (r__1 = vr[ii + is * vr_dim1], abs(r__1));
		    sscal_(&ki, &remax, &vr[is * vr_dim1 + 1], &c__1);

		    i__2 = *n;
		    for (k = ki + 1; k <= i__2; ++k) {
			vr[k + is * vr_dim1] = 0.f;
/* L70: */
		    }

		} else if (nb == 1) {
/*                 ------------------------------ */
/*                 version 1: back-transform each vector with GEMV, Q*x. */
		    if (ki > 1) {
			i__2 = ki - 1;
			sgemv_("N", n, &i__2, &c_b29, &vr[vr_offset], ldvr, &
				work[iv * *n + 1], &c__1, &work[ki + iv * *n],
				 &vr[ki * vr_dim1 + 1], &c__1);
		    }

		    ii = isamax_(n, &vr[ki * vr_dim1 + 1], &c__1);
		    remax = 1.f / (r__1 = vr[ii + ki * vr_dim1], abs(r__1));
		    sscal_(n, &remax, &vr[ki * vr_dim1 + 1], &c__1);

		} else {
/*                 ------------------------------ */
/*                 version 2: back-transform block of vectors with GEMM */
/*                 zero out below vector */
		    i__2 = *n;
		    for (k = ki + 1; k <= i__2; ++k) {
			work[k + iv * *n] = 0.f;
		    }
		    iscomplex[iv - 1] = ip;
/*                 back-transform and normalization is done below */
		}
	    } else {

/*              -------------------------------------------------------- */
/*              Complex right eigenvector. */

/*              Initial solve */
/*              [ ( T(KI-1,KI-1) T(KI-1,KI) ) - (WR + I*WI) ]*X = 0. */
/*              [ ( T(KI,  KI-1) T(KI,  KI) )               ] */

		if ((r__1 = t[ki - 1 + ki * t_dim1], abs(r__1)) >= (r__2 = t[
			ki + (ki - 1) * t_dim1], abs(r__2))) {
		    work[ki - 1 + (iv - 1) * *n] = 1.f;
		    work[ki + iv * *n] = wi / t[ki - 1 + ki * t_dim1];
		} else {
		    work[ki - 1 + (iv - 1) * *n] = -wi / t[ki + (ki - 1) * 
			    t_dim1];
		    work[ki + iv * *n] = 1.f;
		}
		work[ki + (iv - 1) * *n] = 0.f;
		work[ki - 1 + iv * *n] = 0.f;

/*              Form right-hand side. */

		i__2 = ki - 2;
		for (k = 1; k <= i__2; ++k) {
		    work[k + (iv - 1) * *n] = -work[ki - 1 + (iv - 1) * *n] * 
			    t[k + (ki - 1) * t_dim1];
		    work[k + iv * *n] = -work[ki + iv * *n] * t[k + ki * 
			    t_dim1];
/* L80: */
		}

/*              Solve upper quasi-triangular system: */
/*              [ T(1:KI-2,1:KI-2) - (WR+i*WI) ]*X = SCALE*(WORK+i*WORK2) */

		jnxt = ki - 2;
		for (j = ki - 2; j >= 1; --j) {
		    if (j > jnxt) {
			goto L90;
		    }
		    j1 = j;
		    j2 = j;
		    jnxt = j - 1;
		    if (j > 1) {
			if (t[j + (j - 1) * t_dim1] != 0.f) {
			    j1 = j - 1;
			    jnxt = j - 2;
			}
		    }

		    if (j1 == j2) {

/*                    1-by-1 diagonal block */

			slaln2_(&c_false, &c__1, &c__2, &smin, &c_b29, &t[j + 
				j * t_dim1], ldt, &c_b29, &c_b29, &work[j + (
				iv - 1) * *n], n, &wr, &wi, x, &c__2, &scale, 
				&xnorm, &ierr);

/*                    Scale X(1,1) and X(1,2) to avoid overflow when */
/*                    updating the right-hand side. */

			if (xnorm > 1.f) {
			    if (work[j] > bignum / xnorm) {
				x[0] /= xnorm;
				x[2] /= xnorm;
				scale /= xnorm;
			    }
			}

/*                    Scale if necessary */

			if (scale != 1.f) {
			    sscal_(&ki, &scale, &work[(iv - 1) * *n + 1], &
				    c__1);
			    sscal_(&ki, &scale, &work[iv * *n + 1], &c__1);
			}
			work[j + (iv - 1) * *n] = x[0];
			work[j + iv * *n] = x[2];

/*                    Update the right-hand side */

			i__2 = j - 1;
			r__1 = -x[0];
			saxpy_(&i__2, &r__1, &t[j * t_dim1 + 1], &c__1, &work[
				(iv - 1) * *n + 1], &c__1);
			i__2 = j - 1;
			r__1 = -x[2];
			saxpy_(&i__2, &r__1, &t[j * t_dim1 + 1], &c__1, &work[
				iv * *n + 1], &c__1);

		    } else {

/*                    2-by-2 diagonal block */

			slaln2_(&c_false, &c__2, &c__2, &smin, &c_b29, &t[j - 
				1 + (j - 1) * t_dim1], ldt, &c_b29, &c_b29, &
				work[j - 1 + (iv - 1) * *n], n, &wr, &wi, x, &
				c__2, &scale, &xnorm, &ierr);

/*                    Scale X to avoid overflow when updating */
/*                    the right-hand side. */

			if (xnorm > 1.f) {
/* Computing MAX */
			    r__1 = work[j - 1], r__2 = work[j];
			    beta = f2cmax(r__1,r__2);
			    if (beta > bignum / xnorm) {
				rec = 1.f / xnorm;
				x[0] *= rec;
				x[2] *= rec;
				x[1] *= rec;
				x[3] *= rec;
				scale *= rec;
			    }
			}

/*                    Scale if necessary */

			if (scale != 1.f) {
			    sscal_(&ki, &scale, &work[(iv - 1) * *n + 1], &
				    c__1);
			    sscal_(&ki, &scale, &work[iv * *n + 1], &c__1);
			}
			work[j - 1 + (iv - 1) * *n] = x[0];
			work[j + (iv - 1) * *n] = x[1];
			work[j - 1 + iv * *n] = x[2];
			work[j + iv * *n] = x[3];

/*                    Update the right-hand side */

			i__2 = j - 2;
			r__1 = -x[0];
			saxpy_(&i__2, &r__1, &t[(j - 1) * t_dim1 + 1], &c__1, 
				&work[(iv - 1) * *n + 1], &c__1);
			i__2 = j - 2;
			r__1 = -x[1];
			saxpy_(&i__2, &r__1, &t[j * t_dim1 + 1], &c__1, &work[
				(iv - 1) * *n + 1], &c__1);
			i__2 = j - 2;
			r__1 = -x[2];
			saxpy_(&i__2, &r__1, &t[(j - 1) * t_dim1 + 1], &c__1, 
				&work[iv * *n + 1], &c__1);
			i__2 = j - 2;
			r__1 = -x[3];
			saxpy_(&i__2, &r__1, &t[j * t_dim1 + 1], &c__1, &work[
				iv * *n + 1], &c__1);
		    }
L90:
		    ;
		}

/*              Copy the vector x or Q*x to VR and normalize. */

		if (! over) {
/*                 ------------------------------ */
/*                 no back-transform: copy x to VR and normalize. */
		    scopy_(&ki, &work[(iv - 1) * *n + 1], &c__1, &vr[(is - 1) 
			    * vr_dim1 + 1], &c__1);
		    scopy_(&ki, &work[iv * *n + 1], &c__1, &vr[is * vr_dim1 + 
			    1], &c__1);

		    emax = 0.f;
		    i__2 = ki;
		    for (k = 1; k <= i__2; ++k) {
/* Computing MAX */
			r__3 = emax, r__4 = (r__1 = vr[k + (is - 1) * vr_dim1]
				, abs(r__1)) + (r__2 = vr[k + is * vr_dim1], 
				abs(r__2));
			emax = f2cmax(r__3,r__4);
/* L100: */
		    }
		    remax = 1.f / emax;
		    sscal_(&ki, &remax, &vr[(is - 1) * vr_dim1 + 1], &c__1);
		    sscal_(&ki, &remax, &vr[is * vr_dim1 + 1], &c__1);

		    i__2 = *n;
		    for (k = ki + 1; k <= i__2; ++k) {
			vr[k + (is - 1) * vr_dim1] = 0.f;
			vr[k + is * vr_dim1] = 0.f;
/* L110: */
		    }

		} else if (nb == 1) {
/*                 ------------------------------ */
/*                 version 1: back-transform each vector with GEMV, Q*x. */
		    if (ki > 2) {
			i__2 = ki - 2;
			sgemv_("N", n, &i__2, &c_b29, &vr[vr_offset], ldvr, &
				work[(iv - 1) * *n + 1], &c__1, &work[ki - 1 
				+ (iv - 1) * *n], &vr[(ki - 1) * vr_dim1 + 1],
				 &c__1);
			i__2 = ki - 2;
			sgemv_("N", n, &i__2, &c_b29, &vr[vr_offset], ldvr, &
				work[iv * *n + 1], &c__1, &work[ki + iv * *n],
				 &vr[ki * vr_dim1 + 1], &c__1);
		    } else {
			sscal_(n, &work[ki - 1 + (iv - 1) * *n], &vr[(ki - 1) 
				* vr_dim1 + 1], &c__1);
			sscal_(n, &work[ki + iv * *n], &vr[ki * vr_dim1 + 1], 
				&c__1);
		    }

		    emax = 0.f;
		    i__2 = *n;
		    for (k = 1; k <= i__2; ++k) {
/* Computing MAX */
			r__3 = emax, r__4 = (r__1 = vr[k + (ki - 1) * vr_dim1]
				, abs(r__1)) + (r__2 = vr[k + ki * vr_dim1], 
				abs(r__2));
			emax = f2cmax(r__3,r__4);
/* L120: */
		    }
		    remax = 1.f / emax;
		    sscal_(n, &remax, &vr[(ki - 1) * vr_dim1 + 1], &c__1);
		    sscal_(n, &remax, &vr[ki * vr_dim1 + 1], &c__1);

		} else {
/*                 ------------------------------ */
/*                 version 2: back-transform block of vectors with GEMM */
/*                 zero out below vector */
		    i__2 = *n;
		    for (k = ki + 1; k <= i__2; ++k) {
			work[k + (iv - 1) * *n] = 0.f;
			work[k + iv * *n] = 0.f;
		    }
		    iscomplex[iv - 2] = -ip;
		    iscomplex[iv - 1] = ip;
		    --iv;
/*                 back-transform and normalization is done below */
		}
	    }
	    if (nb > 1) {
/*              -------------------------------------------------------- */
/*              Blocked version of back-transform */
/*              For complex case, KI2 includes both vectors (KI-1 and KI) */
		if (ip == 0) {
		    ki2 = ki;
		} else {
		    ki2 = ki - 1;
		}
/*              Columns IV:NB of work are valid vectors. */
/*              When the number of vectors stored reaches NB-1 or NB, */
/*              or if this was last vector, do the GEMM */
		if (iv <= 2 || ki2 == 1) {
		    i__2 = nb - iv + 1;
		    i__3 = ki2 + nb - iv;
		    sgemm_("N", "N", n, &i__2, &i__3, &c_b29, &vr[vr_offset], 
			    ldvr, &work[iv * *n + 1], n, &c_b17, &work[(nb + 
			    iv) * *n + 1], n);
/*                 normalize vectors */
		    i__2 = nb;
		    for (k = iv; k <= i__2; ++k) {
			if (iscomplex[k - 1] == 0) {
/*                       real eigenvector */
			    ii = isamax_(n, &work[(nb + k) * *n + 1], &c__1);
			    remax = 1.f / (r__1 = work[ii + (nb + k) * *n], 
				    abs(r__1));
			} else if (iscomplex[k - 1] == 1) {
/*                       first eigenvector of conjugate pair */
			    emax = 0.f;
			    i__3 = *n;
			    for (ii = 1; ii <= i__3; ++ii) {
/* Computing MAX */
				r__3 = emax, r__4 = (r__1 = work[ii + (nb + k)
					 * *n], abs(r__1)) + (r__2 = work[ii 
					+ (nb + k + 1) * *n], abs(r__2));
				emax = f2cmax(r__3,r__4);
			    }
			    remax = 1.f / emax;
/*                    else if ISCOMPLEX(K).EQ.-1 */
/*                       second eigenvector of conjugate pair */
/*                       reuse same REMAX as previous K */
			}
			sscal_(n, &remax, &work[(nb + k) * *n + 1], &c__1);
		    }
		    i__2 = nb - iv + 1;
		    slacpy_("F", n, &i__2, &work[(nb + iv) * *n + 1], n, &vr[
			    ki2 * vr_dim1 + 1], ldvr);
		    iv = nb;
		} else {
		    --iv;
		}
	    }

/* blocked back-transform */
	    --is;
	    if (ip != 0) {
		--is;
	    }
L140:
	    ;
	}
    }
    if (leftv) {

/*        ============================================================ */
/*        Compute left eigenvectors. */

/*        IV is index of column in current block. */
/*        For complex left vector, uses IV for real part and IV+1 for complex part. */
/*        Non-blocked version always uses IV=1; */
/*        blocked     version starts with IV=1, goes up to NB-1 or NB. */
/*        (Note the "0-th" column is used for 1-norms computed above.) */
	iv = 1;
	ip = 0;
	is = 1;
	i__2 = *n;
	for (ki = 1; ki <= i__2; ++ki) {
	    if (ip == 1) {
/*              previous iteration (ki-1) was first of conjugate pair, */
/*              so this ki is second of conjugate pair; skip to end of loop */
		ip = -1;
		goto L260;
	    } else if (ki == *n) {
/*              last column, so this ki must be real eigenvalue */
		ip = 0;
	    } else if (t[ki + 1 + ki * t_dim1] == 0.f) {
/*              zero on sub-diagonal, so this ki is real eigenvalue */
		ip = 0;
	    } else {
/*              non-zero on sub-diagonal, so this ki is first of conjugate pair */
		ip = 1;
	    }

	    if (somev) {
		if (! select[ki]) {
		    goto L260;
		}
	    }

/*           Compute the KI-th eigenvalue (WR,WI). */

	    wr = t[ki + ki * t_dim1];
	    wi = 0.f;
	    if (ip != 0) {
		wi = sqrt((r__1 = t[ki + (ki + 1) * t_dim1], abs(r__1))) * 
			sqrt((r__2 = t[ki + 1 + ki * t_dim1], abs(r__2)));
	    }
/* Computing MAX */
	    r__1 = ulp * (abs(wr) + abs(wi));
	    smin = f2cmax(r__1,smlnum);

	    if (ip == 0) {

/*              -------------------------------------------------------- */
/*              Real left eigenvector */

		work[ki + iv * *n] = 1.f;

/*              Form right-hand side. */

		i__3 = *n;
		for (k = ki + 1; k <= i__3; ++k) {
		    work[k + iv * *n] = -t[ki + k * t_dim1];
/* L160: */
		}

/*              Solve transposed quasi-triangular system: */
/*              [ T(KI+1:N,KI+1:N) - WR ]**T * X = SCALE*WORK */

		vmax = 1.f;
		vcrit = bignum;

		jnxt = ki + 1;
		i__3 = *n;
		for (j = ki + 1; j <= i__3; ++j) {
		    if (j < jnxt) {
			goto L170;
		    }
		    j1 = j;
		    j2 = j;
		    jnxt = j + 1;
		    if (j < *n) {
			if (t[j + 1 + j * t_dim1] != 0.f) {
			    j2 = j + 1;
			    jnxt = j + 2;
			}
		    }

		    if (j1 == j2) {

/*                    1-by-1 diagonal block */

/*                    Scale if necessary to avoid overflow when forming */
/*                    the right-hand side. */

			if (work[j] > vcrit) {
			    rec = 1.f / vmax;
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &rec, &work[ki + iv * *n], &c__1);
			    vmax = 1.f;
			    vcrit = bignum;
			}

			i__4 = j - ki - 1;
			work[j + iv * *n] -= sdot_(&i__4, &t[ki + 1 + j * 
				t_dim1], &c__1, &work[ki + 1 + iv * *n], &
				c__1);

/*                    Solve [ T(J,J) - WR ]**T * X = WORK */

			slaln2_(&c_false, &c__1, &c__1, &smin, &c_b29, &t[j + 
				j * t_dim1], ldt, &c_b29, &c_b29, &work[j + 
				iv * *n], n, &wr, &c_b17, x, &c__2, &scale, &
				xnorm, &ierr);

/*                    Scale if necessary */

			if (scale != 1.f) {
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &scale, &work[ki + iv * *n], &c__1);
			}
			work[j + iv * *n] = x[0];
/* Computing MAX */
			r__2 = (r__1 = work[j + iv * *n], abs(r__1));
			vmax = f2cmax(r__2,vmax);
			vcrit = bignum / vmax;

		    } else {

/*                    2-by-2 diagonal block */

/*                    Scale if necessary to avoid overflow when forming */
/*                    the right-hand side. */

/* Computing MAX */
			r__1 = work[j], r__2 = work[j + 1];
			beta = f2cmax(r__1,r__2);
			if (beta > vcrit) {
			    rec = 1.f / vmax;
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &rec, &work[ki + iv * *n], &c__1);
			    vmax = 1.f;
			    vcrit = bignum;
			}

			i__4 = j - ki - 1;
			work[j + iv * *n] -= sdot_(&i__4, &t[ki + 1 + j * 
				t_dim1], &c__1, &work[ki + 1 + iv * *n], &
				c__1);

			i__4 = j - ki - 1;
			work[j + 1 + iv * *n] -= sdot_(&i__4, &t[ki + 1 + (j 
				+ 1) * t_dim1], &c__1, &work[ki + 1 + iv * *n]
				, &c__1);

/*                    Solve */
/*                    [ T(J,J)-WR   T(J,J+1)      ]**T * X = SCALE*( WORK1 ) */
/*                    [ T(J+1,J)    T(J+1,J+1)-WR ]                ( WORK2 ) */

			slaln2_(&c_true, &c__2, &c__1, &smin, &c_b29, &t[j + 
				j * t_dim1], ldt, &c_b29, &c_b29, &work[j + 
				iv * *n], n, &wr, &c_b17, x, &c__2, &scale, &
				xnorm, &ierr);

/*                    Scale if necessary */

			if (scale != 1.f) {
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &scale, &work[ki + iv * *n], &c__1);
			}
			work[j + iv * *n] = x[0];
			work[j + 1 + iv * *n] = x[1];

/* Computing MAX */
			r__3 = (r__1 = work[j + iv * *n], abs(r__1)), r__4 = (
				r__2 = work[j + 1 + iv * *n], abs(r__2)), 
				r__3 = f2cmax(r__3,r__4);
			vmax = f2cmax(r__3,vmax);
			vcrit = bignum / vmax;

		    }
L170:
		    ;
		}

/*              Copy the vector x or Q*x to VL and normalize. */

		if (! over) {
/*                 ------------------------------ */
/*                 no back-transform: copy x to VL and normalize. */
		    i__3 = *n - ki + 1;
		    scopy_(&i__3, &work[ki + iv * *n], &c__1, &vl[ki + is * 
			    vl_dim1], &c__1);

		    i__3 = *n - ki + 1;
		    ii = isamax_(&i__3, &vl[ki + is * vl_dim1], &c__1) + ki - 
			    1;
		    remax = 1.f / (r__1 = vl[ii + is * vl_dim1], abs(r__1));
		    i__3 = *n - ki + 1;
		    sscal_(&i__3, &remax, &vl[ki + is * vl_dim1], &c__1);

		    i__3 = ki - 1;
		    for (k = 1; k <= i__3; ++k) {
			vl[k + is * vl_dim1] = 0.f;
/* L180: */
		    }

		} else if (nb == 1) {
/*                 ------------------------------ */
/*                 version 1: back-transform each vector with GEMV, Q*x. */
		    if (ki < *n) {
			i__3 = *n - ki;
			sgemv_("N", n, &i__3, &c_b29, &vl[(ki + 1) * vl_dim1 
				+ 1], ldvl, &work[ki + 1 + iv * *n], &c__1, &
				work[ki + iv * *n], &vl[ki * vl_dim1 + 1], &
				c__1);
		    }

		    ii = isamax_(n, &vl[ki * vl_dim1 + 1], &c__1);
		    remax = 1.f / (r__1 = vl[ii + ki * vl_dim1], abs(r__1));
		    sscal_(n, &remax, &vl[ki * vl_dim1 + 1], &c__1);

		} else {
/*                 ------------------------------ */
/*                 version 2: back-transform block of vectors with GEMM */
/*                 zero out above vector */
/*                 could go from KI-NV+1 to KI-1 */
		    i__3 = ki - 1;
		    for (k = 1; k <= i__3; ++k) {
			work[k + iv * *n] = 0.f;
		    }
		    iscomplex[iv - 1] = ip;
/*                 back-transform and normalization is done below */
		}
	    } else {

/*              -------------------------------------------------------- */
/*              Complex left eigenvector. */

/*              Initial solve: */
/*              [ ( T(KI,KI)    T(KI,KI+1)  )**T - (WR - I* WI) ]*X = 0. */
/*              [ ( T(KI+1,KI) T(KI+1,KI+1) )                   ] */

		if ((r__1 = t[ki + (ki + 1) * t_dim1], abs(r__1)) >= (r__2 = 
			t[ki + 1 + ki * t_dim1], abs(r__2))) {
		    work[ki + iv * *n] = wi / t[ki + (ki + 1) * t_dim1];
		    work[ki + 1 + (iv + 1) * *n] = 1.f;
		} else {
		    work[ki + iv * *n] = 1.f;
		    work[ki + 1 + (iv + 1) * *n] = -wi / t[ki + 1 + ki * 
			    t_dim1];
		}
		work[ki + 1 + iv * *n] = 0.f;
		work[ki + (iv + 1) * *n] = 0.f;

/*              Form right-hand side. */

		i__3 = *n;
		for (k = ki + 2; k <= i__3; ++k) {
		    work[k + iv * *n] = -work[ki + iv * *n] * t[ki + k * 
			    t_dim1];
		    work[k + (iv + 1) * *n] = -work[ki + 1 + (iv + 1) * *n] * 
			    t[ki + 1 + k * t_dim1];
/* L190: */
		}

/*              Solve transposed quasi-triangular system: */
/*              [ T(KI+2:N,KI+2:N)**T - (WR-i*WI) ]*X = WORK1+i*WORK2 */

		vmax = 1.f;
		vcrit = bignum;

		jnxt = ki + 2;
		i__3 = *n;
		for (j = ki + 2; j <= i__3; ++j) {
		    if (j < jnxt) {
			goto L200;
		    }
		    j1 = j;
		    j2 = j;
		    jnxt = j + 1;
		    if (j < *n) {
			if (t[j + 1 + j * t_dim1] != 0.f) {
			    j2 = j + 1;
			    jnxt = j + 2;
			}
		    }

		    if (j1 == j2) {

/*                    1-by-1 diagonal block */

/*                    Scale if necessary to avoid overflow when */
/*                    forming the right-hand side elements. */

			if (work[j] > vcrit) {
			    rec = 1.f / vmax;
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &rec, &work[ki + iv * *n], &c__1);
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &rec, &work[ki + (iv + 1) * *n], &
				    c__1);
			    vmax = 1.f;
			    vcrit = bignum;
			}

			i__4 = j - ki - 2;
			work[j + iv * *n] -= sdot_(&i__4, &t[ki + 2 + j * 
				t_dim1], &c__1, &work[ki + 2 + iv * *n], &
				c__1);
			i__4 = j - ki - 2;
			work[j + (iv + 1) * *n] -= sdot_(&i__4, &t[ki + 2 + j 
				* t_dim1], &c__1, &work[ki + 2 + (iv + 1) * *
				n], &c__1);

/*                    Solve [ T(J,J)-(WR-i*WI) ]*(X11+i*X12)= WK+I*WK2 */

			r__1 = -wi;
			slaln2_(&c_false, &c__1, &c__2, &smin, &c_b29, &t[j + 
				j * t_dim1], ldt, &c_b29, &c_b29, &work[j + 
				iv * *n], n, &wr, &r__1, x, &c__2, &scale, &
				xnorm, &ierr);

/*                    Scale if necessary */

			if (scale != 1.f) {
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &scale, &work[ki + iv * *n], &c__1);
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &scale, &work[ki + (iv + 1) * *n], &
				    c__1);
			}
			work[j + iv * *n] = x[0];
			work[j + (iv + 1) * *n] = x[2];
/* Computing MAX */
			r__3 = (r__1 = work[j + iv * *n], abs(r__1)), r__4 = (
				r__2 = work[j + (iv + 1) * *n], abs(r__2)), 
				r__3 = f2cmax(r__3,r__4);
			vmax = f2cmax(r__3,vmax);
			vcrit = bignum / vmax;

		    } else {

/*                    2-by-2 diagonal block */

/*                    Scale if necessary to avoid overflow when forming */
/*                    the right-hand side elements. */

/* Computing MAX */
			r__1 = work[j], r__2 = work[j + 1];
			beta = f2cmax(r__1,r__2);
			if (beta > vcrit) {
			    rec = 1.f / vmax;
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &rec, &work[ki + iv * *n], &c__1);
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &rec, &work[ki + (iv + 1) * *n], &
				    c__1);
			    vmax = 1.f;
			    vcrit = bignum;
			}

			i__4 = j - ki - 2;
			work[j + iv * *n] -= sdot_(&i__4, &t[ki + 2 + j * 
				t_dim1], &c__1, &work[ki + 2 + iv * *n], &
				c__1);

			i__4 = j - ki - 2;
			work[j + (iv + 1) * *n] -= sdot_(&i__4, &t[ki + 2 + j 
				* t_dim1], &c__1, &work[ki + 2 + (iv + 1) * *
				n], &c__1);

			i__4 = j - ki - 2;
			work[j + 1 + iv * *n] -= sdot_(&i__4, &t[ki + 2 + (j 
				+ 1) * t_dim1], &c__1, &work[ki + 2 + iv * *n]
				, &c__1);

			i__4 = j - ki - 2;
			work[j + 1 + (iv + 1) * *n] -= sdot_(&i__4, &t[ki + 2 
				+ (j + 1) * t_dim1], &c__1, &work[ki + 2 + (
				iv + 1) * *n], &c__1);

/*                    Solve 2-by-2 complex linear equation */
/*                    [ (T(j,j)   T(j,j+1)  )**T - (wr-i*wi)*I ]*X = SCALE*B */
/*                    [ (T(j+1,j) T(j+1,j+1))                  ] */

			r__1 = -wi;
			slaln2_(&c_true, &c__2, &c__2, &smin, &c_b29, &t[j + 
				j * t_dim1], ldt, &c_b29, &c_b29, &work[j + 
				iv * *n], n, &wr, &r__1, x, &c__2, &scale, &
				xnorm, &ierr);

/*                    Scale if necessary */

			if (scale != 1.f) {
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &scale, &work[ki + iv * *n], &c__1);
			    i__4 = *n - ki + 1;
			    sscal_(&i__4, &scale, &work[ki + (iv + 1) * *n], &
				    c__1);
			}
			work[j + iv * *n] = x[0];
			work[j + (iv + 1) * *n] = x[2];
			work[j + 1 + iv * *n] = x[1];
			work[j + 1 + (iv + 1) * *n] = x[3];
/* Computing MAX */
			r__1 = abs(x[0]), r__2 = abs(x[2]), r__1 = f2cmax(r__1,
				r__2), r__2 = abs(x[1]), r__1 = f2cmax(r__1,r__2)
				, r__2 = abs(x[3]), r__1 = f2cmax(r__1,r__2);
			vmax = f2cmax(r__1,vmax);
			vcrit = bignum / vmax;

		    }
L200:
		    ;
		}

/*              Copy the vector x or Q*x to VL and normalize. */

		if (! over) {
/*                 ------------------------------ */
/*                 no back-transform: copy x to VL and normalize. */
		    i__3 = *n - ki + 1;
		    scopy_(&i__3, &work[ki + iv * *n], &c__1, &vl[ki + is * 
			    vl_dim1], &c__1);
		    i__3 = *n - ki + 1;
		    scopy_(&i__3, &work[ki + (iv + 1) * *n], &c__1, &vl[ki + (
			    is + 1) * vl_dim1], &c__1);

		    emax = 0.f;
		    i__3 = *n;
		    for (k = ki; k <= i__3; ++k) {
/* Computing MAX */
			r__3 = emax, r__4 = (r__1 = vl[k + is * vl_dim1], abs(
				r__1)) + (r__2 = vl[k + (is + 1) * vl_dim1], 
				abs(r__2));
			emax = f2cmax(r__3,r__4);
/* L220: */
		    }
		    remax = 1.f / emax;
		    i__3 = *n - ki + 1;
		    sscal_(&i__3, &remax, &vl[ki + is * vl_dim1], &c__1);
		    i__3 = *n - ki + 1;
		    sscal_(&i__3, &remax, &vl[ki + (is + 1) * vl_dim1], &c__1)
			    ;

		    i__3 = ki - 1;
		    for (k = 1; k <= i__3; ++k) {
			vl[k + is * vl_dim1] = 0.f;
			vl[k + (is + 1) * vl_dim1] = 0.f;
/* L230: */
		    }

		} else if (nb == 1) {
/*                 ------------------------------ */
/*                 version 1: back-transform each vector with GEMV, Q*x. */
		    if (ki < *n - 1) {
			i__3 = *n - ki - 1;
			sgemv_("N", n, &i__3, &c_b29, &vl[(ki + 2) * vl_dim1 
				+ 1], ldvl, &work[ki + 2 + iv * *n], &c__1, &
				work[ki + iv * *n], &vl[ki * vl_dim1 + 1], &
				c__1);
			i__3 = *n - ki - 1;
			sgemv_("N", n, &i__3, &c_b29, &vl[(ki + 2) * vl_dim1 
				+ 1], ldvl, &work[ki + 2 + (iv + 1) * *n], &
				c__1, &work[ki + 1 + (iv + 1) * *n], &vl[(ki 
				+ 1) * vl_dim1 + 1], &c__1);
		    } else {
			sscal_(n, &work[ki + iv * *n], &vl[ki * vl_dim1 + 1], 
				&c__1);
			sscal_(n, &work[ki + 1 + (iv + 1) * *n], &vl[(ki + 1) 
				* vl_dim1 + 1], &c__1);
		    }

		    emax = 0.f;
		    i__3 = *n;
		    for (k = 1; k <= i__3; ++k) {
/* Computing MAX */
			r__3 = emax, r__4 = (r__1 = vl[k + ki * vl_dim1], abs(
				r__1)) + (r__2 = vl[k + (ki + 1) * vl_dim1], 
				abs(r__2));
			emax = f2cmax(r__3,r__4);
/* L240: */
		    }
		    remax = 1.f / emax;
		    sscal_(n, &remax, &vl[ki * vl_dim1 + 1], &c__1);
		    sscal_(n, &remax, &vl[(ki + 1) * vl_dim1 + 1], &c__1);

		} else {
/*                 ------------------------------ */
/*                 version 2: back-transform block of vectors with GEMM */
/*                 zero out above vector */
/*                 could go from KI-NV+1 to KI-1 */
		    i__3 = ki - 1;
		    for (k = 1; k <= i__3; ++k) {
			work[k + iv * *n] = 0.f;
			work[k + (iv + 1) * *n] = 0.f;
		    }
		    iscomplex[iv - 1] = ip;
		    iscomplex[iv] = -ip;
		    ++iv;
/*                 back-transform and normalization is done below */
		}
	    }
	    if (nb > 1) {
/*              -------------------------------------------------------- */
/*              Blocked version of back-transform */
/*              For complex case, KI2 includes both vectors (KI and KI+1) */
		if (ip == 0) {
		    ki2 = ki;
		} else {
		    ki2 = ki + 1;
		}
/*              Columns 1:IV of work are valid vectors. */
/*              When the number of vectors stored reaches NB-1 or NB, */
/*              or if this was last vector, do the GEMM */
		if (iv >= nb - 1 || ki2 == *n) {
		    i__3 = *n - ki2 + iv;
		    sgemm_("N", "N", n, &iv, &i__3, &c_b29, &vl[(ki2 - iv + 1)
			     * vl_dim1 + 1], ldvl, &work[ki2 - iv + 1 + *n], 
			    n, &c_b17, &work[(nb + 1) * *n + 1], n);
/*                 normalize vectors */
		    i__3 = iv;
		    for (k = 1; k <= i__3; ++k) {
			if (iscomplex[k - 1] == 0) {
/*                       real eigenvector */
			    ii = isamax_(n, &work[(nb + k) * *n + 1], &c__1);
			    remax = 1.f / (r__1 = work[ii + (nb + k) * *n], 
				    abs(r__1));
			} else if (iscomplex[k - 1] == 1) {
/*                       first eigenvector of conjugate pair */
			    emax = 0.f;
			    i__4 = *n;
			    for (ii = 1; ii <= i__4; ++ii) {
/* Computing MAX */
				r__3 = emax, r__4 = (r__1 = work[ii + (nb + k)
					 * *n], abs(r__1)) + (r__2 = work[ii 
					+ (nb + k + 1) * *n], abs(r__2));
				emax = f2cmax(r__3,r__4);
			    }
			    remax = 1.f / emax;
/*                    else if ISCOMPLEX(K).EQ.-1 */
/*                       second eigenvector of conjugate pair */
/*                       reuse same REMAX as previous K */
			}
			sscal_(n, &remax, &work[(nb + k) * *n + 1], &c__1);
		    }
		    slacpy_("F", n, &iv, &work[(nb + 1) * *n + 1], n, &vl[(
			    ki2 - iv + 1) * vl_dim1 + 1], ldvl);
		    iv = 1;
		} else {
		    ++iv;
		}
	    }

/* blocked back-transform */
	    ++is;
	    if (ip != 0) {
		++is;
	    }
L260:
	    ;
	}
    }

    return;

/*     End of STREVC3 */

} /* strevc3_ */


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
static integer c__1 = 1;
static integer c_n1 = -1;
static integer c__2 = 2;

/* > \brief \b CTREVC3 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CTREVC3 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ctrevc3
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ctrevc3
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ctrevc3
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CTREVC3( SIDE, HOWMNY, SELECT, N, T, LDT, VL, LDVL, VR, */
/*                           LDVR, MM, M, WORK, LWORK, RWORK, LRWORK, INFO) */

/*       CHARACTER          HOWMNY, SIDE */
/*       INTEGER            INFO, LDT, LDVL, LDVR, LWORK, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       REAL               RWORK( * ) */
/*       COMPLEX            T( LDT, * ), VL( LDVL, * ), VR( LDVR, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CTREVC3 computes some or all of the right and/or left eigenvectors of */
/* > a complex upper triangular matrix T. */
/* > Matrices of this type are produced by the Schur factorization of */
/* > a complex general matrix:  A = Q*T*Q**H, as computed by CHSEQR. */
/* > */
/* > The right eigenvector x and the left eigenvector y of T corresponding */
/* > to an eigenvalue w are defined by: */
/* > */
/* >              T*x = w*x,     (y**H)*T = w*(y**H) */
/* > */
/* > where y**H denotes the conjugate transpose of the vector y. */
/* > The eigenvalues are not input to this routine, but are read directly */
/* > from the diagonal of T. */
/* > */
/* > This routine returns the matrices X and/or Y of right and left */
/* > eigenvectors of T, or the products Q*X and/or Q*Y, where Q is an */
/* > input matrix. If Q is the unitary factor that reduces a matrix A to */
/* > Schur form T, then Q*X and Q*Y are the matrices of right and left */
/* > eigenvectors of A. */
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
/* >                  backtransformed using the matrices supplied in */
/* >                  VR and/or VL; */
/* >          = 'S':  compute selected right and/or left eigenvectors, */
/* >                  as indicated by the logical array SELECT. */
/* > \endverbatim */
/* > */
/* > \param[in] SELECT */
/* > \verbatim */
/* >          SELECT is LOGICAL array, dimension (N) */
/* >          If HOWMNY = 'S', SELECT specifies the eigenvectors to be */
/* >          computed. */
/* >          The eigenvector corresponding to the j-th eigenvalue is */
/* >          computed if SELECT(j) = .TRUE.. */
/* >          Not referenced if HOWMNY = 'A' or 'B'. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix T. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] T */
/* > \verbatim */
/* >          T is COMPLEX array, dimension (LDT,N) */
/* >          The upper triangular matrix T.  T is modified, but restored */
/* >          on exit. */
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
/* >          VL is COMPLEX array, dimension (LDVL,MM) */
/* >          On entry, if SIDE = 'L' or 'B' and HOWMNY = 'B', VL must */
/* >          contain an N-by-N matrix Q (usually the unitary matrix Q of */
/* >          Schur vectors returned by CHSEQR). */
/* >          On exit, if SIDE = 'L' or 'B', VL contains: */
/* >          if HOWMNY = 'A', the matrix Y of left eigenvectors of T; */
/* >          if HOWMNY = 'B', the matrix Q*Y; */
/* >          if HOWMNY = 'S', the left eigenvectors of T specified by */
/* >                           SELECT, stored consecutively in the columns */
/* >                           of VL, in the same order as their */
/* >                           eigenvalues. */
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
/* >          VR is COMPLEX array, dimension (LDVR,MM) */
/* >          On entry, if SIDE = 'R' or 'B' and HOWMNY = 'B', VR must */
/* >          contain an N-by-N matrix Q (usually the unitary matrix Q of */
/* >          Schur vectors returned by CHSEQR). */
/* >          On exit, if SIDE = 'R' or 'B', VR contains: */
/* >          if HOWMNY = 'A', the matrix X of right eigenvectors of T; */
/* >          if HOWMNY = 'B', the matrix Q*X; */
/* >          if HOWMNY = 'S', the right eigenvectors of T specified by */
/* >                           SELECT, stored consecutively in the columns */
/* >                           of VR, in the same order as their */
/* >                           eigenvalues. */
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
/* >          Each selected eigenvector occupies one column. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (MAX(1,LWORK)) */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of array WORK. LWORK >= f2cmax(1,2*N). */
/* >          For optimum performance, LWORK >= N + 2*N*NB, where NB is */
/* >          the optimal blocksize. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (LRWORK) */
/* > \endverbatim */
/* > */
/* > \param[in] LRWORK */
/* > \verbatim */
/* >          LRWORK is INTEGER */
/* >          The dimension of array RWORK. LRWORK >= f2cmax(1,N). */
/* > */
/* >          If LRWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the RWORK array, returns */
/* >          this value as the first entry of the RWORK array, and no error */
/* >          message related to LRWORK is issued by XERBLA. */
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

/*  @generated from ztrevc3.f, fortran z -> c, Tue Apr 19 01:47:44 2016 */

/* > \ingroup complexOTHERcomputational */

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
/* Subroutine */ void ctrevc3_(char *side, char *howmny, logical *select, 
	integer *n, complex *t, integer *ldt, complex *vl, integer *ldvl, 
	complex *vr, integer *ldvr, integer *mm, integer *m, complex *work, 
	integer *lwork, real *rwork, integer *lrwork, integer *info)
{
    /* System generated locals */
    address a__1[2];
    integer t_dim1, t_offset, vl_dim1, vl_offset, vr_dim1, vr_offset, i__1, 
	    i__2[2], i__3, i__4, i__5, i__6;
    real r__1, r__2, r__3;
    complex q__1, q__2;
    char ch__1[2];

    /* Local variables */
    logical allv;
    real unfl, ovfl, smin;
    logical over;
    integer i__, j, k;
    real scale;
    extern /* Subroutine */ void cgemm_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void cgemv_(char *, integer *, integer *, complex *
	    , complex *, integer *, complex *, integer *, complex *, complex *
	    , integer *);
    real remax;
    extern /* Subroutine */ void ccopy_(integer *, complex *, integer *, 
	    complex *, integer *);
    logical leftv, bothv, somev;
    integer nb, ii, ki;
    extern /* Subroutine */ void slabad_(real *, real *);
    integer is, iv;
    extern integer icamax_(integer *, complex *, integer *);
    extern real slamch_(char *);
    extern /* Subroutine */ void csscal_(integer *, real *, complex *, integer 
	    *), claset_(char *, integer *, integer *, complex *, complex *, 
	    complex *, integer *), clacpy_(char *, integer *, integer 
	    *, complex *, integer *, complex *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ void clatrs_(char *, char *, char *, char *, 
	    integer *, complex *, integer *, complex *, real *, real *, 
	    integer *);
    extern real scasum_(integer *, complex *, integer *);
    logical rightv;
    integer maxwrk;
    real smlnum;
    logical lquery;
    real ulp;


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
    --rwork;

    /* Function Body */
    bothv = lsame_(side, "B");
    rightv = lsame_(side, "R") || bothv;
    leftv = lsame_(side, "L") || bothv;

    allv = lsame_(howmny, "A");
    over = lsame_(howmny, "B");
    somev = lsame_(howmny, "S");

/*     Set M to the number of columns required to store the selected */
/*     eigenvectors. */

    if (somev) {
	*m = 0;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (select[j]) {
		++(*m);
	    }
/* L10: */
	}
    } else {
	*m = *n;
    }

    *info = 0;
/* Writing concatenation */
    i__2[0] = 1, a__1[0] = side;
    i__2[1] = 1, a__1[1] = howmny;
    s_cat(ch__1, a__1, i__2, &c__2, (ftnlen)2);
    nb = ilaenv_(&c__1, "CTREVC", ch__1, n, &c_n1, &c_n1, &c_n1, (ftnlen)6, (
	    ftnlen)2);
    maxwrk = *n + (*n << 1) * nb;
    work[1].r = (real) maxwrk, work[1].i = 0.f;
    rwork[1] = (real) (*n);
    lquery = *lwork == -1 || *lrwork == -1;
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
    } else if (*mm < *m) {
	*info = -11;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 1, i__3 = *n << 1;
	if (*lwork < f2cmax(i__1,i__3) && ! lquery) {
	    *info = -14;
	} else if (*lrwork < f2cmax(1,*n) && ! lquery) {
	    *info = -16;
	}
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CTREVC3", &i__1, (ftnlen)7);
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
	i__1 = (nb << 1) + 1;
	claset_("F", n, &i__1, &c_b1, &c_b1, &work[1], n);
    } else {
	nb = 1;
    }

/*     Set the constants to control overflow. */

    unfl = slamch_("Safe minimum");
    ovfl = 1.f / unfl;
    slabad_(&unfl, &ovfl);
    ulp = slamch_("Precision");
    smlnum = unfl * (*n / ulp);

/*     Store the diagonal elements of T in working array WORK. */

    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__3 = i__;
	i__4 = i__ + i__ * t_dim1;
	work[i__3].r = t[i__4].r, work[i__3].i = t[i__4].i;
/* L20: */
    }

/*     Compute 1-norm of each column of strictly upper triangular */
/*     part of T to control overflow in triangular solver. */

    rwork[1] = 0.f;
    i__1 = *n;
    for (j = 2; j <= i__1; ++j) {
	i__3 = j - 1;
	rwork[j] = scasum_(&i__3, &t[j * t_dim1 + 1], &c__1);
/* L30: */
    }

    if (rightv) {

/*        ============================================================ */
/*        Compute right eigenvectors. */

/*        IV is index of column in current block. */
/*        Non-blocked version always uses IV=NB=1; */
/*        blocked     version starts with IV=NB, goes down to 1. */
/*        (Note the "0-th" column is used to store the original diagonal.) */
	iv = nb;
	is = *m;
	for (ki = *n; ki >= 1; --ki) {
	    if (somev) {
		if (! select[ki]) {
		    goto L80;
		}
	    }
/* Computing MAX */
	    i__1 = ki + ki * t_dim1;
	    r__3 = ulp * ((r__1 = t[i__1].r, abs(r__1)) + (r__2 = r_imag(&t[
		    ki + ki * t_dim1]), abs(r__2)));
	    smin = f2cmax(r__3,smlnum);

/*           -------------------------------------------------------- */
/*           Complex right eigenvector */

	    i__1 = ki + iv * *n;
	    work[i__1].r = 1.f, work[i__1].i = 0.f;

/*           Form right-hand side. */

	    i__1 = ki - 1;
	    for (k = 1; k <= i__1; ++k) {
		i__3 = k + iv * *n;
		i__4 = k + ki * t_dim1;
		q__1.r = -t[i__4].r, q__1.i = -t[i__4].i;
		work[i__3].r = q__1.r, work[i__3].i = q__1.i;
/* L40: */
	    }

/*           Solve upper triangular system: */
/*           [ T(1:KI-1,1:KI-1) - T(KI,KI) ]*X = SCALE*WORK. */

	    i__1 = ki - 1;
	    for (k = 1; k <= i__1; ++k) {
		i__3 = k + k * t_dim1;
		i__4 = k + k * t_dim1;
		i__5 = ki + ki * t_dim1;
		q__1.r = t[i__4].r - t[i__5].r, q__1.i = t[i__4].i - t[i__5]
			.i;
		t[i__3].r = q__1.r, t[i__3].i = q__1.i;
		i__3 = k + k * t_dim1;
		if ((r__1 = t[i__3].r, abs(r__1)) + (r__2 = r_imag(&t[k + k * 
			t_dim1]), abs(r__2)) < smin) {
		    i__4 = k + k * t_dim1;
		    t[i__4].r = smin, t[i__4].i = 0.f;
		}
/* L50: */
	    }

	    if (ki > 1) {
		i__1 = ki - 1;
		clatrs_("Upper", "No transpose", "Non-unit", "Y", &i__1, &t[
			t_offset], ldt, &work[iv * *n + 1], &scale, &rwork[1],
			 info);
		i__1 = ki + iv * *n;
		work[i__1].r = scale, work[i__1].i = 0.f;
	    }

/*           Copy the vector x or Q*x to VR and normalize. */

	    if (! over) {
/*              ------------------------------ */
/*              no back-transform: copy x to VR and normalize. */
		ccopy_(&ki, &work[iv * *n + 1], &c__1, &vr[is * vr_dim1 + 1], 
			&c__1);

		ii = icamax_(&ki, &vr[is * vr_dim1 + 1], &c__1);
		i__1 = ii + is * vr_dim1;
		remax = 1.f / ((r__1 = vr[i__1].r, abs(r__1)) + (r__2 = 
			r_imag(&vr[ii + is * vr_dim1]), abs(r__2)));
		csscal_(&ki, &remax, &vr[is * vr_dim1 + 1], &c__1);

		i__1 = *n;
		for (k = ki + 1; k <= i__1; ++k) {
		    i__3 = k + is * vr_dim1;
		    vr[i__3].r = 0.f, vr[i__3].i = 0.f;
/* L60: */
		}

	    } else if (nb == 1) {
/*              ------------------------------ */
/*              version 1: back-transform each vector with GEMV, Q*x. */
		if (ki > 1) {
		    i__1 = ki - 1;
		    q__1.r = scale, q__1.i = 0.f;
		    cgemv_("N", n, &i__1, &c_b2, &vr[vr_offset], ldvr, &work[
			    iv * *n + 1], &c__1, &q__1, &vr[ki * vr_dim1 + 1],
			     &c__1);
		}

		ii = icamax_(n, &vr[ki * vr_dim1 + 1], &c__1);
		i__1 = ii + ki * vr_dim1;
		remax = 1.f / ((r__1 = vr[i__1].r, abs(r__1)) + (r__2 = 
			r_imag(&vr[ii + ki * vr_dim1]), abs(r__2)));
		csscal_(n, &remax, &vr[ki * vr_dim1 + 1], &c__1);

	    } else {
/*              ------------------------------ */
/*              version 2: back-transform block of vectors with GEMM */
/*              zero out below vector */
		i__1 = *n;
		for (k = ki + 1; k <= i__1; ++k) {
		    i__3 = k + iv * *n;
		    work[i__3].r = 0.f, work[i__3].i = 0.f;
		}

/*              Columns IV:NB of work are valid vectors. */
/*              When the number of vectors stored reaches NB, */
/*              or if this was last vector, do the GEMM */
		if (iv == 1 || ki == 1) {
		    i__1 = nb - iv + 1;
		    i__3 = ki + nb - iv;
		    cgemm_("N", "N", n, &i__1, &i__3, &c_b2, &vr[vr_offset], 
			    ldvr, &work[iv * *n + 1], n, &c_b1, &work[(nb + 
			    iv) * *n + 1], n);
/*                 normalize vectors */
		    i__1 = nb;
		    for (k = iv; k <= i__1; ++k) {
			ii = icamax_(n, &work[(nb + k) * *n + 1], &c__1);
			i__3 = ii + (nb + k) * *n;
			remax = 1.f / ((r__1 = work[i__3].r, abs(r__1)) + (
				r__2 = r_imag(&work[ii + (nb + k) * *n]), abs(
				r__2)));
			csscal_(n, &remax, &work[(nb + k) * *n + 1], &c__1);
		    }
		    i__1 = nb - iv + 1;
		    clacpy_("F", n, &i__1, &work[(nb + iv) * *n + 1], n, &vr[
			    ki * vr_dim1 + 1], ldvr);
		    iv = nb;
		} else {
		    --iv;
		}
	    }

/*           Restore the original diagonal elements of T. */

	    i__1 = ki - 1;
	    for (k = 1; k <= i__1; ++k) {
		i__3 = k + k * t_dim1;
		i__4 = k;
		t[i__3].r = work[i__4].r, t[i__3].i = work[i__4].i;
/* L70: */
	    }

	    --is;
L80:
	    ;
	}
    }

    if (leftv) {

/*        ============================================================ */
/*        Compute left eigenvectors. */

/*        IV is index of column in current block. */
/*        Non-blocked version always uses IV=1; */
/*        blocked     version starts with IV=1, goes up to NB. */
/*        (Note the "0-th" column is used to store the original diagonal.) */
	iv = 1;
	is = 1;
	i__1 = *n;
	for (ki = 1; ki <= i__1; ++ki) {

	    if (somev) {
		if (! select[ki]) {
		    goto L130;
		}
	    }
/* Computing MAX */
	    i__3 = ki + ki * t_dim1;
	    r__3 = ulp * ((r__1 = t[i__3].r, abs(r__1)) + (r__2 = r_imag(&t[
		    ki + ki * t_dim1]), abs(r__2)));
	    smin = f2cmax(r__3,smlnum);

/*           -------------------------------------------------------- */
/*           Complex left eigenvector */

	    i__3 = ki + iv * *n;
	    work[i__3].r = 1.f, work[i__3].i = 0.f;

/*           Form right-hand side. */

	    i__3 = *n;
	    for (k = ki + 1; k <= i__3; ++k) {
		i__4 = k + iv * *n;
		r_cnjg(&q__2, &t[ki + k * t_dim1]);
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		work[i__4].r = q__1.r, work[i__4].i = q__1.i;
/* L90: */
	    }

/*           Solve conjugate-transposed triangular system: */
/*           [ T(KI+1:N,KI+1:N) - T(KI,KI) ]**H * X = SCALE*WORK. */

	    i__3 = *n;
	    for (k = ki + 1; k <= i__3; ++k) {
		i__4 = k + k * t_dim1;
		i__5 = k + k * t_dim1;
		i__6 = ki + ki * t_dim1;
		q__1.r = t[i__5].r - t[i__6].r, q__1.i = t[i__5].i - t[i__6]
			.i;
		t[i__4].r = q__1.r, t[i__4].i = q__1.i;
		i__4 = k + k * t_dim1;
		if ((r__1 = t[i__4].r, abs(r__1)) + (r__2 = r_imag(&t[k + k * 
			t_dim1]), abs(r__2)) < smin) {
		    i__5 = k + k * t_dim1;
		    t[i__5].r = smin, t[i__5].i = 0.f;
		}
/* L100: */
	    }

	    if (ki < *n) {
		i__3 = *n - ki;
		clatrs_("Upper", "Conjugate transpose", "Non-unit", "Y", &
			i__3, &t[ki + 1 + (ki + 1) * t_dim1], ldt, &work[ki + 
			1 + iv * *n], &scale, &rwork[1], info);
		i__3 = ki + iv * *n;
		work[i__3].r = scale, work[i__3].i = 0.f;
	    }

/*           Copy the vector x or Q*x to VL and normalize. */

	    if (! over) {
/*              ------------------------------ */
/*              no back-transform: copy x to VL and normalize. */
		i__3 = *n - ki + 1;
		ccopy_(&i__3, &work[ki + iv * *n], &c__1, &vl[ki + is * 
			vl_dim1], &c__1);

		i__3 = *n - ki + 1;
		ii = icamax_(&i__3, &vl[ki + is * vl_dim1], &c__1) + ki - 1;
		i__3 = ii + is * vl_dim1;
		remax = 1.f / ((r__1 = vl[i__3].r, abs(r__1)) + (r__2 = 
			r_imag(&vl[ii + is * vl_dim1]), abs(r__2)));
		i__3 = *n - ki + 1;
		csscal_(&i__3, &remax, &vl[ki + is * vl_dim1], &c__1);

		i__3 = ki - 1;
		for (k = 1; k <= i__3; ++k) {
		    i__4 = k + is * vl_dim1;
		    vl[i__4].r = 0.f, vl[i__4].i = 0.f;
/* L110: */
		}

	    } else if (nb == 1) {
/*              ------------------------------ */
/*              version 1: back-transform each vector with GEMV, Q*x. */
		if (ki < *n) {
		    i__3 = *n - ki;
		    q__1.r = scale, q__1.i = 0.f;
		    cgemv_("N", n, &i__3, &c_b2, &vl[(ki + 1) * vl_dim1 + 1], 
			    ldvl, &work[ki + 1 + iv * *n], &c__1, &q__1, &vl[
			    ki * vl_dim1 + 1], &c__1);
		}

		ii = icamax_(n, &vl[ki * vl_dim1 + 1], &c__1);
		i__3 = ii + ki * vl_dim1;
		remax = 1.f / ((r__1 = vl[i__3].r, abs(r__1)) + (r__2 = 
			r_imag(&vl[ii + ki * vl_dim1]), abs(r__2)));
		csscal_(n, &remax, &vl[ki * vl_dim1 + 1], &c__1);

	    } else {
/*              ------------------------------ */
/*              version 2: back-transform block of vectors with GEMM */
/*              zero out above vector */
/*              could go from KI-NV+1 to KI-1 */
		i__3 = ki - 1;
		for (k = 1; k <= i__3; ++k) {
		    i__4 = k + iv * *n;
		    work[i__4].r = 0.f, work[i__4].i = 0.f;
		}

/*              Columns 1:IV of work are valid vectors. */
/*              When the number of vectors stored reaches NB, */
/*              or if this was last vector, do the GEMM */
		if (iv == nb || ki == *n) {
		    i__3 = *n - ki + iv;
		    cgemm_("N", "N", n, &iv, &i__3, &c_b2, &vl[(ki - iv + 1) *
			     vl_dim1 + 1], ldvl, &work[ki - iv + 1 + *n], n, &
			    c_b1, &work[(nb + 1) * *n + 1], n);
/*                 normalize vectors */
		    i__3 = iv;
		    for (k = 1; k <= i__3; ++k) {
			ii = icamax_(n, &work[(nb + k) * *n + 1], &c__1);
			i__4 = ii + (nb + k) * *n;
			remax = 1.f / ((r__1 = work[i__4].r, abs(r__1)) + (
				r__2 = r_imag(&work[ii + (nb + k) * *n]), abs(
				r__2)));
			csscal_(n, &remax, &work[(nb + k) * *n + 1], &c__1);
		    }
		    clacpy_("F", n, &iv, &work[(nb + 1) * *n + 1], n, &vl[(ki 
			    - iv + 1) * vl_dim1 + 1], ldvl);
		    iv = 1;
		} else {
		    ++iv;
		}
	    }

/*           Restore the original diagonal elements of T. */

	    i__3 = *n;
	    for (k = ki + 1; k <= i__3; ++k) {
		i__4 = k + k * t_dim1;
		i__5 = k;
		t[i__4].r = work[i__5].r, t[i__4].i = work[i__5].i;
/* L120: */
	    }

	    ++is;
L130:
	    ;
	}
    }

    return;

/*     End of CTREVC3 */

} /* ctrevc3_ */


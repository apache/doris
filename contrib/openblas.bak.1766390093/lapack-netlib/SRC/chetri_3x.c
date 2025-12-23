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

static complex c_b1 = {1.f,0.f};
static complex c_b2 = {0.f,0.f};

/* > \brief \b CHETRI_3X */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CHETRI_3X + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chetri_
3x.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chetri_
3x.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chetri_
3x.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CHETRI_3X( UPLO, N, A, LDA, E, IPIV, WORK, NB, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            INFO, LDA, N, NB */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            A( LDA, * ),  E( * ), WORK( N+NB+1, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > CHETRI_3X computes the inverse of a complex Hermitian indefinite */
/* > matrix A using the factorization computed by CHETRF_RK or CHETRF_BK: */
/* > */
/* >     A = P*U*D*(U**H)*(P**T) or A = P*L*D*(L**H)*(P**T), */
/* > */
/* > where U (or L) is unit upper (or lower) triangular matrix, */
/* > U**H (or L**H) is the conjugate of U (or L), P is a permutation */
/* > matrix, P**T is the transpose of P, and D is Hermitian and block */
/* > diagonal with 1-by-1 and 2-by-2 diagonal blocks. */
/* > */
/* > This is the blocked version of the algorithm, calling Level 3 BLAS. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          Specifies whether the details of the factorization are */
/* >          stored as an upper or lower triangular matrix. */
/* >          = 'U':  Upper triangle of A is stored; */
/* >          = 'L':  Lower triangle of A is stored. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >          On entry, diagonal of the block diagonal matrix D and */
/* >          factors U or L as computed by CHETRF_RK and CHETRF_BK: */
/* >            a) ONLY diagonal elements of the Hermitian block diagonal */
/* >               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k); */
/* >               (superdiagonal (or subdiagonal) elements of D */
/* >                should be provided on entry in array E), and */
/* >            b) If UPLO = 'U': factor U in the superdiagonal part of A. */
/* >               If UPLO = 'L': factor L in the subdiagonal part of A. */
/* > */
/* >          On exit, if INFO = 0, the Hermitian inverse of the original */
/* >          matrix. */
/* >             If UPLO = 'U': the upper triangular part of the inverse */
/* >             is formed and the part of A below the diagonal is not */
/* >             referenced; */
/* >             If UPLO = 'L': the lower triangular part of the inverse */
/* >             is formed and the part of A above the diagonal is not */
/* >             referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is COMPLEX array, dimension (N) */
/* >          On entry, contains the superdiagonal (or subdiagonal) */
/* >          elements of the Hermitian block diagonal matrix D */
/* >          with 1-by-1 or 2-by-2 diagonal blocks, where */
/* >          If UPLO = 'U': E(i) = D(i-1,i), i=2:N, E(1) not referenced; */
/* >          If UPLO = 'L': E(i) = D(i+1,i), i=1:N-1, E(N) not referenced. */
/* > */
/* >          NOTE: For 1-by-1 diagonal block D(k), where */
/* >          1 <= k <= N, the element E(k) is not referenced in both */
/* >          UPLO = 'U' or UPLO = 'L' cases. */
/* > \endverbatim */
/* > */
/* > \param[in] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >          Details of the interchanges and the block structure of D */
/* >          as determined by CHETRF_RK or CHETRF_BK. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (N+NB+1,NB+3). */
/* > \endverbatim */
/* > */
/* > \param[in] NB */
/* > \verbatim */
/* >          NB is INTEGER */
/* >          Block size. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          > 0: if INFO = i, D(i,i) = 0; the matrix is singular and its */
/* >               inverse could not be computed. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2017 */

/* > \ingroup complexHEcomputational */

/* > \par Contributors: */
/*  ================== */
/* > \verbatim */
/* > */
/* >  June 2017,  Igor Kozachenko, */
/* >                  Computer Science Division, */
/* >                  University of California, Berkeley */
/* > */
/* > \endverbatim */

/*  ===================================================================== */
/* Subroutine */ void chetri_3x_(char *uplo, integer *n, complex *a, integer *
	lda, complex *e, integer *ipiv, complex *work, integer *nb, integer *
	info)
{
    /* System generated locals */
    integer a_dim1, a_offset, work_dim1, work_offset, i__1, i__2, i__3, i__4, 
	    i__5, i__6;
    real r__1;
    complex q__1, q__2, q__3;

    /* Local variables */
    integer invd;
    extern /* Subroutine */ void cheswapr_(char *, integer *, complex *, 
	    integer *, integer *, integer *);
    complex akkp1, d__;
    integer i__, j, k;
    real t;
    extern /* Subroutine */ void cgemm_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void ctrmm_(char *, char *, char *, char *, 
	    integer *, integer *, complex *, complex *, integer *, complex *, 
	    integer *);
    logical upper;
    real ak;
    complex u01_i_j__;
    integer u11;
    complex u11_i_j__;
    integer ip;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer icount;
    extern /* Subroutine */ int ctrtri_(char *, char *, integer *, complex *, 
	    integer *, integer *);
    integer nnb, cut;
    real akp1;
    complex u01_ip1_j__, u11_ip1_j__;


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2017 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --e;
    --ipiv;
    work_dim1 = *n + *nb + 1;
    work_offset = 1 + work_dim1 * 1;
    work -= work_offset;

    /* Function Body */
    *info = 0;
    upper = lsame_(uplo, "U");
    if (! upper && ! lsame_(uplo, "L")) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -4;
    }

/*     Quick return if possible */

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CHETRI_3X", &i__1, (ftnlen)9);
	return;
    }
    if (*n == 0) {
	return;
    }

/*     Workspace got Non-diag elements of D */

    i__1 = *n;
    for (k = 1; k <= i__1; ++k) {
	i__2 = k + work_dim1;
	i__3 = k;
	work[i__2].r = e[i__3].r, work[i__2].i = e[i__3].i;
    }

/*     Check that the diagonal matrix D is nonsingular. */

    if (upper) {

/*        Upper triangular storage: examine D from bottom to top */

	for (*info = *n; *info >= 1; --(*info)) {
	    i__1 = *info + *info * a_dim1;
	    if (ipiv[*info] > 0 && (a[i__1].r == 0.f && a[i__1].i == 0.f)) {
		return;
	    }
	}
    } else {

/*        Lower triangular storage: examine D from top to bottom. */

	i__1 = *n;
	for (*info = 1; *info <= i__1; ++(*info)) {
	    i__2 = *info + *info * a_dim1;
	    if (ipiv[*info] > 0 && (a[i__2].r == 0.f && a[i__2].i == 0.f)) {
		return;
	    }
	}
    }

    *info = 0;

/*     Splitting Workspace */
/*     U01 is a block ( N, NB+1 ) */
/*     The first element of U01 is in WORK( 1, 1 ) */
/*     U11 is a block ( NB+1, NB+1 ) */
/*     The first element of U11 is in WORK( N+1, 1 ) */

    u11 = *n;

/*     INVD is a block ( N, 2 ) */
/*     The first element of INVD is in WORK( 1, INVD ) */

    invd = *nb + 2;
    if (upper) {

/*        Begin Upper */

/*        invA = P * inv(U**H) * inv(D) * inv(U) * P**T. */

	ctrtri_(uplo, "U", n, &a[a_offset], lda, info);

/*        inv(D) and inv(D) * inv(U) */

	k = 1;
	while(k <= *n) {
	    if (ipiv[k] > 0) {
/*              1 x 1 diagonal NNB */
		i__1 = k + invd * work_dim1;
		i__2 = k + k * a_dim1;
		r__1 = 1.f / a[i__2].r;
		work[i__1].r = r__1, work[i__1].i = 0.f;
		i__1 = k + (invd + 1) * work_dim1;
		work[i__1].r = 0.f, work[i__1].i = 0.f;
	    } else {
/*              2 x 2 diagonal NNB */
		t = c_abs(&work[k + 1 + work_dim1]);
		i__1 = k + k * a_dim1;
		ak = a[i__1].r / t;
		i__1 = k + 1 + (k + 1) * a_dim1;
		akp1 = a[i__1].r / t;
		i__1 = k + 1 + work_dim1;
		q__1.r = work[i__1].r / t, q__1.i = work[i__1].i / t;
		akkp1.r = q__1.r, akkp1.i = q__1.i;
		r__1 = ak * akp1;
		q__2.r = r__1 - 1.f, q__2.i = 0.f;
		q__1.r = t * q__2.r, q__1.i = t * q__2.i;
		d__.r = q__1.r, d__.i = q__1.i;
		i__1 = k + invd * work_dim1;
		q__2.r = akp1, q__2.i = 0.f;
		c_div(&q__1, &q__2, &d__);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		i__1 = k + 1 + (invd + 1) * work_dim1;
		q__2.r = ak, q__2.i = 0.f;
		c_div(&q__1, &q__2, &d__);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		i__1 = k + (invd + 1) * work_dim1;
		q__2.r = -akkp1.r, q__2.i = -akkp1.i;
		c_div(&q__1, &q__2, &d__);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		i__1 = k + 1 + invd * work_dim1;
		r_cnjg(&q__1, &work[k + (invd + 1) * work_dim1]);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		++k;
	    }
	    ++k;
	}

/*        inv(U**H) = (inv(U))**H */

/*        inv(U**H) * inv(D) * inv(U) */

	cut = *n;
	while(cut > 0) {
	    nnb = *nb;
	    if (cut <= nnb) {
		nnb = cut;
	    } else {
		icount = 0;
/*              count negative elements, */
		i__1 = cut;
		for (i__ = cut + 1 - nnb; i__ <= i__1; ++i__) {
		    if (ipiv[i__] < 0) {
			++icount;
		    }
		}
/*              need a even number for a clear cut */
		if (icount % 2 == 1) {
		    ++nnb;
		}
	    }
	    cut -= nnb;

/*           U01 Block */

	    i__1 = cut;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = nnb;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = i__ + j * work_dim1;
		    i__4 = i__ + (cut + j) * a_dim1;
		    work[i__3].r = a[i__4].r, work[i__3].i = a[i__4].i;
		}
	    }

/*           U11 Block */

	    i__1 = nnb;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = u11 + i__ + i__ * work_dim1;
		work[i__2].r = 1.f, work[i__2].i = 0.f;
		i__2 = i__ - 1;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = u11 + i__ + j * work_dim1;
		    work[i__3].r = 0.f, work[i__3].i = 0.f;
		}
		i__2 = nnb;
		for (j = i__ + 1; j <= i__2; ++j) {
		    i__3 = u11 + i__ + j * work_dim1;
		    i__4 = cut + i__ + (cut + j) * a_dim1;
		    work[i__3].r = a[i__4].r, work[i__3].i = a[i__4].i;
		}
	    }

/*           invD * U01 */

	    i__ = 1;
	    while(i__ <= cut) {
		if (ipiv[i__] > 0) {
		    i__1 = nnb;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = i__ + j * work_dim1;
			i__3 = i__ + invd * work_dim1;
			i__4 = i__ + j * work_dim1;
			q__1.r = work[i__3].r * work[i__4].r - work[i__3].i * 
				work[i__4].i, q__1.i = work[i__3].r * work[
				i__4].i + work[i__3].i * work[i__4].r;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		} else {
		    i__1 = nnb;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = i__ + j * work_dim1;
			u01_i_j__.r = work[i__2].r, u01_i_j__.i = work[i__2]
				.i;
			i__2 = i__ + 1 + j * work_dim1;
			u01_ip1_j__.r = work[i__2].r, u01_ip1_j__.i = work[
				i__2].i;
			i__2 = i__ + j * work_dim1;
			i__3 = i__ + invd * work_dim1;
			q__2.r = work[i__3].r * u01_i_j__.r - work[i__3].i * 
				u01_i_j__.i, q__2.i = work[i__3].r * 
				u01_i_j__.i + work[i__3].i * u01_i_j__.r;
			i__4 = i__ + (invd + 1) * work_dim1;
			q__3.r = work[i__4].r * u01_ip1_j__.r - work[i__4].i *
				 u01_ip1_j__.i, q__3.i = work[i__4].r * 
				u01_ip1_j__.i + work[i__4].i * u01_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
			i__2 = i__ + 1 + j * work_dim1;
			i__3 = i__ + 1 + invd * work_dim1;
			q__2.r = work[i__3].r * u01_i_j__.r - work[i__3].i * 
				u01_i_j__.i, q__2.i = work[i__3].r * 
				u01_i_j__.i + work[i__3].i * u01_i_j__.r;
			i__4 = i__ + 1 + (invd + 1) * work_dim1;
			q__3.r = work[i__4].r * u01_ip1_j__.r - work[i__4].i *
				 u01_ip1_j__.i, q__3.i = work[i__4].r * 
				u01_ip1_j__.i + work[i__4].i * u01_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		    ++i__;
		}
		++i__;
	    }

/*           invD1 * U11 */

	    i__ = 1;
	    while(i__ <= nnb) {
		if (ipiv[cut + i__] > 0) {
		    i__1 = nnb;
		    for (j = i__; j <= i__1; ++j) {
			i__2 = u11 + i__ + j * work_dim1;
			i__3 = cut + i__ + invd * work_dim1;
			i__4 = u11 + i__ + j * work_dim1;
			q__1.r = work[i__3].r * work[i__4].r - work[i__3].i * 
				work[i__4].i, q__1.i = work[i__3].r * work[
				i__4].i + work[i__3].i * work[i__4].r;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		} else {
		    i__1 = nnb;
		    for (j = i__; j <= i__1; ++j) {
			i__2 = u11 + i__ + j * work_dim1;
			u11_i_j__.r = work[i__2].r, u11_i_j__.i = work[i__2]
				.i;
			i__2 = u11 + i__ + 1 + j * work_dim1;
			u11_ip1_j__.r = work[i__2].r, u11_ip1_j__.i = work[
				i__2].i;
			i__2 = u11 + i__ + j * work_dim1;
			i__3 = cut + i__ + invd * work_dim1;
			i__4 = u11 + i__ + j * work_dim1;
			q__2.r = work[i__3].r * work[i__4].r - work[i__3].i * 
				work[i__4].i, q__2.i = work[i__3].r * work[
				i__4].i + work[i__3].i * work[i__4].r;
			i__5 = cut + i__ + (invd + 1) * work_dim1;
			i__6 = u11 + i__ + 1 + j * work_dim1;
			q__3.r = work[i__5].r * work[i__6].r - work[i__5].i * 
				work[i__6].i, q__3.i = work[i__5].r * work[
				i__6].i + work[i__5].i * work[i__6].r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
			i__2 = u11 + i__ + 1 + j * work_dim1;
			i__3 = cut + i__ + 1 + invd * work_dim1;
			q__2.r = work[i__3].r * u11_i_j__.r - work[i__3].i * 
				u11_i_j__.i, q__2.i = work[i__3].r * 
				u11_i_j__.i + work[i__3].i * u11_i_j__.r;
			i__4 = cut + i__ + 1 + (invd + 1) * work_dim1;
			q__3.r = work[i__4].r * u11_ip1_j__.r - work[i__4].i *
				 u11_ip1_j__.i, q__3.i = work[i__4].r * 
				u11_ip1_j__.i + work[i__4].i * u11_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		    ++i__;
		}
		++i__;
	    }

/*           U11**H * invD1 * U11 -> U11 */

	    i__1 = *n + *nb + 1;
	    ctrmm_("L", "U", "C", "U", &nnb, &nnb, &c_b1, &a[cut + 1 + (cut + 
		    1) * a_dim1], lda, &work[u11 + 1 + work_dim1], &i__1);

	    i__1 = nnb;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = nnb;
		for (j = i__; j <= i__2; ++j) {
		    i__3 = cut + i__ + (cut + j) * a_dim1;
		    i__4 = u11 + i__ + j * work_dim1;
		    a[i__3].r = work[i__4].r, a[i__3].i = work[i__4].i;
		}
	    }

/*           U01**H * invD * U01 -> A( CUT+I, CUT+J ) */

	    i__1 = *n + *nb + 1;
	    i__2 = *n + *nb + 1;
	    cgemm_("C", "N", &nnb, &nnb, &cut, &c_b1, &a[(cut + 1) * a_dim1 + 
		    1], lda, &work[work_offset], &i__1, &c_b2, &work[u11 + 1 
		    + work_dim1], &i__2);

/*           U11 =  U11**H * invD1 * U11 + U01**H * invD * U01 */

	    i__1 = nnb;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = nnb;
		for (j = i__; j <= i__2; ++j) {
		    i__3 = cut + i__ + (cut + j) * a_dim1;
		    i__4 = cut + i__ + (cut + j) * a_dim1;
		    i__5 = u11 + i__ + j * work_dim1;
		    q__1.r = a[i__4].r + work[i__5].r, q__1.i = a[i__4].i + 
			    work[i__5].i;
		    a[i__3].r = q__1.r, a[i__3].i = q__1.i;
		}
	    }

/*           U01 =  U00**H * invD0 * U01 */

	    i__1 = *n + *nb + 1;
	    ctrmm_("L", uplo, "C", "U", &cut, &nnb, &c_b1, &a[a_offset], lda, 
		    &work[work_offset], &i__1);

/*           Update U01 */

	    i__1 = cut;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = nnb;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = i__ + (cut + j) * a_dim1;
		    i__4 = i__ + j * work_dim1;
		    a[i__3].r = work[i__4].r, a[i__3].i = work[i__4].i;
		}
	    }

/*           Next Block */

	}

/*        Apply PERMUTATIONS P and P**T: */
/*        P * inv(U**H) * inv(D) * inv(U) * P**T. */
/*        Interchange rows and columns I and IPIV(I) in reverse order */
/*        from the formation order of IPIV vector for Upper case. */

/*        ( We can use a loop over IPIV with increment 1, */
/*        since the ABS value of IPIV(I) represents the row (column) */
/*        index of the interchange with row (column) i in both 1x1 */
/*        and 2x2 pivot cases, i.e. we don't need separate code branches */
/*        for 1x1 and 2x2 pivot cases ) */

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    ip = (i__2 = ipiv[i__], abs(i__2));
	    if (ip != i__) {
		if (i__ < ip) {
		    cheswapr_(uplo, n, &a[a_offset], lda, &i__, &ip);
		}
		if (i__ > ip) {
		    cheswapr_(uplo, n, &a[a_offset], lda, &ip, &i__);
		}
	    }
	}

    } else {

/*        Begin Lower */

/*        inv A = P * inv(L**H) * inv(D) * inv(L) * P**T. */

	ctrtri_(uplo, "U", n, &a[a_offset], lda, info);

/*        inv(D) and inv(D) * inv(L) */

	k = *n;
	while(k >= 1) {
	    if (ipiv[k] > 0) {
/*              1 x 1 diagonal NNB */
		i__1 = k + invd * work_dim1;
		i__2 = k + k * a_dim1;
		r__1 = 1.f / a[i__2].r;
		work[i__1].r = r__1, work[i__1].i = 0.f;
		i__1 = k + (invd + 1) * work_dim1;
		work[i__1].r = 0.f, work[i__1].i = 0.f;
	    } else {
/*              2 x 2 diagonal NNB */
		t = c_abs(&work[k - 1 + work_dim1]);
		i__1 = k - 1 + (k - 1) * a_dim1;
		ak = a[i__1].r / t;
		i__1 = k + k * a_dim1;
		akp1 = a[i__1].r / t;
		i__1 = k - 1 + work_dim1;
		q__1.r = work[i__1].r / t, q__1.i = work[i__1].i / t;
		akkp1.r = q__1.r, akkp1.i = q__1.i;
		r__1 = ak * akp1;
		q__2.r = r__1 - 1.f, q__2.i = 0.f;
		q__1.r = t * q__2.r, q__1.i = t * q__2.i;
		d__.r = q__1.r, d__.i = q__1.i;
		i__1 = k - 1 + invd * work_dim1;
		q__2.r = akp1, q__2.i = 0.f;
		c_div(&q__1, &q__2, &d__);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		i__1 = k + invd * work_dim1;
		q__2.r = ak, q__2.i = 0.f;
		c_div(&q__1, &q__2, &d__);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		i__1 = k + (invd + 1) * work_dim1;
		q__2.r = -akkp1.r, q__2.i = -akkp1.i;
		c_div(&q__1, &q__2, &d__);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		i__1 = k - 1 + (invd + 1) * work_dim1;
		r_cnjg(&q__1, &work[k + (invd + 1) * work_dim1]);
		work[i__1].r = q__1.r, work[i__1].i = q__1.i;
		--k;
	    }
	    --k;
	}

/*        inv(L**H) = (inv(L))**H */

/*        inv(L**H) * inv(D) * inv(L) */

	cut = 0;
	while(cut < *n) {
	    nnb = *nb;
	    if (cut + nnb > *n) {
		nnb = *n - cut;
	    } else {
		icount = 0;
/*              count negative elements, */
		i__1 = cut + nnb;
		for (i__ = cut + 1; i__ <= i__1; ++i__) {
		    if (ipiv[i__] < 0) {
			++icount;
		    }
		}
/*              need a even number for a clear cut */
		if (icount % 2 == 1) {
		    ++nnb;
		}
	    }

/*           L21 Block */

	    i__1 = *n - cut - nnb;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = nnb;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = i__ + j * work_dim1;
		    i__4 = cut + nnb + i__ + (cut + j) * a_dim1;
		    work[i__3].r = a[i__4].r, work[i__3].i = a[i__4].i;
		}
	    }

/*           L11 Block */

	    i__1 = nnb;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = u11 + i__ + i__ * work_dim1;
		work[i__2].r = 1.f, work[i__2].i = 0.f;
		i__2 = nnb;
		for (j = i__ + 1; j <= i__2; ++j) {
		    i__3 = u11 + i__ + j * work_dim1;
		    work[i__3].r = 0.f, work[i__3].i = 0.f;
		}
		i__2 = i__ - 1;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = u11 + i__ + j * work_dim1;
		    i__4 = cut + i__ + (cut + j) * a_dim1;
		    work[i__3].r = a[i__4].r, work[i__3].i = a[i__4].i;
		}
	    }

/*           invD*L21 */

	    i__ = *n - cut - nnb;
	    while(i__ >= 1) {
		if (ipiv[cut + nnb + i__] > 0) {
		    i__1 = nnb;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = i__ + j * work_dim1;
			i__3 = cut + nnb + i__ + invd * work_dim1;
			i__4 = i__ + j * work_dim1;
			q__1.r = work[i__3].r * work[i__4].r - work[i__3].i * 
				work[i__4].i, q__1.i = work[i__3].r * work[
				i__4].i + work[i__3].i * work[i__4].r;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		} else {
		    i__1 = nnb;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = i__ + j * work_dim1;
			u01_i_j__.r = work[i__2].r, u01_i_j__.i = work[i__2]
				.i;
			i__2 = i__ - 1 + j * work_dim1;
			u01_ip1_j__.r = work[i__2].r, u01_ip1_j__.i = work[
				i__2].i;
			i__2 = i__ + j * work_dim1;
			i__3 = cut + nnb + i__ + invd * work_dim1;
			q__2.r = work[i__3].r * u01_i_j__.r - work[i__3].i * 
				u01_i_j__.i, q__2.i = work[i__3].r * 
				u01_i_j__.i + work[i__3].i * u01_i_j__.r;
			i__4 = cut + nnb + i__ + (invd + 1) * work_dim1;
			q__3.r = work[i__4].r * u01_ip1_j__.r - work[i__4].i *
				 u01_ip1_j__.i, q__3.i = work[i__4].r * 
				u01_ip1_j__.i + work[i__4].i * u01_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
			i__2 = i__ - 1 + j * work_dim1;
			i__3 = cut + nnb + i__ - 1 + (invd + 1) * work_dim1;
			q__2.r = work[i__3].r * u01_i_j__.r - work[i__3].i * 
				u01_i_j__.i, q__2.i = work[i__3].r * 
				u01_i_j__.i + work[i__3].i * u01_i_j__.r;
			i__4 = cut + nnb + i__ - 1 + invd * work_dim1;
			q__3.r = work[i__4].r * u01_ip1_j__.r - work[i__4].i *
				 u01_ip1_j__.i, q__3.i = work[i__4].r * 
				u01_ip1_j__.i + work[i__4].i * u01_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		    --i__;
		}
		--i__;
	    }

/*           invD1*L11 */

	    i__ = nnb;
	    while(i__ >= 1) {
		if (ipiv[cut + i__] > 0) {
		    i__1 = nnb;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = u11 + i__ + j * work_dim1;
			i__3 = cut + i__ + invd * work_dim1;
			i__4 = u11 + i__ + j * work_dim1;
			q__1.r = work[i__3].r * work[i__4].r - work[i__3].i * 
				work[i__4].i, q__1.i = work[i__3].r * work[
				i__4].i + work[i__3].i * work[i__4].r;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		} else {
		    i__1 = nnb;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = u11 + i__ + j * work_dim1;
			u11_i_j__.r = work[i__2].r, u11_i_j__.i = work[i__2]
				.i;
			i__2 = u11 + i__ - 1 + j * work_dim1;
			u11_ip1_j__.r = work[i__2].r, u11_ip1_j__.i = work[
				i__2].i;
			i__2 = u11 + i__ + j * work_dim1;
			i__3 = cut + i__ + invd * work_dim1;
			i__4 = u11 + i__ + j * work_dim1;
			q__2.r = work[i__3].r * work[i__4].r - work[i__3].i * 
				work[i__4].i, q__2.i = work[i__3].r * work[
				i__4].i + work[i__3].i * work[i__4].r;
			i__5 = cut + i__ + (invd + 1) * work_dim1;
			q__3.r = work[i__5].r * u11_ip1_j__.r - work[i__5].i *
				 u11_ip1_j__.i, q__3.i = work[i__5].r * 
				u11_ip1_j__.i + work[i__5].i * u11_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
			i__2 = u11 + i__ - 1 + j * work_dim1;
			i__3 = cut + i__ - 1 + (invd + 1) * work_dim1;
			q__2.r = work[i__3].r * u11_i_j__.r - work[i__3].i * 
				u11_i_j__.i, q__2.i = work[i__3].r * 
				u11_i_j__.i + work[i__3].i * u11_i_j__.r;
			i__4 = cut + i__ - 1 + invd * work_dim1;
			q__3.r = work[i__4].r * u11_ip1_j__.r - work[i__4].i *
				 u11_ip1_j__.i, q__3.i = work[i__4].r * 
				u11_ip1_j__.i + work[i__4].i * u11_ip1_j__.r;
			q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
			work[i__2].r = q__1.r, work[i__2].i = q__1.i;
		    }
		    --i__;
		}
		--i__;
	    }

/*           L11**H * invD1 * L11 -> L11 */

	    i__1 = *n + *nb + 1;
	    ctrmm_("L", uplo, "C", "U", &nnb, &nnb, &c_b1, &a[cut + 1 + (cut 
		    + 1) * a_dim1], lda, &work[u11 + 1 + work_dim1], &i__1);

	    i__1 = nnb;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = i__;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = cut + i__ + (cut + j) * a_dim1;
		    i__4 = u11 + i__ + j * work_dim1;
		    a[i__3].r = work[i__4].r, a[i__3].i = work[i__4].i;
		}
	    }

	    if (cut + nnb < *n) {

/*              L21**H * invD2*L21 -> A( CUT+I, CUT+J ) */

		i__1 = *n - nnb - cut;
		i__2 = *n + *nb + 1;
		i__3 = *n + *nb + 1;
		cgemm_("C", "N", &nnb, &nnb, &i__1, &c_b1, &a[cut + nnb + 1 + 
			(cut + 1) * a_dim1], lda, &work[work_offset], &i__2, &
			c_b2, &work[u11 + 1 + work_dim1], &i__3);

/*              L11 =  L11**H * invD1 * L11 + U01**H * invD * U01 */

		i__1 = nnb;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    i__2 = i__;
		    for (j = 1; j <= i__2; ++j) {
			i__3 = cut + i__ + (cut + j) * a_dim1;
			i__4 = cut + i__ + (cut + j) * a_dim1;
			i__5 = u11 + i__ + j * work_dim1;
			q__1.r = a[i__4].r + work[i__5].r, q__1.i = a[i__4].i 
				+ work[i__5].i;
			a[i__3].r = q__1.r, a[i__3].i = q__1.i;
		    }
		}

/*              L01 =  L22**H * invD2 * L21 */

		i__1 = *n - nnb - cut;
		i__2 = *n + *nb + 1;
		ctrmm_("L", uplo, "C", "U", &i__1, &nnb, &c_b1, &a[cut + nnb 
			+ 1 + (cut + nnb + 1) * a_dim1], lda, &work[
			work_offset], &i__2);

/*              Update L21 */

		i__1 = *n - cut - nnb;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    i__2 = nnb;
		    for (j = 1; j <= i__2; ++j) {
			i__3 = cut + nnb + i__ + (cut + j) * a_dim1;
			i__4 = i__ + j * work_dim1;
			a[i__3].r = work[i__4].r, a[i__3].i = work[i__4].i;
		    }
		}

	    } else {

/*              L11 =  L11**H * invD1 * L11 */

		i__1 = nnb;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    i__2 = i__;
		    for (j = 1; j <= i__2; ++j) {
			i__3 = cut + i__ + (cut + j) * a_dim1;
			i__4 = u11 + i__ + j * work_dim1;
			a[i__3].r = work[i__4].r, a[i__3].i = work[i__4].i;
		    }
		}
	    }

/*           Next Block */

	    cut += nnb;

	}

/*        Apply PERMUTATIONS P and P**T: */
/*        P * inv(L**H) * inv(D) * inv(L) * P**T. */
/*        Interchange rows and columns I and IPIV(I) in reverse order */
/*        from the formation order of IPIV vector for Lower case. */

/*        ( We can use a loop over IPIV with increment -1, */
/*        since the ABS value of IPIV(I) represents the row (column) */
/*        index of the interchange with row (column) i in both 1x1 */
/*        and 2x2 pivot cases, i.e. we don't need separate code branches */
/*        for 1x1 and 2x2 pivot cases ) */

	for (i__ = *n; i__ >= 1; --i__) {
	    ip = (i__1 = ipiv[i__], abs(i__1));
	    if (ip != i__) {
		if (i__ < ip) {
		    cheswapr_(uplo, n, &a[a_offset], lda, &i__, &ip);
		}
		if (i__ > ip) {
		    cheswapr_(uplo, n, &a[a_offset], lda, &ip, &i__);
		}
	    }
	}

    }

    return;

/*     End of CHETRI_3X */

} /* chetri_3x__ */


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

/* > \brief \b SLANSF */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLANSF + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slansf.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slansf.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slansf.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       REAL FUNCTION SLANSF( NORM, TRANSR, UPLO, N, A, WORK ) */

/*       CHARACTER          NORM, TRANSR, UPLO */
/*       INTEGER            N */
/*       REAL               A( 0: * ), WORK( 0: * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLANSF returns the value of the one norm, or the Frobenius norm, or */
/* > the infinity norm, or the element of largest absolute value of a */
/* > real symmetric matrix A in RFP format. */
/* > \endverbatim */
/* > */
/* > \return SLANSF */
/* > \verbatim */
/* > */
/* >    SLANSF = ( f2cmax(abs(A(i,j))), NORM = 'M' or 'm' */
/* >             ( */
/* >             ( norm1(A),         NORM = '1', 'O' or 'o' */
/* >             ( */
/* >             ( normI(A),         NORM = 'I' or 'i' */
/* >             ( */
/* >             ( normF(A),         NORM = 'F', 'f', 'E' or 'e' */
/* > */
/* > where  norm1  denotes the  one norm of a matrix (maximum column sum), */
/* > normI  denotes the  infinity norm  of a matrix  (maximum row sum) and */
/* > normF  denotes the  Frobenius norm of a matrix (square root of sum of */
/* > squares).  Note that  f2cmax(abs(A(i,j)))  is not a  matrix norm. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] NORM */
/* > \verbatim */
/* >          NORM is CHARACTER*1 */
/* >          Specifies the value to be returned in SLANSF as described */
/* >          above. */
/* > \endverbatim */
/* > */
/* > \param[in] TRANSR */
/* > \verbatim */
/* >          TRANSR is CHARACTER*1 */
/* >          Specifies whether the RFP format of A is normal or */
/* >          transposed format. */
/* >          = 'N':  RFP format is Normal; */
/* >          = 'T':  RFP format is Transpose. */
/* > \endverbatim */
/* > */
/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >           On entry, UPLO specifies whether the RFP matrix A came from */
/* >           an upper or lower triangular matrix as follows: */
/* >           = 'U': RFP A came from an upper triangular matrix; */
/* >           = 'L': RFP A came from a lower triangular matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A. N >= 0. When N = 0, SLANSF is */
/* >          set to zero. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is REAL array, dimension ( N*(N+1)/2 ); */
/* >          On entry, the upper (if UPLO = 'U') or lower (if UPLO = 'L') */
/* >          part of the symmetric matrix A stored in RFP format. See the */
/* >          "Notes" below for more details. */
/* >          Unchanged on exit. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)), */
/* >          where LWORK >= N when NORM = 'I' or '1' or 'O'; otherwise, */
/* >          WORK is not referenced. */
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
real slansf_(char *norm, char *transr, char *uplo, integer *n, real *a, real *
	work)
{
    /* System generated locals */
    integer i__1, i__2;
    real ret_val, r__1;

    /* Local variables */
    real temp;
    integer i__, j, k, l;
    real s, scale;
    extern logical lsame_(char *, char *);
    real value;
    integer n1;
    real aa;
    extern logical sisnan_(real *);
    extern /* Subroutine */ void slassq_(integer *, real *, integer *, real *, 
	    real *);
    integer lda, ifm, noe, ilu;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    if (*n == 0) {
	ret_val = 0.f;
	return ret_val;
    } else if (*n == 1) {
	ret_val = abs(a[0]);
	return ret_val;
    }

/*     set noe = 1 if n is odd. if n is even set noe=0 */

    noe = 1;
    if (*n % 2 == 0) {
	noe = 0;
    }

/*     set ifm = 0 when form='T or 't' and 1 otherwise */

    ifm = 1;
    if (lsame_(transr, "T")) {
	ifm = 0;
    }

/*     set ilu = 0 when uplo='U or 'u' and 1 otherwise */

    ilu = 1;
    if (lsame_(uplo, "U")) {
	ilu = 0;
    }

/*     set lda = (n+1)/2 when ifm = 0 */
/*     set lda = n when ifm = 1 and noe = 1 */
/*     set lda = n+1 when ifm = 1 and noe = 0 */

    if (ifm == 1) {
	if (noe == 1) {
	    lda = *n;
	} else {
/*           noe=0 */
	    lda = *n + 1;
	}
    } else {
/*        ifm=0 */
	lda = (*n + 1) / 2;
    }

    if (lsame_(norm, "M")) {

/*       Find f2cmax(abs(A(i,j))). */

	k = (*n + 1) / 2;
	value = 0.f;
	if (noe == 1) {
/*           n is odd */
	    if (ifm == 1) {
/*           A is n by k */
		i__1 = k - 1;
		for (j = 0; j <= i__1; ++j) {
		    i__2 = *n - 1;
		    for (i__ = 0; i__ <= i__2; ++i__) {
			temp = (r__1 = a[i__ + j * lda], abs(r__1));
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    } else {
/*              xpose case; A is k by n */
		i__1 = *n - 1;
		for (j = 0; j <= i__1; ++j) {
		    i__2 = k - 1;
		    for (i__ = 0; i__ <= i__2; ++i__) {
			temp = (r__1 = a[i__ + j * lda], abs(r__1));
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    }
	} else {
/*           n is even */
	    if (ifm == 1) {
/*              A is n+1 by k */
		i__1 = k - 1;
		for (j = 0; j <= i__1; ++j) {
		    i__2 = *n;
		    for (i__ = 0; i__ <= i__2; ++i__) {
			temp = (r__1 = a[i__ + j * lda], abs(r__1));
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    } else {
/*              xpose case; A is k by n+1 */
		i__1 = *n;
		for (j = 0; j <= i__1; ++j) {
		    i__2 = k - 1;
		    for (i__ = 0; i__ <= i__2; ++i__) {
			temp = (r__1 = a[i__ + j * lda], abs(r__1));
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    }
	}
    } else if (lsame_(norm, "I") || lsame_(norm, "O") || *(unsigned char *)norm == '1') {

/*        Find normI(A) ( = norm1(A), since A is symmetric). */

	if (ifm == 1) {
	    k = *n / 2;
	    if (noe == 1) {
/*              n is odd */
		if (ilu == 0) {
		    i__1 = k - 1;
		    for (i__ = 0; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    i__1 = k;
		    for (j = 0; j <= i__1; ++j) {
			s = 0.f;
			i__2 = k + j - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(i,j+k) */
			    s += aa;
			    work[i__] += aa;
			}
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j+k,j+k) */
			work[j + k] = s + aa;
			if (i__ == k + k) {
			    goto L10;
			}
			++i__;
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j,j) */
			work[j] += aa;
			s = 0.f;
			i__2 = k - 1;
			for (l = j + 1; l <= i__2; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(l,j) */
			    s += aa;
			    work[l] += aa;
			}
			work[j] += s;
		    }
L10:
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		} else {
/*                 ilu = 1 */
		    ++k;
/*                 k=(n+1)/2 for n odd and ilu=1 */
		    i__1 = *n - 1;
		    for (i__ = k; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    for (j = k - 1; j >= 0; --j) {
			s = 0.f;
			i__1 = j - 2;
			for (i__ = 0; i__ <= i__1; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(j+k,i+k) */
			    s += aa;
			    work[i__ + k] += aa;
			}
			if (j > 0) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(j+k,j+k) */
			    s += aa;
			    work[i__ + k] += s;
/*                       i=j */
			    ++i__;
			}
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j,j) */
			work[j] = aa;
			s = 0.f;
			i__1 = *n - 1;
			for (l = j + 1; l <= i__1; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(l,j) */
			    s += aa;
			    work[l] += aa;
			}
			work[j] += s;
		    }
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    } else {
/*              n is even */
		if (ilu == 0) {
		    i__1 = k - 1;
		    for (i__ = 0; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			s = 0.f;
			i__2 = k + j - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(i,j+k) */
			    s += aa;
			    work[i__] += aa;
			}
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j+k,j+k) */
			work[j + k] = s + aa;
			++i__;
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j,j) */
			work[j] += aa;
			s = 0.f;
			i__2 = k - 1;
			for (l = j + 1; l <= i__2; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(l,j) */
			    s += aa;
			    work[l] += aa;
			}
			work[j] += s;
		    }
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		} else {
/*                 ilu = 1 */
		    i__1 = *n - 1;
		    for (i__ = k; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    for (j = k - 1; j >= 0; --j) {
			s = 0.f;
			i__1 = j - 1;
			for (i__ = 0; i__ <= i__1; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(j+k,i+k) */
			    s += aa;
			    work[i__ + k] += aa;
			}
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j+k,j+k) */
			s += aa;
			work[i__ + k] += s;
/*                    i=j */
			++i__;
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    -> A(j,j) */
			work[j] = aa;
			s = 0.f;
			i__1 = *n - 1;
			for (l = j + 1; l <= i__1; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       -> A(l,j) */
			    s += aa;
			    work[l] += aa;
			}
			work[j] += s;
		    }
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    }
	} else {
/*           ifm=0 */
	    k = *n / 2;
	    if (noe == 1) {
/*              n is odd */
		if (ilu == 0) {
		    n1 = k;
/*                 n/2 */
		    ++k;
/*                 k is the row size and lda */
		    i__1 = *n - 1;
		    for (i__ = n1; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    i__1 = n1 - 1;
		    for (j = 0; j <= i__1; ++j) {
			s = 0.f;
			i__2 = k - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j,n1+i) */
			    work[i__ + n1] += aa;
			    s += aa;
			}
			work[j] = s;
		    }
/*                 j=n1=k-1 is special */
		    s = (r__1 = a[j * lda], abs(r__1));
/*                 A(k-1,k-1) */
		    i__1 = k - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(k-1,i+n1) */
			work[i__ + n1] += aa;
			s += aa;
		    }
		    work[j] += s;
		    i__1 = *n - 1;
		    for (j = k; j <= i__1; ++j) {
			s = 0.f;
			i__2 = j - k - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(i,j-k) */
			    work[i__] += aa;
			    s += aa;
			}
/*                    i=j-k */
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(j-k,j-k) */
			s += aa;
			work[j - k] += s;
			++i__;
			s = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(j,j) */
			i__2 = *n - 1;
			for (l = j + 1; l <= i__2; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j,l) */
			    work[l] += aa;
			    s += aa;
			}
			work[j] += s;
		    }
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		} else {
/*                 ilu=1 */
		    ++k;
/*                 k=(n+1)/2 for n odd and ilu=1 */
		    i__1 = *n - 1;
		    for (i__ = k; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
/*                    process */
			s = 0.f;
			i__2 = j - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j,i) */
			    work[i__] += aa;
			    s += aa;
			}
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    i=j so process of A(j,j) */
			s += aa;
			work[j] = s;
/*                    is initialised here */
			++i__;
/*                    i=j process A(j+k,j+k) */
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
			s = aa;
			i__2 = *n - 1;
			for (l = k + j + 1; l <= i__2; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(l,k+j) */
			    s += aa;
			    work[l] += aa;
			}
			work[k + j] += s;
		    }
/*                 j=k-1 is special :process col A(k-1,0:k-1) */
		    s = 0.f;
		    i__1 = k - 2;
		    for (i__ = 0; i__ <= i__1; ++i__) {
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(k,i) */
			work[i__] += aa;
			s += aa;
		    }
/*                 i=k-1 */
		    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                 A(k-1,k-1) */
		    s += aa;
		    work[i__] = s;
/*                 done with col j=k+1 */
		    i__1 = *n - 1;
		    for (j = k; j <= i__1; ++j) {
/*                    process col j of A = A(j,0:k-1) */
			s = 0.f;
			i__2 = k - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j,i) */
			    work[i__] += aa;
			    s += aa;
			}
			work[j] += s;
		    }
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    } else {
/*              n is even */
		if (ilu == 0) {
		    i__1 = *n - 1;
		    for (i__ = k; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			s = 0.f;
			i__2 = k - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j,i+k) */
			    work[i__ + k] += aa;
			    s += aa;
			}
			work[j] = s;
		    }
/*                 j=k */
		    aa = (r__1 = a[j * lda], abs(r__1));
/*                 A(k,k) */
		    s = aa;
		    i__1 = k - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(k,k+i) */
			work[i__ + k] += aa;
			s += aa;
		    }
		    work[j] += s;
		    i__1 = *n - 1;
		    for (j = k + 1; j <= i__1; ++j) {
			s = 0.f;
			i__2 = j - 2 - k;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(i,j-k-1) */
			    work[i__] += aa;
			    s += aa;
			}
/*                     i=j-1-k */
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(j-k-1,j-k-1) */
			s += aa;
			work[j - k - 1] += s;
			++i__;
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(j,j) */
			s = aa;
			i__2 = *n - 1;
			for (l = j + 1; l <= i__2; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j,l) */
			    work[l] += aa;
			    s += aa;
			}
			work[j] += s;
		    }
/*                 j=n */
		    s = 0.f;
		    i__1 = k - 2;
		    for (i__ = 0; i__ <= i__1; ++i__) {
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(i,k-1) */
			work[i__] += aa;
			s += aa;
		    }
/*                 i=k-1 */
		    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                 A(k-1,k-1) */
		    s += aa;
		    work[i__] += s;
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		} else {
/*                 ilu=1 */
		    i__1 = *n - 1;
		    for (i__ = k; i__ <= i__1; ++i__) {
			work[i__] = 0.f;
		    }
/*                 j=0 is special :process col A(k:n-1,k) */
		    s = abs(a[0]);
/*                 A(k,k) */
		    i__1 = k - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			aa = (r__1 = a[i__], abs(r__1));
/*                    A(k+i,k) */
			work[i__ + k] += aa;
			s += aa;
		    }
		    work[k] += s;
		    i__1 = k - 1;
		    for (j = 1; j <= i__1; ++j) {
/*                    process */
			s = 0.f;
			i__2 = j - 2;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j-1,i) */
			    work[i__] += aa;
			    s += aa;
			}
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    i=j-1 so process of A(j-1,j-1) */
			s += aa;
			work[j - 1] = s;
/*                    is initialised here */
			++i__;
/*                    i=j process A(j+k,j+k) */
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
			s = aa;
			i__2 = *n - 1;
			for (l = k + j + 1; l <= i__2; ++l) {
			    ++i__;
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(l,k+j) */
			    s += aa;
			    work[l] += aa;
			}
			work[k + j] += s;
		    }
/*                 j=k is special :process col A(k,0:k-1) */
		    s = 0.f;
		    i__1 = k - 2;
		    for (i__ = 0; i__ <= i__1; ++i__) {
			aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                    A(k,i) */
			work[i__] += aa;
			s += aa;
		    }
/*                 i=k-1 */
		    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                 A(k-1,k-1) */
		    s += aa;
		    work[i__] = s;
/*                 done with col j=k+1 */
		    i__1 = *n;
		    for (j = k + 1; j <= i__1; ++j) {
/*                    process col j-1 of A = A(j-1,0:k-1) */
			s = 0.f;
			i__2 = k - 1;
			for (i__ = 0; i__ <= i__2; ++i__) {
			    aa = (r__1 = a[i__ + j * lda], abs(r__1));
/*                       A(j-1,i) */
			    work[i__] += aa;
			    s += aa;
			}
			work[j - 1] += s;
		    }
		    value = work[0];
		    i__1 = *n - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = work[i__];
			if (value < temp || sisnan_(&temp)) {
			    value = temp;
			}
		    }
		}
	    }
	}
    } else if (lsame_(norm, "F") || lsame_(norm, "E")) {

/*       Find normF(A). */

	k = (*n + 1) / 2;
	scale = 0.f;
	s = 1.f;
	if (noe == 1) {
/*           n is odd */
	    if (ifm == 1) {
/*              A is normal */
		if (ilu == 0) {
/*                 A is upper */
		    i__1 = k - 3;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k - j - 2;
			slassq_(&i__2, &a[k + j + 1 + j * lda], &c__1, &scale,
				 &s);
/*                    L at A(k,0) */
		    }
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k + j - 1;
			slassq_(&i__2, &a[j * lda], &c__1, &scale, &s);
/*                    trap U at A(0,0) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = k - 1;
		    i__2 = lda + 1;
		    slassq_(&i__1, &a[k], &i__2, &scale, &s);
/*                 tri L at A(k,0) */
		    i__1 = lda + 1;
		    slassq_(&k, &a[k - 1], &i__1, &scale, &s);
/*                 tri U at A(k-1,0) */
		} else {
/*                 ilu=1 & A is lower */
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = *n - j - 1;
			slassq_(&i__2, &a[j + 1 + j * lda], &c__1, &scale, &s)
				;
/*                    trap L at A(0,0) */
		    }
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
			slassq_(&j, &a[(j + 1) * lda], &c__1, &scale, &s);
/*                    U at A(0,1) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = lda + 1;
		    slassq_(&k, a, &i__1, &scale, &s);
/*                 tri L at A(0,0) */
		    i__1 = k - 1;
		    i__2 = lda + 1;
		    slassq_(&i__1, &a[lda], &i__2, &scale, &s);
/*                 tri U at A(0,1) */
		}
	    } else {
/*              A is xpose */
		if (ilu == 0) {
/*                 A**T is upper */
		    i__1 = k - 2;
		    for (j = 1; j <= i__1; ++j) {
			slassq_(&j, &a[(k + j) * lda], &c__1, &scale, &s);
/*                    U at A(0,k) */
		    }
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
			slassq_(&k, &a[j * lda], &c__1, &scale, &s);
/*                    k by k-1 rect. at A(0,0) */
		    }
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k - j - 1;
			slassq_(&i__2, &a[j + 1 + (j + k - 1) * lda], &c__1, &
				scale, &s);
/*                    L at A(0,k-1) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = k - 1;
		    i__2 = lda + 1;
		    slassq_(&i__1, &a[k * lda], &i__2, &scale, &s);
/*                 tri U at A(0,k) */
		    i__1 = lda + 1;
		    slassq_(&k, &a[(k - 1) * lda], &i__1, &scale, &s);
/*                 tri L at A(0,k-1) */
		} else {
/*                 A**T is lower */
		    i__1 = k - 1;
		    for (j = 1; j <= i__1; ++j) {
			slassq_(&j, &a[j * lda], &c__1, &scale, &s);
/*                    U at A(0,0) */
		    }
		    i__1 = *n - 1;
		    for (j = k; j <= i__1; ++j) {
			slassq_(&k, &a[j * lda], &c__1, &scale, &s);
/*                    k by k-1 rect. at A(0,k) */
		    }
		    i__1 = k - 3;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k - j - 2;
			slassq_(&i__2, &a[j + 2 + j * lda], &c__1, &scale, &s)
				;
/*                    L at A(1,0) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = lda + 1;
		    slassq_(&k, a, &i__1, &scale, &s);
/*                 tri U at A(0,0) */
		    i__1 = k - 1;
		    i__2 = lda + 1;
		    slassq_(&i__1, &a[1], &i__2, &scale, &s);
/*                 tri L at A(1,0) */
		}
	    }
	} else {
/*           n is even */
	    if (ifm == 1) {
/*              A is normal */
		if (ilu == 0) {
/*                 A is upper */
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k - j - 1;
			slassq_(&i__2, &a[k + j + 2 + j * lda], &c__1, &scale,
				 &s);
/*                    L at A(k+1,0) */
		    }
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k + j;
			slassq_(&i__2, &a[j * lda], &c__1, &scale, &s);
/*                    trap U at A(0,0) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = lda + 1;
		    slassq_(&k, &a[k + 1], &i__1, &scale, &s);
/*                 tri L at A(k+1,0) */
		    i__1 = lda + 1;
		    slassq_(&k, &a[k], &i__1, &scale, &s);
/*                 tri U at A(k,0) */
		} else {
/*                 ilu=1 & A is lower */
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = *n - j - 1;
			slassq_(&i__2, &a[j + 2 + j * lda], &c__1, &scale, &s)
				;
/*                    trap L at A(1,0) */
		    }
		    i__1 = k - 1;
		    for (j = 1; j <= i__1; ++j) {
			slassq_(&j, &a[j * lda], &c__1, &scale, &s);
/*                    U at A(0,0) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = lda + 1;
		    slassq_(&k, &a[1], &i__1, &scale, &s);
/*                 tri L at A(1,0) */
		    i__1 = lda + 1;
		    slassq_(&k, a, &i__1, &scale, &s);
/*                 tri U at A(0,0) */
		}
	    } else {
/*              A is xpose */
		if (ilu == 0) {
/*                 A**T is upper */
		    i__1 = k - 1;
		    for (j = 1; j <= i__1; ++j) {
			slassq_(&j, &a[(k + 1 + j) * lda], &c__1, &scale, &s);
/*                    U at A(0,k+1) */
		    }
		    i__1 = k - 1;
		    for (j = 0; j <= i__1; ++j) {
			slassq_(&k, &a[j * lda], &c__1, &scale, &s);
/*                    k by k rect. at A(0,0) */
		    }
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k - j - 1;
			slassq_(&i__2, &a[j + 1 + (j + k) * lda], &c__1, &
				scale, &s);
/*                    L at A(0,k) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = lda + 1;
		    slassq_(&k, &a[(k + 1) * lda], &i__1, &scale, &s);
/*                 tri U at A(0,k+1) */
		    i__1 = lda + 1;
		    slassq_(&k, &a[k * lda], &i__1, &scale, &s);
/*                 tri L at A(0,k) */
		} else {
/*                 A**T is lower */
		    i__1 = k - 1;
		    for (j = 1; j <= i__1; ++j) {
			slassq_(&j, &a[(j + 1) * lda], &c__1, &scale, &s);
/*                    U at A(0,1) */
		    }
		    i__1 = *n;
		    for (j = k + 1; j <= i__1; ++j) {
			slassq_(&k, &a[j * lda], &c__1, &scale, &s);
/*                    k by k rect. at A(0,k+1) */
		    }
		    i__1 = k - 2;
		    for (j = 0; j <= i__1; ++j) {
			i__2 = k - j - 1;
			slassq_(&i__2, &a[j + 1 + j * lda], &c__1, &scale, &s)
				;
/*                    L at A(0,0) */
		    }
		    s += s;
/*                 double s for the off diagonal elements */
		    i__1 = lda + 1;
		    slassq_(&k, &a[lda], &i__1, &scale, &s);
/*                 tri L at A(0,1) */
		    i__1 = lda + 1;
		    slassq_(&k, a, &i__1, &scale, &s);
/*                 tri U at A(0,0) */
		}
	    }
	}
	value = scale * sqrt(s);
    }

    ret_val = value;
    return ret_val;

/*     End of SLANSF */

} /* slansf_ */


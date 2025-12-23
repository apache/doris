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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
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
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)

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



/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/



/* Table of constant values */

static integer c_n1 = -1;
static integer c__1 = 1;
static integer c__0 = 0;
static integer c__2 = 2;

/* Subroutine */ int sgedmd_(char *jobs, char *jobz, char *jobr, char *jobf, 
	integer *whtsvd, integer *m, integer *n, real *x, integer *ldx, real *
	y, integer *ldy, integer *nrnk, real *tol, integer *k, real *reig, 
	real *imeig, real *z__, integer *ldz, real *res, real *b, integer *
	ldb, real *w, integer *ldw, real *s, integer *lds, real *work, 
	integer *lwork, integer *iwork, integer *liwork, integer *info)
{
    /* System generated locals */
    integer x_dim1, x_offset, y_dim1, y_offset, z_dim1, z_offset, b_dim1, 
	    b_offset, w_dim1, w_offset, s_dim1, s_offset, i__1, i__2;
    real r__1, r__2;

    /* Local variables */
    real zero, ssum;
    integer info1, info2;
    real xscl1, xscl2;
    extern real snrm2_(integer *, real *, integer *);
    integer i__, j;
    real scale;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ int sscal_(integer *, real *, real *, integer *);
    logical badxy;
    real small;
    extern /* Subroutine */ int sgemm_(char *, char *, integer *, integer *, 
	    integer *, real *, real *, integer *, real *, integer *, real *, 
	    real *, integer *), sgeev_(char *, char *, 
	    integer *, real *, integer *, real *, real *, real *, integer *, 
	    real *, integer *, real *, integer *, integer *);
    char jobzl[1];
    extern /* Subroutine */ int saxpy_(integer *, real *, real *, integer *, 
	    real *, integer *);
    logical wntex;
    real ab[4]	/* was [2][2] */;
    extern real slamch_(char *), slange_(char *, integer *, integer *,
	     real *, integer *, real *);
    extern /* Subroutine */ int sgesdd_(char *, integer *, integer *, real *, 
	    integer *, real *, real *, integer *, real *, integer *, real *, 
	    integer *, integer *, integer *), xerbla_(char *, integer 
	    *);
    char t_or_n__[1];
    extern /* Subroutine */ int slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *);
    extern integer isamax_(integer *, real *, integer *);
    logical sccolx, sccoly;
    extern logical sisnan_(real *);
    extern /* Subroutine */ int sgesvd_(char *, char *, integer *, integer *, 
	    real *, integer *, real *, real *, integer *, real *, integer *, 
	    real *, integer *, integer *);
    integer lwrsdd, mwrsdd;
    extern /* Subroutine */ int sgejsv_(char *, char *, char *, char *, char *
	    , char *, integer *, integer *, real *, integer *, real *, real *,
	     integer *, real *, integer *, real *, integer *, integer *, 
	    integer *), 
	    slacpy_(char *, integer *, integer *, real *, integer *, real *, 
	    integer *);
    integer iminwr;
    logical wntref, wntvec;
    real rootsc;
    integer lwrkev, mlwork, mwrkev, numrnk, olwork;
    real rdummy[2];
    integer lwrsvd, mwrsvd;
    logical lquery, wntres;
    char jsvopt[1];
    extern /* Subroutine */ int slassq_(integer *, real *, integer *, real *, 
	    real *), mecago_();
    integer mwrsvj, lwrsvq, mwrsvq;
    real rdummy2[2], ofl, one;
    extern /* Subroutine */ int sgesvdq_(char *, char *, char *, char *, char 
	    *, integer *, integer *, real *, integer *, real *, real *, 
	    integer *, real *, integer *, integer *, integer *, integer *, 
	    real *, integer *, real *, integer *, integer *);

/* March 2023 */
/* ..... */
/*      USE                   iso_fortran_env */
/*      INTEGER, PARAMETER :: WP = real32 */
/* ..... */
/*     Scalar arguments */
/*     Array arguments */
/* ............................................................ */
/*     Purpose */
/*     ======= */
/*     SGEDMD computes the Dynamic Mode Decomposition (DMD) for */
/*     a pair of data snapshot matrices. For the input matrices */
/*     X and Y such that Y = A*X with an unaccessible matrix */
/*     A, SGEDMD computes a certain number of Ritz pairs of A using */
/*     the standard Rayleigh-Ritz extraction from a subspace of */
/*     range(X) that is determined using the leading left singular */
/*     vectors of X. Optionally, SGEDMD returns the residuals */
/*     of the computed Ritz pairs, the information needed for */
/*     a refinement of the Ritz vectors, or the eigenvectors of */
/*     the Exact DMD. */
/*     For further details see the references listed */
/*     below. For more details of the implementation see [3]. */

/*     References */
/*     ========== */
/*     [1] P. Schmid: Dynamic mode decomposition of numerical */
/*         and experimental data, */
/*         Journal of Fluid Mechanics 656, 5-28, 2010. */
/*     [2] Z. Drmac, I. Mezic, R. Mohr: Data driven modal */
/*         decompositions: analysis and enhancements, */
/*         SIAM J. on Sci. Comp. 40 (4), A2253-A2285, 2018. */
/*     [3] Z. Drmac: A LAPACK implementation of the Dynamic */
/*         Mode Decomposition I. Technical report. AIMDyn Inc. */
/*         and LAPACK Working Note 298. */
/*     [4] J. Tu, C. W. Rowley, D. M. Luchtenburg, S. L. */
/*         Brunton, N. Kutz: On Dynamic Mode Decomposition: */
/*         Theory and Applications, Journal of Computational */
/*         Dynamics 1(2), 391 -421, 2014. */

/* ...................................................................... */
/*     Developed and supported by: */
/*     =========================== */
/*     Developed and coded by Zlatko Drmac, Faculty of Science, */
/*     University of Zagreb;  drmac@math.hr */
/*     In cooperation with */
/*     AIMdyn Inc., Santa Barbara, CA. */
/*     and supported by */
/*     - DARPA SBIR project "Koopman Operator-Based Forecasting */
/*     for Nonstationary Processes from Near-Term, Limited */
/*     Observational Data" Contract No: W31P4Q-21-C-0007 */
/*     - DARPA PAI project "Physics-Informed Machine Learning */
/*     Methodologies" Contract No: HR0011-18-9-0033 */
/*     - DARPA MoDyL project "A Data-Driven, Operator-Theoretic */
/*     Framework for Space-Time Analysis of Process Dynamics" */
/*     Contract No: HR0011-16-C-0116 */
/*     Any opinions, findings and conclusions or recommendations */
/*     expressed in this material are those of the author and */
/*     do not necessarily reflect the views of the DARPA SBIR */
/*     Program Office */
/* ============================================================ */
/*     Distribution Statement A: */
/*     Approved for Public Release, Distribution Unlimited. */
/*     Cleared by DARPA on September 29, 2022 */
/* ============================================================ */
/* ...................................................................... */
/*     Arguments */
/*     ========= */
/*     JOBS (input) CHARACTER*1 */
/*     Determines whether the initial data snapshots are scaled */
/*     by a diagonal matrix. */
/*     'S' :: The data snapshots matrices X and Y are multiplied */
/*            with a diagonal matrix D so that X*D has unit */
/*            nonzero columns (in the Euclidean 2-norm) */
/*     'C' :: The snapshots are scaled as with the 'S' option. */
/*            If it is found that an i-th column of X is zero */
/*            vector and the corresponding i-th column of Y is */
/*            non-zero, then the i-th column of Y is set to */
/*            zero and a warning flag is raised. */
/*     'Y' :: The data snapshots matrices X and Y are multiplied */
/*            by a diagonal matrix D so that Y*D has unit */
/*            nonzero columns (in the Euclidean 2-norm) */
/*     'N' :: No data scaling. */
/* ..... */
/*     JOBZ (input) CHARACTER*1 */
/*     Determines whether the eigenvectors (Koopman modes) will */
/*     be computed. */
/*     'V' :: The eigenvectors (Koopman modes) will be computed */
/*            and returned in the matrix Z. */
/*            See the description of Z. */
/*     'F' :: The eigenvectors (Koopman modes) will be returned */
/*            in factored form as the product X(:,1:K)*W, where X */
/*            contains a POD basis (leading left singular vectors */
/*            of the data matrix X) and W contains the eigenvectors */
/*            of the corresponding Rayleigh quotient. */
/*            See the descriptions of K, X, W, Z. */
/*     'N' :: The eigenvectors are not computed. */
/* ..... */
/*     JOBR (input) CHARACTER*1 */
/*     Determines whether to compute the residuals. */
/*     'R' :: The residuals for the computed eigenpairs will be */
/*            computed and stored in the array RES. */
/*            See the description of RES. */
/*            For this option to be legal, JOBZ must be 'V'. */
/*     'N' :: The residuals are not computed. */
/* ..... */
/*     JOBF (input) CHARACTER*1 */
/*     Specifies whether to store information needed for post- */
/*     processing (e.g. computing refined Ritz vectors) */
/*     'R' :: The matrix needed for the refinement of the Ritz */
/*            vectors is computed and stored in the array B. */
/*            See the description of B. */
/*     'E' :: The unscaled eigenvectors of the Exact DMD are */
/*            computed and returned in the array B. See the */
/*            description of B. */
/*     'N' :: No eigenvector refinement data is computed. */
/* ..... */
/*     WHTSVD (input) INTEGER, WHSTVD in { 1, 2, 3, 4 } */
/*     Allows for a selection of the SVD algorithm from the */
/*     LAPACK library. */
/*     1 :: SGESVD (the QR SVD algorithm) */
/*     2 :: SGESDD (the Divide and Conquer algorithm; if enough */
/*          workspace available, this is the fastest option) */
/*     3 :: SGESVDQ (the preconditioned QR SVD  ; this and 4 */
/*          are the most accurate options) */
/*     4 :: SGEJSV (the preconditioned Jacobi SVD; this and 3 */
/*          are the most accurate options) */
/*     For the four methods above, a significant difference in */
/*     the accuracy of small singular values is possible if */
/*     the snapshots vary in norm so that X is severely */
/*     ill-conditioned. If small (smaller than EPS*||X||) */
/*     singular values are of interest and JOBS=='N',  then */
/*     the options (3, 4) give the most accurate results, where */
/*     the option 4 is slightly better and with stronger */
/*     theoretical background. */
/*     If JOBS=='S', i.e. the columns of X will be normalized, */
/*     then all methods give nearly equally accurate results. */
/* ..... */
/*     M (input) INTEGER, M>= 0 */
/*     The state space dimension (the row dimension of X, Y). */
/* ..... */
/*     N (input) INTEGER, 0 <= N <= M */
/*     The number of data snapshot pairs */
/*     (the number of columns of X and Y). */
/* ..... */
/*     X (input/output) REAL(KIND=WP) M-by-N array */
/*     > On entry, X contains the data snapshot matrix X. It is */
/*     assumed that the column norms of X are in the range of */
/*     the normalized floating point numbers. */
/*     < On exit, the leading K columns of X contain a POD basis, */
/*     i.e. the leading K left singular vectors of the input */
/*     data matrix X, U(:,1:K). All N columns of X contain all */
/*     left singular vectors of the input matrix X. */
/*     See the descriptions of K, Z and W. */
/* ..... */
/*     LDX (input) INTEGER, LDX >= M */
/*     The leading dimension of the array X. */
/* ..... */
/*     Y (input/workspace/output) REAL(KIND=WP) M-by-N array */
/*     > On entry, Y contains the data snapshot matrix Y */
/*     < On exit, */
/*     If JOBR == 'R', the leading K columns of Y  contain */
/*     the residual vectors for the computed Ritz pairs. */
/*     See the description of RES. */
/*     If JOBR == 'N', Y contains the original input data, */
/*                     scaled according to the value of JOBS. */
/* ..... */
/*     LDY (input) INTEGER , LDY >= M */
/*     The leading dimension of the array Y. */
/* ..... */
/*     NRNK (input) INTEGER */
/*     Determines the mode how to compute the numerical rank, */
/*     i.e. how to truncate small singular values of the input */
/*     matrix X. On input, if */
/*     NRNK = -1 :: i-th singular value sigma(i) is truncated */
/*                  if sigma(i) <= TOL*sigma(1) */
/*                  This option is recommended. */
/*     NRNK = -2 :: i-th singular value sigma(i) is truncated */
/*                  if sigma(i) <= TOL*sigma(i-1) */
/*                  This option is included for R&D purposes. */
/*                  It requires highly accurate SVD, which */
/*                  may not be feasible. */
/*     The numerical rank can be enforced by using positive */
/*     value of NRNK as follows: */
/*     0 < NRNK <= N :: at most NRNK largest singular values */
/*     will be used. If the number of the computed nonzero */
/*     singular values is less than NRNK, then only those */
/*     nonzero values will be used and the actually used */
/*     dimension is less than NRNK. The actual number of */
/*     the nonzero singular values is returned in the variable */
/*     K. See the descriptions of TOL and  K. */
/* ..... */
/*     TOL (input) REAL(KIND=WP), 0 <= TOL < 1 */
/*     The tolerance for truncating small singular values. */
/*     See the description of NRNK. */
/* ..... */
/*     K (output) INTEGER,  0 <= K <= N */
/*     The dimension of the POD basis for the data snapshot */
/*     matrix X and the number of the computed Ritz pairs. */
/*     The value of K is determined according to the rule set */
/*     by the parameters NRNK and TOL. */
/*     See the descriptions of NRNK and TOL. */
/* ..... */
/*     REIG (output) REAL(KIND=WP) N-by-1 array */
/*     The leading K (K<=N) entries of REIG contain */
/*     the real parts of the computed eigenvalues */
/*     REIG(1:K) + sqrt(-1)*IMEIG(1:K). */
/*     See the descriptions of K, IMEIG, and Z. */
/* ..... */
/*     IMEIG (output) REAL(KIND=WP) N-by-1 array */
/*     The leading K (K<=N) entries of IMEIG contain */
/*     the imaginary parts of the computed eigenvalues */
/*     REIG(1:K) + sqrt(-1)*IMEIG(1:K). */
/*     The eigenvalues are determined as follows: */
/*     If IMEIG(i) == 0, then the corresponding eigenvalue is */
/*     real, LAMBDA(i) = REIG(i). */
/*     If IMEIG(i)>0, then the corresponding complex */
/*     conjugate pair of eigenvalues reads */
/*     LAMBDA(i)   = REIG(i) + sqrt(-1)*IMAG(i) */
/*     LAMBDA(i+1) = REIG(i) - sqrt(-1)*IMAG(i) */
/*     That is, complex conjugate pairs have consecutive */
/*     indices (i,i+1), with the positive imaginary part */
/*     listed first. */
/*     See the descriptions of K, REIG, and Z. */
/* ..... */
/*     Z (workspace/output) REAL(KIND=WP)  M-by-N array */
/*     If JOBZ =='V' then */
/*        Z contains real Ritz vectors as follows: */
/*        If IMEIG(i)=0, then Z(:,i) is an eigenvector of */
/*        the i-th Ritz value; ||Z(:,i)||_2=1. */
/*        If IMEIG(i) > 0 (and IMEIG(i+1) < 0) then */
/*        [Z(:,i) Z(:,i+1)] span an invariant subspace and */
/*        the Ritz values extracted from this subspace are */
/*        REIG(i) + sqrt(-1)*IMEIG(i) and */
/*        REIG(i) - sqrt(-1)*IMEIG(i). */
/*        The corresponding eigenvectors are */
/*        Z(:,i) + sqrt(-1)*Z(:,i+1) and */
/*        Z(:,i) - sqrt(-1)*Z(:,i+1), respectively. */
/*        || Z(:,i:i+1)||_F = 1. */
/*     If JOBZ == 'F', then the above descriptions hold for */
/*     the columns of X(:,1:K)*W(1:K,1:K), where the columns */
/*     of W(1:k,1:K) are the computed eigenvectors of the */
/*     K-by-K Rayleigh quotient. The columns of W(1:K,1:K) */
/*     are similarly structured: If IMEIG(i) == 0 then */
/*     X(:,1:K)*W(:,i) is an eigenvector, and if IMEIG(i)>0 */
/*     then X(:,1:K)*W(:,i)+sqrt(-1)*X(:,1:K)*W(:,i+1) and */
/*          X(:,1:K)*W(:,i)-sqrt(-1)*X(:,1:K)*W(:,i+1) */
/*     are the eigenvectors of LAMBDA(i), LAMBDA(i+1). */
/*     See the descriptions of REIG, IMEIG, X and W. */
/* ..... */
/*     LDZ (input) INTEGER , LDZ >= M */
/*     The leading dimension of the array Z. */
/* ..... */
/*     RES (output) REAL(KIND=WP) N-by-1 array */
/*     RES(1:K) contains the residuals for the K computed */
/*     Ritz pairs. */
/*     If LAMBDA(i) is real, then */
/*        RES(i) = || A * Z(:,i) - LAMBDA(i)*Z(:,i))||_2. */
/*     If [LAMBDA(i), LAMBDA(i+1)] is a complex conjugate pair */
/*     then */
/*     RES(i)=RES(i+1) = || A * Z(:,i:i+1) - Z(:,i:i+1) *B||_F */
/*     where B = [ real(LAMBDA(i)) imag(LAMBDA(i)) ] */
/*               [-imag(LAMBDA(i)) real(LAMBDA(i)) ]. */
/*     It holds that */
/*     RES(i)   = || A*ZC(:,i)   - LAMBDA(i)  *ZC(:,i)   ||_2 */
/*     RES(i+1) = || A*ZC(:,i+1) - LAMBDA(i+1)*ZC(:,i+1) ||_2 */
/*     where ZC(:,i)   =  Z(:,i) + sqrt(-1)*Z(:,i+1) */
/*           ZC(:,i+1) =  Z(:,i) - sqrt(-1)*Z(:,i+1) */
/*     See the description of REIG, IMEIG and Z. */
/* ..... */
/*     B (output) REAL(KIND=WP)  M-by-N array. */
/*     IF JOBF =='R', B(1:M,1:K) contains A*U(:,1:K), and can */
/*     be used for computing the refined vectors; see further */
/*     details in the provided references. */
/*     If JOBF == 'E', B(1:M,1;K) contains */
/*     A*U(:,1:K)*W(1:K,1:K), which are the vectors from the */
/*     Exact DMD, up to scaling by the inverse eigenvalues. */
/*     If JOBF =='N', then B is not referenced. */
/*     See the descriptions of X, W, K. */
/* ..... */
/*     LDB (input) INTEGER, LDB >= M */
/*     The leading dimension of the array B. */
/* ..... */
/*     W (workspace/output) REAL(KIND=WP) N-by-N array */
/*     On exit, W(1:K,1:K) contains the K computed */
/*     eigenvectors of the matrix Rayleigh quotient (real and */
/*     imaginary parts for each complex conjugate pair of the */
/*     eigenvalues). The Ritz vectors (returned in Z) are the */
/*     product of X (containing a POD basis for the input */
/*     matrix X) and W. See the descriptions of K, S, X and Z. */
/*     W is also used as a workspace to temporarily store the */
/*     left singular vectors of X. */
/* ..... */
/*     LDW (input) INTEGER, LDW >= N */
/*     The leading dimension of the array W. */
/* ..... */
/*     S (workspace/output) REAL(KIND=WP) N-by-N array */
/*     The array S(1:K,1:K) is used for the matrix Rayleigh */
/*     quotient. This content is overwritten during */
/*     the eigenvalue decomposition by SGEEV. */
/*     See the description of K. */
/* ..... */
/*     LDS (input) INTEGER, LDS >= N */
/*     The leading dimension of the array S. */
/* ..... */
/*     WORK (workspace/output) REAL(KIND=WP) LWORK-by-1 array */
/*     On exit, WORK(1:N) contains the singular values of */
/*     X (for JOBS=='N') or column scaled X (JOBS=='S', 'C'). */
/*     If WHTSVD==4, then WORK(N+1) and WORK(N+2) contain */
/*     scaling factor WORK(N+2)/WORK(N+1) used to scale X */
/*     and Y to avoid overflow in the SVD of X. */
/*     This may be of interest if the scaling option is off */
/*     and as many as possible smallest eigenvalues are */
/*     desired to the highest feasible accuracy. */
/*     If the call to SGEDMD is only workspace query, then */
/*     WORK(1) contains the minimal workspace length and */
/*     WORK(2) is the optimal workspace length. Hence, the */
/*     length of work is at least 2. */
/*     See the description of LWORK. */
/* ..... */
/*     LWORK (input) INTEGER */
/*     The minimal length of the workspace vector WORK. */
/*     LWORK is calculated as follows: */
/*     If WHTSVD == 1 :: */
/*        If JOBZ == 'V', then */
/*        LWORK >= MAX(2, N + LWORK_SVD, N+MAX(1,4*N)). */
/*        If JOBZ == 'N'  then */
/*        LWORK >= MAX(2, N + LWORK_SVD, N+MAX(1,3*N)). */
/*        Here LWORK_SVD = MAX(1,3*N+M,5*N) is the minimal */
/*        workspace length of SGESVD. */
/*     If WHTSVD == 2 :: */
/*        If JOBZ == 'V', then */
/*        LWORK >= MAX(2, N + LWORK_SVD, N+MAX(1,4*N)) */
/*        If JOBZ == 'N', then */
/*        LWORK >= MAX(2, N + LWORK_SVD, N+MAX(1,3*N)) */
/*        Here LWORK_SVD = MAX(M, 5*N*N+4*N)+3*N*N is the */
/*        minimal workspace length of SGESDD. */
/*     If WHTSVD == 3 :: */
/*        If JOBZ == 'V', then */
/*        LWORK >= MAX(2, N+LWORK_SVD,N+MAX(1,4*N)) */
/*        If JOBZ == 'N', then */
/*        LWORK >= MAX(2, N+LWORK_SVD,N+MAX(1,3*N)) */
/*        Here LWORK_SVD = N+M+MAX(3*N+1, */
/*                        MAX(1,3*N+M,5*N),MAX(1,N)) */
/*        is the minimal workspace length of SGESVDQ. */
/*     If WHTSVD == 4 :: */
/*        If JOBZ == 'V', then */
/*        LWORK >= MAX(2, N+LWORK_SVD,N+MAX(1,4*N)) */
/*        If JOBZ == 'N', then */
/*        LWORK >= MAX(2, N+LWORK_SVD,N+MAX(1,3*N)) */
/*        Here LWORK_SVD = MAX(7,2*M+N,6*N+2*N*N) is the */
/*        minimal workspace length of SGEJSV. */
/*     The above expressions are not simplified in order to */
/*     make the usage of WORK more transparent, and for */
/*     easier checking. In any case, LWORK >= 2. */
/*     If on entry LWORK = -1, then a workspace query is */
/*     assumed and the procedure only computes the minimal */
/*     and the optimal workspace lengths for both WORK and */
/*     IWORK. See the descriptions of WORK and IWORK. */
/* ..... */
/*     IWORK (workspace/output) INTEGER LIWORK-by-1 array */
/*     Workspace that is required only if WHTSVD equals */
/*     2 , 3 or 4. (See the description of WHTSVD). */
/*     If on entry LWORK =-1 or LIWORK=-1, then the */
/*     minimal length of IWORK is computed and returned in */
/*     IWORK(1). See the description of LIWORK. */
/* ..... */
/*     LIWORK (input) INTEGER */
/*     The minimal length of the workspace vector IWORK. */
/*     If WHTSVD == 1, then only IWORK(1) is used; LIWORK >=1 */
/*     If WHTSVD == 2, then LIWORK >= MAX(1,8*MIN(M,N)) */
/*     If WHTSVD == 3, then LIWORK >= MAX(1,M+N-1) */
/*     If WHTSVD == 4, then LIWORK >= MAX(3,M+3*N) */
/*     If on entry LIWORK = -1, then a workspace query is */
/*     assumed and the procedure only computes the minimal */
/*     and the optimal workspace lengths for both WORK and */
/*     IWORK. See the descriptions of WORK and IWORK. */
/* ..... */
/*     INFO (output) INTEGER */
/*     -i < 0 :: On entry, the i-th argument had an */
/*               illegal value */
/*        = 0 :: Successful return. */
/*        = 1 :: Void input. Quick exit (M=0 or N=0). */
/*        = 2 :: The SVD computation of X did not converge. */
/*               Suggestion: Check the input data and/or */
/*               repeat with different WHTSVD. */
/*        = 3 :: The computation of the eigenvalues did not */
/*               converge. */
/*        = 4 :: If data scaling was requested on input and */
/*               the procedure found inconsistency in the data */
/*               such that for some column index i, */
/*               X(:,i) = 0 but Y(:,i) /= 0, then Y(:,i) is set */
/*               to zero if JOBS=='C'. The computation proceeds */
/*               with original or modified data and warning */
/*               flag is set with INFO=4. */
/* ............................................................. */
/* ............................................................. */
/*     Parameters */
/*     ~~~~~~~~~~ */
/*     Local scalars */
/*     ~~~~~~~~~~~~~ */
/*     Local arrays */
/*     ~~~~~~~~~~~~ */
/*     External functions (BLAS and LAPACK) */
/*     ~~~~~~~~~~~~~~~~~ */
/*     External subroutines (BLAS and LAPACK) */
/*     ~~~~~~~~~~~~~~~~~~~~ */
/*     Intrinsic functions */
/*     ~~~~~~~~~~~~~~~~~~~ */
/* ............................................................ */
    /* Parameter adjustments */
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    y_dim1 = *ldy;
    y_offset = 1 + y_dim1 * 1;
    y -= y_offset;
    --reig;
    --imeig;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --res;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    w_dim1 = *ldw;
    w_offset = 1 + w_dim1 * 1;
    w -= w_offset;
    s_dim1 = *lds;
    s_offset = 1 + s_dim1 * 1;
    s -= s_offset;
    --work;
    --iwork;

    /* Function Body */
    one = 1.f;
    zero = 0.f;

/*    Test the input arguments */

    wntres = lsame_(jobr, "R");
    sccolx = lsame_(jobs, "S") || lsame_(jobs, "C");
    sccoly = lsame_(jobs, "Y");
    wntvec = lsame_(jobz, "V");
    wntref = lsame_(jobf, "R");
    wntex = lsame_(jobf, "E");
    *info = 0;
    lquery = *lwork == -1 || *liwork == -1;

    if (! (sccolx || sccoly || lsame_(jobs, "N"))) {
	*info = -1;
    } else if (! (wntvec || lsame_(jobz, "N") || lsame_(
	    jobz, "F"))) {
	*info = -2;
    } else if (! (wntres || lsame_(jobr, "N")) || 
	    wntres && ! wntvec) {
	*info = -3;
    } else if (! (wntref || wntex || lsame_(jobf, "N")))
	     {
	*info = -4;
    } else if (! (*whtsvd == 1 || *whtsvd == 2 || *whtsvd == 3 || *whtsvd == 
	    4)) {
	*info = -5;
    } else if (*m < 0) {
	*info = -6;
    } else if (*n < 0 || *n > *m) {
	*info = -7;
    } else if (*ldx < *m) {
	*info = -9;
    } else if (*ldy < *m) {
	*info = -11;
    } else if (! (*nrnk == -2 || *nrnk == -1 || *nrnk >= 1 && *nrnk <= *n)) {
	*info = -12;
    } else if (*tol < zero || *tol >= one) {
	*info = -13;
    } else if (*ldz < *m) {
	*info = -18;
    } else if ((wntref || wntex) && *ldb < *m) {
	*info = -21;
    } else if (*ldw < *n) {
	*info = -23;
    } else if (*lds < *n) {
	*info = -25;
    }

    if (*info == 0) {
/* Compute the minimal and the optimal workspace */
/* requirements. Simulate running the code and */
/* determine minimal and optimal sizes of the */
/* workspace at any moment of the run. */
	if (*n == 0) {
/* Quick return. All output except K is void. */
/* INFO=1 signals the void input. */
/* In case of a workspace query, the default */
/* minimal workspace lengths are returned. */
	    if (lquery) {
		iwork[1] = 1;
		work[1] = 2.f;
		work[2] = 2.f;
	    } else {
		*k = 0;
	    }
	    *info = 1;
	    return 0;
	}
	mlwork = f2cmax(2,*n);
	olwork = f2cmax(2,*n);
	iminwr = 1;
/*         SELECT CASE ( WHTSVD ) */
	if (*whtsvd == 1) {
/* The following is specified as the minimal */
/* length of WORK in the definition of SGESVD: */
/* MWRSVD = MAX(1,3*MIN(M,N)+MAX(M,N),5*MIN(M,N)) */
/* Computing MAX */
	    i__1 = 1, i__2 = f2cmin(*m,*n) * 3 + f2cmax(*m,*n), i__1 = f2cmax(i__1,
		    i__2), i__2 = f2cmin(*m,*n) * 5;
	    mwrsvd = f2cmax(i__1,i__2);
/* Computing MAX */
	    i__1 = mlwork, i__2 = *n + mwrsvd;
	    mlwork = f2cmax(i__1,i__2);
	    if (lquery) {
		sgesvd_("O", "S", m, n, &x[x_offset], ldx, &work[1], &b[
			b_offset], ldb, &w[w_offset], ldw, rdummy, &c_n1, &
			info1);
/* Computing MAX */
		i__1 = mwrsvd, i__2 = (integer) rdummy[0];
		lwrsvd = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = olwork, i__2 = *n + lwrsvd;
		olwork = f2cmax(i__1,i__2);
	    }
	} else if (*whtsvd == 2) {
/* The following is specified as the minimal */
/* length of WORK in the definition of SGESDD: */
/* MWRSDD = 3*MIN(M,N)*MIN(M,N) + */
/* MAX( MAX(M,N),5*MIN(M,N)*MIN(M,N)+4*MIN(M,N) ) */
/* IMINWR = 8*MIN(M,N) */
/* Computing MAX */
	    i__1 = f2cmax(*m,*n), i__2 = f2cmin(*m,*n) * 5 * f2cmin(*m,*n) + (f2cmin(*m,*
		    n) << 2);
	    mwrsdd = f2cmin(*m,*n) * 3 * f2cmin(*m,*n) + f2cmax(i__1,i__2);
/* Computing MAX */
	    i__1 = mlwork, i__2 = *n + mwrsdd;
	    mlwork = f2cmax(i__1,i__2);
	    iminwr = f2cmin(*m,*n) << 3;
	    if (lquery) {
		sgesdd_("O", m, n, &x[x_offset], ldx, &work[1], &b[b_offset], 
			ldb, &w[w_offset], ldw, rdummy, &c_n1, &iwork[1], &
			info1);
/* Computing MAX */
		i__1 = mwrsdd, i__2 = (integer) rdummy[0];
		lwrsdd = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = olwork, i__2 = *n + lwrsdd;
		olwork = f2cmax(i__1,i__2);
	    }
	} else if (*whtsvd == 3) {
/* LWQP3 = 3*N+1 */
/* LWORQ = MAX(N, 1) */
/* MWRSVD = MAX(1,3*MIN(M,N)+MAX(M,N),5*MIN(M,N)) */
/* MWRSVQ = N + MAX( LWQP3, MWRSVD, LWORQ )+ MAX(M,2) */
/* MLWORK = N + MWRSVQ */
/* IMINWR = M+N-1 */
	    sgesvdq_("H", "P", "N", "R", "R", m, n, &x[x_offset], ldx, &work[
		    1], &z__[z_offset], ldz, &w[w_offset], ldw, &numrnk, &
		    iwork[1], &c_n1, rdummy, &c_n1, rdummy2, &c_n1, &info1);
	    iminwr = iwork[1];
	    mwrsvq = (integer) rdummy[1];
/* Computing MAX */
	    i__1 = mlwork, i__2 = *n + mwrsvq + (integer) rdummy2[0];
	    mlwork = f2cmax(i__1,i__2);
	    if (lquery) {
		lwrsvq = (integer) rdummy[0];
/* Computing MAX */
		i__1 = olwork, i__2 = *n + lwrsvq + (integer) rdummy2[0];
		olwork = f2cmax(i__1,i__2);
	    }
	} else if (*whtsvd == 4) {
	    *(unsigned char *)jsvopt = 'J';
/* MWRSVJ = MAX( 7, 2*M+N, 6*N+2*N*N )! for JSVOPT='V' */
/* Computing MAX */
	    i__1 = 7, i__2 = (*m << 1) + *n, i__1 = f2cmax(i__1,i__2), i__2 = (*
		    n << 2) + *n * *n, i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1)
		     + *n * *n + 6;
	    mwrsvj = f2cmax(i__1,i__2);
/* Computing MAX */
	    i__1 = mlwork, i__2 = *n + mwrsvj;
	    mlwork = f2cmax(i__1,i__2);
/* Computing MAX */
	    i__1 = 3, i__2 = *m + *n * 3;
	    iminwr = f2cmax(i__1,i__2);
	    if (lquery) {
/* Computing MAX */
		i__1 = olwork, i__2 = *n + mwrsvj;
		olwork = f2cmax(i__1,i__2);
	    }
	}
/*         END SELECT */
	if (wntvec || wntex || lsame_(jobz, "F")) {
	    *(unsigned char *)jobzl = 'V';
	} else {
	    *(unsigned char *)jobzl = 'N';
	}
/* Workspace calculation to the SGEEV call */
	if (lsame_(jobzl, "V")) {
/* Computing MAX */
	    i__1 = 1, i__2 = *n << 2;
	    mwrkev = f2cmax(i__1,i__2);
	} else {
/* Computing MAX */
	    i__1 = 1, i__2 = *n * 3;
	    mwrkev = f2cmax(i__1,i__2);
	}
/* Computing MAX */
	i__1 = mlwork, i__2 = *n + mwrkev;
	mlwork = f2cmax(i__1,i__2);
	if (lquery) {
	    sgeev_("N", jobzl, n, &s[s_offset], lds, &reig[1], &imeig[1], &w[
		    w_offset], ldw, &w[w_offset], ldw, rdummy, &c_n1, &info1);
/* Computing MAX */
	    i__1 = mwrkev, i__2 = (integer) rdummy[0];
	    lwrkev = f2cmax(i__1,i__2);
/* Computing MAX */
	    i__1 = olwork, i__2 = *n + lwrkev;
	    olwork = f2cmax(i__1,i__2);
	}

	if (*liwork < iminwr && ! lquery) {
	    *info = -29;
	}
	if (*lwork < mlwork && ! lquery) {
	    *info = -27;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SGEDMD", &i__1);
	return 0;
    } else if (lquery) {
/*     Return minimal and optimal workspace sizes */
	iwork[1] = iminwr;
	work[1] = (real) mlwork;
	work[2] = (real) olwork;
	return 0;
    }
/* ............................................................ */

    ofl = slamch_("O");
    small = slamch_("S");
    badxy = FALSE_;

/*     <1> Optional scaling of the snapshots (columns of X, Y) */
/*     ========================================================== */
    if (sccolx) {
/* The columns of X will be normalized. */
/* To prevent overflows, the column norms of X are */
/* carefully computed using SLASSQ. */
	*k = 0;
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
/* WORK(i) = DNRM2( M, X(1,i), 1 ) */
	    scale = zero;
	    slassq_(m, &x[i__ * x_dim1 + 1], &c__1, &scale, &ssum);
	    if (sisnan_(&scale) || sisnan_(&ssum)) {
		*k = 0;
		*info = -8;
		i__2 = -(*info);
		xerbla_("SGEDMD", &i__2);
	    }
	    if (scale != zero && ssum != zero) {
		rootsc = sqrt(ssum);
		if (scale >= ofl / rootsc) {
/*                 Norm of X(:,i) overflows. First, X(:,i) */
/*                 is scaled by */
/*                 ( ONE / ROOTSC ) / SCALE = 1/||X(:,i)||_2. */
/*                 Next, the norm of X(:,i) is stored without */
/*                 overflow as WORK(i) = - SCALE * (ROOTSC/M), */
/*                 the minus sign indicating the 1/M factor. */
/*                 Scaling is performed without overflow, and */
/*                 underflow may occur in the smallest entries */
/*                 of X(:,i). The relative backward and forward */
/*                 errors are small in the ell_2 norm. */
		    r__1 = one / rootsc;
		    slascl_("G", &c__0, &c__0, &scale, &r__1, m, &c__1, &x[
			    i__ * x_dim1 + 1], m, &info2);
		    work[i__] = -scale * (rootsc / (real) (*m));
		} else {
/*                 X(:,i) will be scaled to unit 2-norm */
		    work[i__] = scale * rootsc;
		    slascl_("G", &c__0, &c__0, &work[i__], &one, m, &c__1, &x[
			    i__ * x_dim1 + 1], m, &info2);
/*                 X(1:M,i) = (ONE/WORK(i)) * X(1:M,i)          ! INTRINSIC */
/* LAPACK */
		}
	    } else {
		work[i__] = zero;
		++(*k);
	    }
	}
	if (*k == *n) {
/* All columns of X are zero. Return error code -8. */
/* (the 8th input variable had an illegal value) */
	    *k = 0;
	    *info = -8;
	    i__1 = -(*info);
	    xerbla_("SGEDMD", &i__1);
	    return 0;
	}
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
/*           Now, apply the same scaling to the columns of Y. */
	    if (work[i__] > zero) {
		r__1 = one / work[i__];
		sscal_(m, &r__1, &y[i__ * y_dim1 + 1], &c__1);
/*               Y(1:M,i) = (ONE/WORK(i)) * Y(1:M,i)      ! INTRINSIC */
/* BLAS CALL */
	    } else if (work[i__] < zero) {
		r__1 = -work[i__];
		r__2 = one / (real) (*m);
		slascl_("G", &c__0, &c__0, &r__1, &r__2, m, &c__1, &y[i__ * 
			y_dim1 + 1], m, &info2);
/* LAPACK CA */
	    } else if (y[isamax_(m, &y[i__ * y_dim1 + 1], &c__1) + i__ * 
		    y_dim1] != zero) {
/*               X(:,i) is zero vector. For consistency, */
/*               Y(:,i) should also be zero. If Y(:,i) is not */
/*               zero, then the data might be inconsistent or */
/*               corrupted. If JOBS == 'C', Y(:,i) is set to */
/*               zero and a warning flag is raised. */
/*               The computation continues but the */
/*               situation will be reported in the output. */
		badxy = TRUE_;
		if (lsame_(jobs, "C")) {
		    sscal_(m, &zero, &y[i__ * y_dim1 + 1], &c__1);
		}
/* BLAS CALL */
	    }
	}
    }

    if (sccoly) {
/* The columns of Y will be normalized. */
/* To prevent overflows, the column norms of Y are */
/* carefully computed using SLASSQ. */
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
/* WORK(i) = DNRM2( M, Y(1,i), 1 ) */
	    scale = zero;
	    slassq_(m, &y[i__ * y_dim1 + 1], &c__1, &scale, &ssum);
	    if (sisnan_(&scale) || sisnan_(&ssum)) {
		*k = 0;
		*info = -10;
		i__2 = -(*info);
		xerbla_("SGEDMD", &i__2);
	    }
	    if (scale != zero && ssum != zero) {
		rootsc = sqrt(ssum);
		if (scale >= ofl / rootsc) {
/*                 Norm of Y(:,i) overflows. First, Y(:,i) */
/*                 is scaled by */
/*                 ( ONE / ROOTSC ) / SCALE = 1/||Y(:,i)||_2. */
/*                 Next, the norm of Y(:,i) is stored without */
/*                 overflow as WORK(i) = - SCALE * (ROOTSC/M), */
/*                 the minus sign indicating the 1/M factor. */
/*                 Scaling is performed without overflow, and */
/*                 underflow may occur in the smallest entries */
/*                 of Y(:,i). The relative backward and forward */
/*                 errors are small in the ell_2 norm. */
		    r__1 = one / rootsc;
		    slascl_("G", &c__0, &c__0, &scale, &r__1, m, &c__1, &y[
			    i__ * y_dim1 + 1], m, &info2);
		    work[i__] = -scale * (rootsc / (real) (*m));
		} else {
/*                 X(:,i) will be scaled to unit 2-norm */
		    work[i__] = scale * rootsc;
		    slascl_("G", &c__0, &c__0, &work[i__], &one, m, &c__1, &y[
			    i__ * y_dim1 + 1], m, &info2);
/*                 Y(1:M,i) = (ONE/WORK(i)) * Y(1:M,i)          ! INTRINSIC */
/* LAPACK */
		}
	    } else {
		work[i__] = zero;
	    }
	}
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
/*           Now, apply the same scaling to the columns of X. */
	    if (work[i__] > zero) {
		r__1 = one / work[i__];
		sscal_(m, &r__1, &x[i__ * x_dim1 + 1], &c__1);
/*               X(1:M,i) = (ONE/WORK(i)) * X(1:M,i)      ! INTRINSIC */
/* BLAS CALL */
	    } else if (work[i__] < zero) {
		r__1 = -work[i__];
		r__2 = one / (real) (*m);
		slascl_("G", &c__0, &c__0, &r__1, &r__2, m, &c__1, &x[i__ * 
			x_dim1 + 1], m, &info2);
/* LAPACK CA */
	    } else if (x[isamax_(m, &x[i__ * x_dim1 + 1], &c__1) + i__ * 
		    x_dim1] != zero) {
/*               Y(:,i) is zero vector.  If X(:,i) is not */
/*               zero, then a warning flag is raised. */
/*               The computation continues but the */
/*               situation will be reported in the output. */
		badxy = TRUE_;
	    }
	}
    }

/*     <2> SVD of the data snapshot matrix X. */
/*     ===================================== */
/*     The left singular vectors are stored in the array X. */
/*     The right singular vectors are in the array W. */
/*     The array W will later on contain the eigenvectors */
/*     of a Rayleigh quotient. */
    numrnk = *n;
/*      SELECT CASE ( WHTSVD ) */
    if (*whtsvd == 1) {
	i__1 = *lwork - *n;
	sgesvd_("O", "S", m, n, &x[x_offset], ldx, &work[1], &b[b_offset], 
		ldb, &w[w_offset], ldw, &work[*n + 1], &i__1, &info1);
/* LAPACK CAL */
	*(unsigned char *)t_or_n__ = 'T';
    } else if (*whtsvd == 2) {
	i__1 = *lwork - *n;
	sgesdd_("O", m, n, &x[x_offset], ldx, &work[1], &b[b_offset], ldb, &w[
		w_offset], ldw, &work[*n + 1], &i__1, &iwork[1], &info1);
/* LAPACK CAL */
	*(unsigned char *)t_or_n__ = 'T';
    } else if (*whtsvd == 3) {
	i__1 = *lwork - *n - f2cmax(2,*m);
	i__2 = f2cmax(2,*m);
	sgesvdq_("H", "P", "N", "R", "R", m, n, &x[x_offset], ldx, &work[1], &
		z__[z_offset], ldz, &w[w_offset], ldw, &numrnk, &iwork[1], 
		liwork, &work[*n + f2cmax(2,*m) + 1], &i__1, &work[*n + 1], &
		i__2, &info1);

	slacpy_("A", m, &numrnk, &z__[z_offset], ldz, &x[x_offset], ldx);
/* LAPACK C */
	*(unsigned char *)t_or_n__ = 'T';
    } else if (*whtsvd == 4) {
	i__1 = *lwork - *n;
	sgejsv_("F", "U", jsvopt, "N", "N", "P", m, n, &x[x_offset], ldx, &
		work[1], &z__[z_offset], ldz, &w[w_offset], ldw, &work[*n + 1]
		, &i__1, &iwork[1], &info1);
/* LAPACK CALL */
	slacpy_("A", m, n, &z__[z_offset], ldz, &x[x_offset], ldx);
/* LAPACK CALL */
	*(unsigned char *)t_or_n__ = 'N';
	xscl1 = work[*n + 1];
	xscl2 = work[*n + 2];
	if (xscl1 != xscl2) {
/* This is an exceptional situation. If the */
/* data matrices are not scaled and the */
/* largest singular value of X overflows. */
/* In that case SGEJSV can return the SVD */
/* in scaled form. The scaling factor can be used */
/* to rescale the data (X and Y). */
	    slascl_("G", &c__0, &c__0, &xscl1, &xscl2, m, n, &y[y_offset], 
		    ldy, &info2);
	}
/*      END SELECT */
    }

    if (info1 > 0) {
/* The SVD selected subroutine did not converge. */
/* Return with an error code. */
	*info = 2;
	return 0;
    }

    if (work[1] == zero) {
/* The largest computed singular value of (scaled) */
/* X is zero. Return error code -8 */
/* (the 8th input variable had an illegal value). */
	*k = 0;
	*info = -8;
	i__1 = -(*info);
	xerbla_("SGEDMD", &i__1);
	return 0;
    }

/* <3> Determine the numerical rank of the data */
/*    snapshots matrix X. This depends on the */
/*    parameters NRNK and TOL. */
/*      SELECT CASE ( NRNK ) */
    if (*nrnk == -1) {
	*k = 1;
	i__1 = numrnk;
	for (i__ = 2; i__ <= i__1; ++i__) {
	    if (work[i__] <= work[1] * *tol || work[i__] <= small) {
		myexit_();
	    }
	    ++(*k);
	}
    } else if (*nrnk == -2) {
	*k = 1;
	i__1 = numrnk - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if (work[i__ + 1] <= work[i__] * *tol || work[i__] <= small) {
		myexit_();
	    }
	    ++(*k);
	}
    } else {
	*k = 1;
	i__1 = *nrnk;
	for (i__ = 2; i__ <= i__1; ++i__) {
	    if (work[i__] <= small) {
		myexit_();
	    }
	    ++(*k);
	}
/*          END SELECT */
    }
/*   Now, U = X(1:M,1:K) is the SVD/POD basis for the */
/*   snapshot data in the input matrix X. */
/* <4> Compute the Rayleigh quotient S = U^T * A * U. */
/*    Depending on the requested outputs, the computation */
/*    is organized to compute additional auxiliary */
/*    matrices (for the residuals and refinements). */

/*    In all formulas below, we need V_k*Sigma_k^(-1) */
/*    where either V_k is in W(1:N,1:K), or V_k^T is in */
/*    W(1:K,1:N). Here Sigma_k=diag(WORK(1:K)). */
    if (lsame_(t_or_n__, "N")) {
	i__1 = *k;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    r__1 = one / work[i__];
	    sscal_(n, &r__1, &w[i__ * w_dim1 + 1], &c__1);
/* W(1:N,i) = (ONE/WORK(i)) * W(1:N,i)      ! INTRINSIC */
/* BLAS CALL */
	}
    } else {
/* This non-unit stride access is due to the fact */
/* that SGESVD, SGESVDQ and SGESDD return the */
/* transposed matrix of the right singular vectors. */
/* DO i = 1, K */
/* CALL SSCAL( N, ONE/WORK(i), W(i,1), LDW )    ! BLAS CALL */
/* ! W(i,1:N) = (ONE/WORK(i)) * W(i,1:N)      ! INTRINSIC */
/* END DO */
	i__1 = *k;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    work[*n + i__] = one / work[i__];
	}
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w[i__ + j * w_dim1] = work[*n + i__] * w[i__ + j * w_dim1];
	    }
	}
    }

    if (wntref) {

/* Need A*U(:,1:K)=Y*V_k*inv(diag(WORK(1:K))) */
/* for computing the refined Ritz vectors */
/* (optionally, outside SGEDMD). */
	sgemm_("N", t_or_n__, m, k, n, &one, &y[y_offset], ldy, &w[w_offset], 
		ldw, &zero, &z__[z_offset], ldz);
/* Z(1:M,1:K)=MATMUL(Y(1:M,1:N),TRANSPOSE(W(1:K,1:N)))  ! INTRI */
/* Z(1:M,1:K)=MATMUL(Y(1:M,1:N),W(1:N,1:K))             ! INTRI */

/* At this point Z contains */
/* A * U(:,1:K) = Y * V_k * Sigma_k^(-1), and */
/* this is needed for computing the residuals. */
/* This matrix is  returned in the array B and */
/* it can be used to compute refined Ritz vectors. */
/* BLAS */
	slacpy_("A", m, k, &z__[z_offset], ldz, &b[b_offset], ldb);
/* B(1:M,1:K) = Z(1:M,1:K)                  ! INTRINSIC */
/* BLAS CALL */
	sgemm_("T", "N", k, k, m, &one, &x[x_offset], ldx, &z__[z_offset], 
		ldz, &zero, &s[s_offset], lds);
/* S(1:K,1:K) = MATMUL(TANSPOSE(X(1:M,1:K)),Z(1:M,1:K)) ! INTRI */
/* At this point S = U^T * A * U is the Rayleigh quotient. */
/* BLAS */
    } else {
/* A * U(:,1:K) is not explicitly needed and the */
/* computation is organized differently. The Rayleigh */
/* quotient is computed more efficiently. */
	sgemm_("T", "N", k, n, m, &one, &x[x_offset], ldx, &y[y_offset], ldy, 
		&zero, &z__[z_offset], ldz);
/* Z(1:K,1:N) = MATMUL( TRANSPOSE(X(1:M,1:K)), Y(1:M,1:N) )  ! IN */
/* In the two SGEMM calls here, can use K for LDZ */
/* B */
	sgemm_("N", t_or_n__, k, k, n, &one, &z__[z_offset], ldz, &w[w_offset]
		, ldw, &zero, &s[s_offset], lds);
/* S(1:K,1:K) = MATMUL(Z(1:K,1:N),TRANSPOSE(W(1:K,1:N))) ! INTRIN */
/* S(1:K,1:K) = MATMUL(Z(1:K,1:N),(W(1:N,1:K)))          ! INTRIN */
/* At this point S = U^T * A * U is the Rayleigh quotient. */
/* If the residuals are requested, save scaled V_k into Z. */
/* Recall that V_k or V_k^T is stored in W. */
/* BLAS */
	if (wntres || wntex) {
	    if (lsame_(t_or_n__, "N")) {
		slacpy_("A", n, k, &w[w_offset], ldw, &z__[z_offset], ldz);
	    } else {
		slacpy_("A", k, n, &w[w_offset], ldw, &z__[z_offset], ldz);
	    }
	}
    }

/* <5> Compute the Ritz values and (if requested) the */
/*   right eigenvectors of the Rayleigh quotient. */

    i__1 = *lwork - *n;
    sgeev_("N", jobzl, k, &s[s_offset], lds, &reig[1], &imeig[1], &w[w_offset]
	    , ldw, &w[w_offset], ldw, &work[*n + 1], &i__1, &info1);

/* W(1:K,1:K) contains the eigenvectors of the Rayleigh */
/* quotient. Even in the case of complex spectrum, all */
/* computation is done in real arithmetic. REIG and */
/* IMEIG are the real and the imaginary parts of the */
/* eigenvalues, so that the spectrum is given as */
/* REIG(:) + sqrt(-1)*IMEIG(:). Complex conjugate pairs */
/* are listed at consecutive positions. For such a */
/* complex conjugate pair of the eigenvalues, the */
/* corresponding eigenvectors are also a complex */
/* conjugate pair with the real and imaginary parts */
/* stored column-wise in W at the corresponding */
/* consecutive column indices. See the description of Z. */
/* Also, see the description of SGEEV. */
/* LAPACK C */
    if (info1 > 0) {
/* SGEEV failed to compute the eigenvalues and */
/* eigenvectors of the Rayleigh quotient. */
	*info = 3;
	return 0;
    }

/* <6> Compute the eigenvectors (if requested) and, */
/* the residuals (if requested). */

    if (wntvec || wntex) {
	if (wntres) {
	    if (wntref) {
/* Here, if the refinement is requested, we have */
/* A*U(:,1:K) already computed and stored in Z. */
/* For the residuals, need Y = A * U(:,1;K) * W. */
		sgemm_("N", "N", m, k, k, &one, &z__[z_offset], ldz, &w[
			w_offset], ldw, &zero, &y[y_offset], ldy);
/* Y(1:M,1:K) = Z(1:M,1:K) * W(1:K,1:K)       ! INTRINSIC */
/* This frees Z; Y contains A * U(:,1:K) * W. */
/* BLAS CALL */
	    } else {
/* Compute S = V_k * Sigma_k^(-1) * W, where */
/* V_k * Sigma_k^(-1) is stored in Z */
		sgemm_(t_or_n__, "N", n, k, k, &one, &z__[z_offset], ldz, &w[
			w_offset], ldw, &zero, &s[s_offset], lds);
/* Then, compute Z = Y * S = */
/* = Y * V_k * Sigma_k^(-1) * W(1:K,1:K) = */
/* = A * U(:,1:K) * W(1:K,1:K) */
		sgemm_("N", "N", m, k, n, &one, &y[y_offset], ldy, &s[
			s_offset], lds, &zero, &z__[z_offset], ldz);
/* Save a copy of Z into Y and free Z for holding */
/* the Ritz vectors. */
		slacpy_("A", m, k, &z__[z_offset], ldz, &y[y_offset], ldy);
		if (wntex) {
		    slacpy_("A", m, k, &z__[z_offset], ldz, &b[b_offset], ldb);
		}
	    }
	} else if (wntex) {
/* Compute S = V_k * Sigma_k^(-1) * W, where */
/* V_k * Sigma_k^(-1) is stored in Z */
	    sgemm_(t_or_n__, "N", n, k, k, &one, &z__[z_offset], ldz, &w[
		    w_offset], ldw, &zero, &s[s_offset], lds);
/* Then, compute Z = Y * S = */
/* = Y * V_k * Sigma_k^(-1) * W(1:K,1:K) = */
/* = A * U(:,1:K) * W(1:K,1:K) */
	    sgemm_("N", "N", m, k, n, &one, &y[y_offset], ldy, &s[s_offset], 
		    lds, &zero, &b[b_offset], ldb);
/* The above call replaces the following two calls */
/* that were used in the developing-testing phase. */
/* CALL SGEMM( 'N', 'N', M, K, N, ONE, Y, LDY, S, & */
/*           LDS, ZERO, Z, LDZ) */
/* Save a copy of Z into B and free Z for holding */
/* the Ritz vectors. */
/* CALL SLACPY( 'A', M, K, Z, LDZ, B, LDB ) */
	}

/* Compute the real form of the Ritz vectors */
	if (wntvec) {
	    sgemm_("N", "N", m, k, k, &one, &x[x_offset], ldx, &w[w_offset], 
		    ldw, &zero, &z__[z_offset], ldz);
	}
/* Z(1:M,1:K) = MATMUL(X(1:M,1:K), W(1:K,1:K))         ! INTRINSIC */

/* BLAS CALL */
	if (wntres) {
	    i__ = 1;
	    while(i__ <= *k) {
		if (imeig[i__] == zero) {
/* have a real eigenvalue with real eigenvector */
		    r__1 = -reig[i__];
		    saxpy_(m, &r__1, &z__[i__ * z_dim1 + 1], &c__1, &y[i__ * 
			    y_dim1 + 1], &c__1);
/* Y(1:M,i) = Y(1:M,i) - REIG(i) * Z(1:M,i)            ! */

		    res[i__] = snrm2_(m, &y[i__ * y_dim1 + 1], &c__1);
		    ++i__;
		} else {
/* Have a complex conjugate pair */
/* REIG(i) +- sqrt(-1)*IMEIG(i). */
/* Since all computation is done in real */
/* arithmetic, the formula for the residual */
/* is recast for real representation of the */
/* complex conjugate eigenpair. See the */
/* description of RES. */
		    ab[0] = reig[i__];
		    ab[1] = -imeig[i__];
		    ab[2] = imeig[i__];
		    ab[3] = reig[i__];
		    r__1 = -one;
		    sgemm_("N", "N", m, &c__2, &c__2, &r__1, &z__[i__ * 
			    z_dim1 + 1], ldz, ab, &c__2, &one, &y[i__ * 
			    y_dim1 + 1], ldy);
/* Y(1:M,i:i+1) = Y(1:M,i:i+1) - Z(1:M,i:i+1) * AB   ! INT */
/* BL */
		    res[i__] = slange_("F", m, &c__2, &y[i__ * y_dim1 + 1], 
			    ldy, &work[*n + 1]);
/* LA */
		    res[i__ + 1] = res[i__];
		    i__ += 2;
		}
	    }
	}
    }

    if (*whtsvd == 4) {
	work[*n + 1] = xscl1;
	work[*n + 2] = xscl2;
    }

/*     Successful exit. */
    if (! badxy) {
	*info = 0;
    } else {
/* A warning on possible data inconsistency. */
/* This should be a rare event. */
	*info = 4;
    }
/* ............................................................ */
    return 0;
/*     ...... */
} /* sgedmd_ */


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




/* > \brief \b DLATM3 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       DOUBLE PRECISION FUNCTION DLATM3( M, N, I, J, ISUB, JSUB, KL, KU, */
/*                        IDIST, ISEED, D, IGRADE, DL, DR, IPVTNG, IWORK, */
/*                        SPARSE ) */


/*       INTEGER            I, IDIST, IGRADE, IPVTNG, ISUB, J, JSUB, KL, */
/*      $                   KU, M, N */
/*       DOUBLE PRECISION   SPARSE */


/*       INTEGER            ISEED( 4 ), IWORK( * ) */
/*       DOUBLE PRECISION   D( * ), DL( * ), DR( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    DLATM3 returns the (ISUB,JSUB) entry of a random matrix of */
/* >    dimension (M, N) described by the other parameters. (ISUB,JSUB) */
/* >    is the final position of the (I,J) entry after pivoting */
/* >    according to IPVTNG and IWORK. DLATM3 is called by the */
/* >    DLATMR routine in order to build random test matrices. No error */
/* >    checking on parameters is done, because this routine is called in */
/* >    a tight loop by DLATMR which has already checked the parameters. */
/* > */
/* >    Use of DLATM3 differs from SLATM2 in the order in which the random */
/* >    number generator is called to fill in random matrix entries. */
/* >    With DLATM2, the generator is called to fill in the pivoted matrix */
/* >    columnwise. With DLATM3, the generator is called to fill in the */
/* >    matrix columnwise, after which it is pivoted. Thus, DLATM3 can */
/* >    be used to construct random matrices which differ only in their */
/* >    order of rows and/or columns. DLATM2 is used to construct band */
/* >    matrices while avoiding calling the random number generator for */
/* >    entries outside the band (and therefore generating random numbers */
/* >    in different orders for different pivot orders). */
/* > */
/* >    The matrix whose (ISUB,JSUB) entry is returned is constructed as */
/* >    follows (this routine only computes one entry): */
/* > */
/* >      If ISUB is outside (1..M) or JSUB is outside (1..N), return zero */
/* >         (this is convenient for generating matrices in band format). */
/* > */
/* >      Generate a matrix A with random entries of distribution IDIST. */
/* > */
/* >      Set the diagonal to D. */
/* > */
/* >      Grade the matrix, if desired, from the left (by DL) and/or */
/* >         from the right (by DR or DL) as specified by IGRADE. */
/* > */
/* >      Permute, if desired, the rows and/or columns as specified by */
/* >         IPVTNG and IWORK. */
/* > */
/* >      Band the matrix to have lower bandwidth KL and upper */
/* >         bandwidth KU. */
/* > */
/* >      Set random entries to zero as specified by SPARSE. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >           Number of rows of matrix. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           Number of columns of matrix. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] I */
/* > \verbatim */
/* >          I is INTEGER */
/* >           Row of unpivoted entry to be returned. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] J */
/* > \verbatim */
/* >          J is INTEGER */
/* >           Column of unpivoted entry to be returned. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISUB */
/* > \verbatim */
/* >          ISUB is INTEGER */
/* >           Row of pivoted entry to be returned. Changed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in,out] JSUB */
/* > \verbatim */
/* >          JSUB is INTEGER */
/* >           Column of pivoted entry to be returned. Changed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >           Lower bandwidth. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >           Upper bandwidth. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] IDIST */
/* > \verbatim */
/* >          IDIST is INTEGER */
/* >           On entry, IDIST specifies the type of distribution to be */
/* >           used to generate a random matrix . */
/* >           1 => UNIFORM( 0, 1 ) */
/* >           2 => UNIFORM( -1, 1 ) */
/* >           3 => NORMAL( 0, 1 ) */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISEED */
/* > \verbatim */
/* >          ISEED is INTEGER array of dimension ( 4 ) */
/* >           Seed for random number generator. */
/* >           Changed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array of dimension ( MIN( I , J ) ) */
/* >           Diagonal entries of matrix. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] IGRADE */
/* > \verbatim */
/* >          IGRADE is INTEGER */
/* >           Specifies grading of matrix as follows: */
/* >           0  => no grading */
/* >           1  => matrix premultiplied by diag( DL ) */
/* >           2  => matrix postmultiplied by diag( DR ) */
/* >           3  => matrix premultiplied by diag( DL ) and */
/* >                         postmultiplied by diag( DR ) */
/* >           4  => matrix premultiplied by diag( DL ) and */
/* >                         postmultiplied by inv( diag( DL ) ) */
/* >           5  => matrix premultiplied by diag( DL ) and */
/* >                         postmultiplied by diag( DL ) */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] DL */
/* > \verbatim */
/* >          DL is DOUBLE PRECISION array ( I or J, as appropriate ) */
/* >           Left scale factors for grading matrix.  Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] DR */
/* > \verbatim */
/* >          DR is DOUBLE PRECISION array ( I or J, as appropriate ) */
/* >           Right scale factors for grading matrix.  Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] IPVTNG */
/* > \verbatim */
/* >          IPVTNG is INTEGER */
/* >           On entry specifies pivoting permutations as follows: */
/* >           0 => none. */
/* >           1 => row pivoting. */
/* >           2 => column pivoting. */
/* >           3 => full pivoting, i.e., on both sides. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array ( I or J, as appropriate ) */
/* >           This array specifies the permutation used. The */
/* >           row (or column) originally in position K is in */
/* >           position IWORK( K ) after pivoting. */
/* >           This differs from IWORK for DLATM2. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] SPARSE */
/* > \verbatim */
/* >          SPARSE is DOUBLE PRECISION between 0. and 1. */
/* >           On entry specifies the sparsity of the matrix */
/* >           if sparse matrix is to be generated. */
/* >           SPARSE should lie between 0 and 1. */
/* >           A uniform ( 0, 1 ) random number x is generated and */
/* >           compared to SPARSE; if x is larger the matrix entry */
/* >           is unchanged and if x is smaller the entry is set */
/* >           to zero. Thus on the average a fraction SPARSE of the */
/* >           entries will be set to zero. */
/* >           Not modified. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup double_matgen */

/*  ===================================================================== */
doublereal dlatm3_(integer *m, integer *n, integer *i__, integer *j, integer *
	isub, integer *jsub, integer *kl, integer *ku, integer *idist, 
	integer *iseed, doublereal *d__, integer *igrade, doublereal *dl, 
	doublereal *dr, integer *ipvtng, integer *iwork, doublereal *sparse)
{
    /* System generated locals */
    doublereal ret_val;

    /* Local variables */
    doublereal temp;
    extern doublereal dlaran_(integer *), dlarnd_(integer *, integer *);


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */





/*  ===================================================================== */







/* ----------------------------------------------------------------------- */



/*     Check for I and J in range */

    /* Parameter adjustments */
    --iwork;
    --dr;
    --dl;
    --d__;
    --iseed;

    /* Function Body */
    if (*i__ < 1 || *i__ > *m || *j < 1 || *j > *n) {
	*isub = *i__;
	*jsub = *j;
	ret_val = 0.;
	return ret_val;
    }

/*     Compute subscripts depending on IPVTNG */

    if (*ipvtng == 0) {
	*isub = *i__;
	*jsub = *j;
    } else if (*ipvtng == 1) {
	*isub = iwork[*i__];
	*jsub = *j;
    } else if (*ipvtng == 2) {
	*isub = *i__;
	*jsub = iwork[*j];
    } else if (*ipvtng == 3) {
	*isub = iwork[*i__];
	*jsub = iwork[*j];
    }

/*     Check for banding */

    if (*jsub > *isub + *ku || *jsub < *isub - *kl) {
	ret_val = 0.;
	return ret_val;
    }

/*     Check for sparsity */

    if (*sparse > 0.) {
	if (dlaran_(&iseed[1]) < *sparse) {
	    ret_val = 0.;
	    return ret_val;
	}
    }

/*     Compute entry and grade it according to IGRADE */

    if (*i__ == *j) {
	temp = d__[*i__];
    } else {
	temp = dlarnd_(idist, &iseed[1]);
    }
    if (*igrade == 1) {
	temp *= dl[*i__];
    } else if (*igrade == 2) {
	temp *= dr[*j];
    } else if (*igrade == 3) {
	temp = temp * dl[*i__] * dr[*j];
    } else if (*igrade == 4 && *i__ != *j) {
	temp = temp * dl[*i__] / dl[*j];
    } else if (*igrade == 5) {
	temp = temp * dl[*i__] * dl[*j];
    }
    ret_val = temp;
    return ret_val;

/*     End of DLATM3 */

} /* dlatm3_ */


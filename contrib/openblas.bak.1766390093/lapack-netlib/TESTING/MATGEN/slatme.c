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




/* Table of constant values */

static integer c__1 = 1;
static real c_b23 = 0.f;
static integer c__0 = 0;
static real c_b39 = 1.f;

/* > \brief \b SLATME */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLATME( N, DIST, ISEED, D, MODE, COND, DMAX, EI, */
/*         RSIGN, */
/*                          UPPER, SIM, DS, MODES, CONDS, KL, KU, ANORM, */
/*         A, */
/*                          LDA, WORK, INFO ) */

/*       CHARACTER          DIST, RSIGN, SIM, UPPER */
/*       INTEGER            INFO, KL, KU, LDA, MODE, MODES, N */
/*       REAL               ANORM, COND, CONDS, DMAX */
/*       CHARACTER          EI( * ) */
/*       INTEGER            ISEED( 4 ) */
/*       REAL               A( LDA, * ), D( * ), DS( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    SLATME generates random non-symmetric square matrices with */
/* >    specified eigenvalues for testing LAPACK programs. */
/* > */
/* >    SLATME operates by applying the following sequence of */
/* >    operations: */
/* > */
/* >    1. Set the diagonal to D, where D may be input or */
/* >         computed according to MODE, COND, DMAX, and RSIGN */
/* >         as described below. */
/* > */
/* >    2. If complex conjugate pairs are desired (MODE=0 and EI(1)='R', */
/* >         or MODE=5), certain pairs of adjacent elements of D are */
/* >         interpreted as the real and complex parts of a complex */
/* >         conjugate pair; A thus becomes block diagonal, with 1x1 */
/* >         and 2x2 blocks. */
/* > */
/* >    3. If UPPER='T', the upper triangle of A is set to random values */
/* >         out of distribution DIST. */
/* > */
/* >    4. If SIM='T', A is multiplied on the left by a random matrix */
/* >         X, whose singular values are specified by DS, MODES, and */
/* >         CONDS, and on the right by X inverse. */
/* > */
/* >    5. If KL < N-1, the lower bandwidth is reduced to KL using */
/* >         Householder transformations.  If KU < N-1, the upper */
/* >         bandwidth is reduced to KU. */
/* > */
/* >    6. If ANORM is not negative, the matrix is scaled to have */
/* >         maximum-element-norm ANORM. */
/* > */
/* >    (Note: since the matrix cannot be reduced beyond Hessenberg form, */
/* >     no packing options are available.) */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           The number of columns (or rows) of A. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] DIST */
/* > \verbatim */
/* >          DIST is CHARACTER*1 */
/* >           On entry, DIST specifies the type of distribution to be used */
/* >           to generate the random eigen-/singular values, and for the */
/* >           upper triangle (see UPPER). */
/* >           'U' => UNIFORM( 0, 1 )  ( 'U' for uniform ) */
/* >           'S' => UNIFORM( -1, 1 ) ( 'S' for symmetric ) */
/* >           'N' => NORMAL( 0, 1 )   ( 'N' for normal ) */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISEED */
/* > \verbatim */
/* >          ISEED is INTEGER array, dimension ( 4 ) */
/* >           On entry ISEED specifies the seed of the random number */
/* >           generator. They should lie between 0 and 4095 inclusive, */
/* >           and ISEED(4) should be odd. The random number generator */
/* >           uses a linear congruential sequence limited to small */
/* >           integers, and so should produce machine independent */
/* >           random numbers. The values of ISEED are changed on */
/* >           exit, and can be used in the next call to SLATME */
/* >           to continue the same random number sequence. */
/* >           Changed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is REAL array, dimension ( N ) */
/* >           This array is used to specify the eigenvalues of A.  If */
/* >           MODE=0, then D is assumed to contain the eigenvalues (but */
/* >           see the description of EI), otherwise they will be */
/* >           computed according to MODE, COND, DMAX, and RSIGN and */
/* >           placed in D. */
/* >           Modified if MODE is nonzero. */
/* > \endverbatim */
/* > */
/* > \param[in] MODE */
/* > \verbatim */
/* >          MODE is INTEGER */
/* >           On entry this describes how the eigenvalues are to */
/* >           be specified: */
/* >           MODE = 0 means use D (with EI) as input */
/* >           MODE = 1 sets D(1)=1 and D(2:N)=1.0/COND */
/* >           MODE = 2 sets D(1:N-1)=1 and D(N)=1.0/COND */
/* >           MODE = 3 sets D(I)=COND**(-(I-1)/(N-1)) */
/* >           MODE = 4 sets D(i)=1 - (i-1)/(N-1)*(1 - 1/COND) */
/* >           MODE = 5 sets D to random numbers in the range */
/* >                    ( 1/COND , 1 ) such that their logarithms */
/* >                    are uniformly distributed.  Each odd-even pair */
/* >                    of elements will be either used as two real */
/* >                    eigenvalues or as the real and imaginary part */
/* >                    of a complex conjugate pair of eigenvalues; */
/* >                    the choice of which is done is random, with */
/* >                    50-50 probability, for each pair. */
/* >           MODE = 6 set D to random numbers from same distribution */
/* >                    as the rest of the matrix. */
/* >           MODE < 0 has the same meaning as ABS(MODE), except that */
/* >              the order of the elements of D is reversed. */
/* >           Thus if MODE is between 1 and 4, D has entries ranging */
/* >              from 1 to 1/COND, if between -1 and -4, D has entries */
/* >              ranging from 1/COND to 1, */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] COND */
/* > \verbatim */
/* >          COND is REAL */
/* >           On entry, this is used as described under MODE above. */
/* >           If used, it must be >= 1. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] DMAX */
/* > \verbatim */
/* >          DMAX is REAL */
/* >           If MODE is neither -6, 0 nor 6, the contents of D, as */
/* >           computed according to MODE and COND, will be scaled by */
/* >           DMAX / f2cmax(abs(D(i))).  Note that DMAX need not be */
/* >           positive: if DMAX is negative (or zero), D will be */
/* >           scaled by a negative number (or zero). */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] EI */
/* > \verbatim */
/* >          EI is CHARACTER*1 array, dimension ( N ) */
/* >           If MODE is 0, and EI(1) is not ' ' (space character), */
/* >           this array specifies which elements of D (on input) are */
/* >           real eigenvalues and which are the real and imaginary parts */
/* >           of a complex conjugate pair of eigenvalues.  The elements */
/* >           of EI may then only have the values 'R' and 'I'.  If */
/* >           EI(j)='R' and EI(j+1)='I', then the j-th eigenvalue is */
/* >           CMPLX( D(j) , D(j+1) ), and the (j+1)-th is the complex */
/* >           conjugate thereof.  If EI(j)=EI(j+1)='R', then the j-th */
/* >           eigenvalue is D(j) (i.e., real).  EI(1) may not be 'I', */
/* >           nor may two adjacent elements of EI both have the value 'I'. */
/* >           If MODE is not 0, then EI is ignored.  If MODE is 0 and */
/* >           EI(1)=' ', then the eigenvalues will all be real. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] RSIGN */
/* > \verbatim */
/* >          RSIGN is CHARACTER*1 */
/* >           If MODE is not 0, 6, or -6, and RSIGN='T', then the */
/* >           elements of D, as computed according to MODE and COND, will */
/* >           be multiplied by a random sign (+1 or -1).  If RSIGN='F', */
/* >           they will not be.  RSIGN may only have the values 'T' or */
/* >           'F'. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] UPPER */
/* > \verbatim */
/* >          UPPER is CHARACTER*1 */
/* >           If UPPER='T', then the elements of A above the diagonal */
/* >           (and above the 2x2 diagonal blocks, if A has complex */
/* >           eigenvalues) will be set to random numbers out of DIST. */
/* >           If UPPER='F', they will not.  UPPER may only have the */
/* >           values 'T' or 'F'. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] SIM */
/* > \verbatim */
/* >          SIM is CHARACTER*1 */
/* >           If SIM='T', then A will be operated on by a "similarity */
/* >           transform", i.e., multiplied on the left by a matrix X and */
/* >           on the right by X inverse.  X = U S V, where U and V are */
/* >           random unitary matrices and S is a (diagonal) matrix of */
/* >           singular values specified by DS, MODES, and CONDS.  If */
/* >           SIM='F', then A will not be transformed. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] DS */
/* > \verbatim */
/* >          DS is REAL array, dimension ( N ) */
/* >           This array is used to specify the singular values of X, */
/* >           in the same way that D specifies the eigenvalues of A. */
/* >           If MODE=0, the DS contains the singular values, which */
/* >           may not be zero. */
/* >           Modified if MODE is nonzero. */
/* > \endverbatim */
/* > */
/* > \param[in] MODES */
/* > \verbatim */
/* >          MODES is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] CONDS */
/* > \verbatim */
/* >          CONDS is REAL */
/* >           Same as MODE and COND, but for specifying the diagonal */
/* >           of S.  MODES=-6 and +6 are not allowed (since they would */
/* >           result in randomly ill-conditioned eigenvalues.) */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >           This specifies the lower bandwidth of the  matrix.  KL=1 */
/* >           specifies upper Hessenberg form.  If KL is at least N-1, */
/* >           then A will have full lower bandwidth.  KL must be at */
/* >           least 1. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >           This specifies the upper bandwidth of the  matrix.  KU=1 */
/* >           specifies lower Hessenberg form.  If KU is at least N-1, */
/* >           then A will have full upper bandwidth; if KU and KL */
/* >           are both at least N-1, then A will be dense.  Only one of */
/* >           KU and KL may be less than N-1.  KU must be at least 1. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] ANORM */
/* > \verbatim */
/* >          ANORM is REAL */
/* >           If ANORM is not negative, then A will be scaled by a non- */
/* >           negative real number to make the maximum-element-norm of A */
/* >           to be ANORM. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[out] A */
/* > \verbatim */
/* >          A is REAL array, dimension ( LDA, N ) */
/* >           On exit A is the desired test matrix. */
/* >           Modified. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >           LDA specifies the first dimension of A as declared in the */
/* >           calling program.  LDA must be at least N. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension ( 3*N ) */
/* >           Workspace. */
/* >           Modified. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >           Error code.  On exit, INFO will be set to one of the */
/* >           following values: */
/* >             0 => normal return */
/* >            -1 => N negative */
/* >            -2 => DIST illegal string */
/* >            -5 => MODE not in range -6 to 6 */
/* >            -6 => COND less than 1.0, and MODE neither -6, 0 nor 6 */
/* >            -8 => EI(1) is not ' ' or 'R', EI(j) is not 'R' or 'I', or */
/* >                  two adjacent elements of EI are 'I'. */
/* >            -9 => RSIGN is not 'T' or 'F' */
/* >           -10 => UPPER is not 'T' or 'F' */
/* >           -11 => SIM   is not 'T' or 'F' */
/* >           -12 => MODES=0 and DS has a zero singular value. */
/* >           -13 => MODES is not in the range -5 to 5. */
/* >           -14 => MODES is nonzero and CONDS is less than 1. */
/* >           -15 => KL is less than 1. */
/* >           -16 => KU is less than 1, or KL and KU are both less than */
/* >                  N-1. */
/* >           -19 => LDA is less than N. */
/* >            1  => Error return from SLATM1 (computing D) */
/* >            2  => Cannot scale to DMAX (f2cmax. eigenvalue is 0) */
/* >            3  => Error return from SLATM1 (computing DS) */
/* >            4  => Error return from SLARGE */
/* >            5  => Zero singular value from SLATM1. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup real_matgen */

/*  ===================================================================== */
/* Subroutine */ void slatme_(integer *n, char *dist, integer *iseed, real *
	d__, integer *mode, real *cond, real *dmax__, char *ei, char *rsign, 
	char *upper, char *sim, real *ds, integer *modes, real *conds, 
	integer *kl, integer *ku, real *anorm, real *a, integer *lda, real *
	work, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2;
    real r__1, r__2, r__3;

    /* Local variables */
    logical bads;
    extern /* Subroutine */ void sger_(integer *, integer *, real *, real *, 
	    integer *, real *, integer *, real *, integer *);
    integer isim;
    real temp;
    logical badei;
    integer i__, j;
    real alpha;
    extern logical lsame_(char *, char *);
    integer iinfo;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    real tempa[1];
    integer icols;
    logical useei;
    integer idist;
    extern /* Subroutine */ void sgemv_(char *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *), scopy_(integer *, real *, integer *, real *, integer *);
    integer irows;
    extern /* Subroutine */ void slatm1_(integer *, real *, integer *, integer 
	    *, integer *, real *, integer *, integer *);
    integer ic, jc, ir, jr;
    extern real slange_(char *, integer *, integer *, real *, integer *, real 
	    *);
    extern /* Subroutine */ void slarge_(integer *, real *, integer *, integer 
	    *, real *, integer *), slarfg_(integer *, real *, real *, integer 
	    *, real *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern real slaran_(integer *);
    integer irsign;
    extern /* Subroutine */ void slaset_(char *, integer *, integer *, real *, 
	    real *, real *, integer *);
    integer iupper;
    extern /* Subroutine */ void slarnv_(integer *, integer *, integer *, real 
	    *);
    real xnorms;
    integer jcr;
    real tau;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     1)      Decode and Test the input parameters. */
/*             Initialize flags & seed. */

    /* Parameter adjustments */
    --iseed;
    --d__;
    --ei;
    --ds;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --work;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Decode DIST */

    if (lsame_(dist, "U")) {
	idist = 1;
    } else if (lsame_(dist, "S")) {
	idist = 2;
    } else if (lsame_(dist, "N")) {
	idist = 3;
    } else {
	idist = -1;
    }

/*     Check EI */

    useei = TRUE_;
    badei = FALSE_;
    if (lsame_(ei + 1, " ") || *mode != 0) {
	useei = FALSE_;
    } else {
	if (lsame_(ei + 1, "R")) {
	    i__1 = *n;
	    for (j = 2; j <= i__1; ++j) {
		if (lsame_(ei + j, "I")) {
		    if (lsame_(ei + (j - 1), "I")) {
			badei = TRUE_;
		    }
		} else {
		    if (! lsame_(ei + j, "R")) {
			badei = TRUE_;
		    }
		}
/* L10: */
	    }
	} else {
	    badei = TRUE_;
	}
    }

/*     Decode RSIGN */

    if (lsame_(rsign, "T")) {
	irsign = 1;
    } else if (lsame_(rsign, "F")) {
	irsign = 0;
    } else {
	irsign = -1;
    }

/*     Decode UPPER */

    if (lsame_(upper, "T")) {
	iupper = 1;
    } else if (lsame_(upper, "F")) {
	iupper = 0;
    } else {
	iupper = -1;
    }

/*     Decode SIM */

    if (lsame_(sim, "T")) {
	isim = 1;
    } else if (lsame_(sim, "F")) {
	isim = 0;
    } else {
	isim = -1;
    }

/*     Check DS, if MODES=0 and ISIM=1 */

    bads = FALSE_;
    if (*modes == 0 && isim == 1) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (ds[j] == 0.f) {
		bads = TRUE_;
	    }
/* L20: */
	}
    }

/*     Set INFO if an error */

    if (*n < 0) {
	*info = -1;
    } else if (idist == -1) {
	*info = -2;
    } else if (abs(*mode) > 6) {
	*info = -5;
    } else if (*mode != 0 && abs(*mode) != 6 && *cond < 1.f) {
	*info = -6;
    } else if (badei) {
	*info = -8;
    } else if (irsign == -1) {
	*info = -9;
    } else if (iupper == -1) {
	*info = -10;
    } else if (isim == -1) {
	*info = -11;
    } else if (bads) {
	*info = -12;
    } else if (isim == 1 && abs(*modes) > 5) {
	*info = -13;
    } else if (isim == 1 && *modes != 0 && *conds < 1.f) {
	*info = -14;
    } else if (*kl < 1) {
	*info = -15;
    } else if (*ku < 1 || *ku < *n - 1 && *kl < *n - 1) {
	*info = -16;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -19;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SLATME", &i__1, 6);
	return;
    }

/*     Initialize random number generator */

    for (i__ = 1; i__ <= 4; ++i__) {
	iseed[i__] = (i__1 = iseed[i__], abs(i__1)) % 4096;
/* L30: */
    }

    if (iseed[4] % 2 != 1) {
	++iseed[4];
    }

/*     2)      Set up diagonal of A */

/*             Compute D according to COND and MODE */

    slatm1_(mode, cond, &irsign, &idist, &iseed[1], &d__[1], n, &iinfo);
    if (iinfo != 0) {
	*info = 1;
	return;
    }
    if (*mode != 0 && abs(*mode) != 6) {

/*        Scale by DMAX */

	temp = abs(d__[1]);
	i__1 = *n;
	for (i__ = 2; i__ <= i__1; ++i__) {
/* Computing MAX */
	    r__2 = temp, r__3 = (r__1 = d__[i__], abs(r__1));
	    temp = f2cmax(r__2,r__3);
/* L40: */
	}

	if (temp > 0.f) {
	    alpha = *dmax__ / temp;
	} else if (*dmax__ != 0.f) {
	    *info = 2;
	    return;
	} else {
	    alpha = 0.f;
	}

	sscal_(n, &alpha, &d__[1], &c__1);

    }

    slaset_("Full", n, n, &c_b23, &c_b23, &a[a_offset], lda);
    i__1 = *lda + 1;
    scopy_(n, &d__[1], &c__1, &a[a_offset], &i__1);

/*     Set up complex conjugate pairs */

    if (*mode == 0) {
	if (useei) {
	    i__1 = *n;
	    for (j = 2; j <= i__1; ++j) {
		if (lsame_(ei + j, "I")) {
		    a[j - 1 + j * a_dim1] = a[j + j * a_dim1];
		    a[j + (j - 1) * a_dim1] = -a[j + j * a_dim1];
		    a[j + j * a_dim1] = a[j - 1 + (j - 1) * a_dim1];
		}
/* L50: */
	    }
	}

    } else if (abs(*mode) == 5) {

	i__1 = *n;
	for (j = 2; j <= i__1; j += 2) {
	    if (slaran_(&iseed[1]) > .5f) {
		a[j - 1 + j * a_dim1] = a[j + j * a_dim1];
		a[j + (j - 1) * a_dim1] = -a[j + j * a_dim1];
		a[j + j * a_dim1] = a[j - 1 + (j - 1) * a_dim1];
	    }
/* L60: */
	}
    }

/*     3)      If UPPER='T', set upper triangle of A to random numbers. */
/*             (but don't modify the corners of 2x2 blocks.) */

    if (iupper != 0) {
	i__1 = *n;
	for (jc = 2; jc <= i__1; ++jc) {
	    if (a[jc - 1 + jc * a_dim1] != 0.f) {
		jr = jc - 2;
	    } else {
		jr = jc - 1;
	    }
	    slarnv_(&idist, &iseed[1], &jr, &a[jc * a_dim1 + 1]);
/* L70: */
	}
    }

/*     4)      If SIM='T', apply similarity transformation. */

/*                                -1 */
/*             Transform is  X A X  , where X = U S V, thus */

/*             it is  U S V A V' (1/S) U' */

    if (isim != 0) {

/*        Compute S (singular values of the eigenvector matrix) */
/*        according to CONDS and MODES */

	slatm1_(modes, conds, &c__0, &c__0, &iseed[1], &ds[1], n, &iinfo);
	if (iinfo != 0) {
	    *info = 3;
	    return;
	}

/*        Multiply by V and V' */

	slarge_(n, &a[a_offset], lda, &iseed[1], &work[1], &iinfo);
	if (iinfo != 0) {
	    *info = 4;
	    return;
	}

/*        Multiply by S and (1/S) */

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    sscal_(n, &ds[j], &a[j + a_dim1], lda);
	    if (ds[j] != 0.f) {
		r__1 = 1.f / ds[j];
		sscal_(n, &r__1, &a[j * a_dim1 + 1], &c__1);
	    } else {
		*info = 5;
		return;
	    }
/* L80: */
	}

/*        Multiply by U and U' */

	slarge_(n, &a[a_offset], lda, &iseed[1], &work[1], &iinfo);
	if (iinfo != 0) {
	    *info = 4;
	    return;
	}
    }

/*     5)      Reduce the bandwidth. */

    if (*kl < *n - 1) {

/*        Reduce bandwidth -- kill column */

	i__1 = *n - 1;
	for (jcr = *kl + 1; jcr <= i__1; ++jcr) {
	    ic = jcr - *kl;
	    irows = *n + 1 - jcr;
	    icols = *n + *kl - jcr;

	    scopy_(&irows, &a[jcr + ic * a_dim1], &c__1, &work[1], &c__1);
	    xnorms = work[1];
	    slarfg_(&irows, &xnorms, &work[2], &c__1, &tau);
	    work[1] = 1.f;

	    sgemv_("T", &irows, &icols, &c_b39, &a[jcr + (ic + 1) * a_dim1], 
		    lda, &work[1], &c__1, &c_b23, &work[irows + 1], &c__1);
	    r__1 = -tau;
	    sger_(&irows, &icols, &r__1, &work[1], &c__1, &work[irows + 1], &
		    c__1, &a[jcr + (ic + 1) * a_dim1], lda);

	    sgemv_("N", n, &irows, &c_b39, &a[jcr * a_dim1 + 1], lda, &work[1]
		    , &c__1, &c_b23, &work[irows + 1], &c__1);
	    r__1 = -tau;
	    sger_(n, &irows, &r__1, &work[irows + 1], &c__1, &work[1], &c__1, 
		    &a[jcr * a_dim1 + 1], lda);

	    a[jcr + ic * a_dim1] = xnorms;
	    i__2 = irows - 1;
	    slaset_("Full", &i__2, &c__1, &c_b23, &c_b23, &a[jcr + 1 + ic * 
		    a_dim1], lda);
/* L90: */
	}
    } else if (*ku < *n - 1) {

/*        Reduce upper bandwidth -- kill a row at a time. */

	i__1 = *n - 1;
	for (jcr = *ku + 1; jcr <= i__1; ++jcr) {
	    ir = jcr - *ku;
	    irows = *n + *ku - jcr;
	    icols = *n + 1 - jcr;

	    scopy_(&icols, &a[ir + jcr * a_dim1], lda, &work[1], &c__1);
	    xnorms = work[1];
	    slarfg_(&icols, &xnorms, &work[2], &c__1, &tau);
	    work[1] = 1.f;

	    sgemv_("N", &irows, &icols, &c_b39, &a[ir + 1 + jcr * a_dim1], 
		    lda, &work[1], &c__1, &c_b23, &work[icols + 1], &c__1);
	    r__1 = -tau;
	    sger_(&irows, &icols, &r__1, &work[icols + 1], &c__1, &work[1], &
		    c__1, &a[ir + 1 + jcr * a_dim1], lda);

	    sgemv_("C", &icols, n, &c_b39, &a[jcr + a_dim1], lda, &work[1], &
		    c__1, &c_b23, &work[icols + 1], &c__1);
	    r__1 = -tau;
	    sger_(&icols, n, &r__1, &work[1], &c__1, &work[icols + 1], &c__1, 
		    &a[jcr + a_dim1], lda);

	    a[ir + jcr * a_dim1] = xnorms;
	    i__2 = icols - 1;
	    slaset_("Full", &c__1, &i__2, &c_b23, &c_b23, &a[ir + (jcr + 1) * 
		    a_dim1], lda);
/* L100: */
	}
    }

/*     Scale the matrix to have norm ANORM */

    if (*anorm >= 0.f) {
	temp = slange_("M", n, n, &a[a_offset], lda, tempa);
	if (temp > 0.f) {
	    alpha = *anorm / temp;
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		sscal_(n, &alpha, &a[j * a_dim1 + 1], &c__1);
/* L110: */
	    }
	}
    }

    return;

/*     End of SLATME */

} /* slatme_ */


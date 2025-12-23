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
static real c_b22 = 0.f;
static logical c_true = TRUE_;
static logical c_false = FALSE_;

/* > \brief \b SLATMT */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLATMT( M, N, DIST, ISEED, SYM, D, MODE, COND, DMAX, */
/*                          RANK, KL, KU, PACK, A, LDA, WORK, INFO ) */

/*       REAL               COND, DMAX */
/*       INTEGER            INFO, KL, KU, LDA, M, MODE, N, RANK */
/*       CHARACTER          DIST, PACK, SYM */
/*       REAL               A( LDA, * ), D( * ), WORK( * ) */
/*       INTEGER            ISEED( 4 ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    SLATMT generates random matrices with specified singular values */
/* >    (or symmetric/hermitian with specified eigenvalues) */
/* >    for testing LAPACK programs. */
/* > */
/* >    SLATMT operates by applying the following sequence of */
/* >    operations: */
/* > */
/* >      Set the diagonal to D, where D may be input or */
/* >         computed according to MODE, COND, DMAX, and SYM */
/* >         as described below. */
/* > */
/* >      Generate a matrix with the appropriate band structure, by one */
/* >         of two methods: */
/* > */
/* >      Method A: */
/* >          Generate a dense M x N matrix by multiplying D on the left */
/* >              and the right by random unitary matrices, then: */
/* > */
/* >          Reduce the bandwidth according to KL and KU, using */
/* >          Householder transformations. */
/* > */
/* >      Method B: */
/* >          Convert the bandwidth-0 (i.e., diagonal) matrix to a */
/* >              bandwidth-1 matrix using Givens rotations, "chasing" */
/* >              out-of-band elements back, much as in QR; then */
/* >              convert the bandwidth-1 to a bandwidth-2 matrix, etc. */
/* >              Note that for reasonably small bandwidths (relative to */
/* >              M and N) this requires less storage, as a dense matrix */
/* >              is not generated.  Also, for symmetric matrices, only */
/* >              one triangle is generated. */
/* > */
/* >      Method A is chosen if the bandwidth is a large fraction of the */
/* >          order of the matrix, and LDA is at least M (so a dense */
/* >          matrix can be stored.)  Method B is chosen if the bandwidth */
/* >          is small (< 1/2 N for symmetric, < .3 N+M for */
/* >          non-symmetric), or LDA is less than M and not less than the */
/* >          bandwidth. */
/* > */
/* >      Pack the matrix if desired. Options specified by PACK are: */
/* >         no packing */
/* >         zero out upper half (if symmetric) */
/* >         zero out lower half (if symmetric) */
/* >         store the upper half columnwise (if symmetric or upper */
/* >               triangular) */
/* >         store the lower half columnwise (if symmetric or lower */
/* >               triangular) */
/* >         store the lower triangle in banded format (if symmetric */
/* >               or lower triangular) */
/* >         store the upper triangle in banded format (if symmetric */
/* >               or upper triangular) */
/* >         store the entire matrix in banded format */
/* >      If Method B is chosen, and band format is specified, then the */
/* >         matrix will be generated in the band format, so no repacking */
/* >         will be necessary. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >           The number of rows of A. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           The number of columns of A. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] DIST */
/* > \verbatim */
/* >          DIST is CHARACTER*1 */
/* >           On entry, DIST specifies the type of distribution to be used */
/* >           to generate the random eigen-/singular values. */
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
/* >           exit, and can be used in the next call to SLATMT */
/* >           to continue the same random number sequence. */
/* >           Changed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] SYM */
/* > \verbatim */
/* >          SYM is CHARACTER*1 */
/* >           If SYM='S' or 'H', the generated matrix is symmetric, with */
/* >             eigenvalues specified by D, COND, MODE, and DMAX; they */
/* >             may be positive, negative, or zero. */
/* >           If SYM='P', the generated matrix is symmetric, with */
/* >             eigenvalues (= singular values) specified by D, COND, */
/* >             MODE, and DMAX; they will not be negative. */
/* >           If SYM='N', the generated matrix is nonsymmetric, with */
/* >             singular values specified by D, COND, MODE, and DMAX; */
/* >             they will not be negative. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is REAL array, dimension ( MIN( M , N ) ) */
/* >           This array is used to specify the singular values or */
/* >           eigenvalues of A (see SYM, above.)  If MODE=0, then D is */
/* >           assumed to contain the singular/eigenvalues, otherwise */
/* >           they will be computed according to MODE, COND, and DMAX, */
/* >           and placed in D. */
/* >           Modified if MODE is nonzero. */
/* > \endverbatim */
/* > */
/* > \param[in] MODE */
/* > \verbatim */
/* >          MODE is INTEGER */
/* >           On entry this describes how the singular/eigenvalues are to */
/* >           be specified: */
/* >           MODE = 0 means use D as input */
/* > */
/* >           MODE = 1 sets D(1)=1 and D(2:RANK)=1.0/COND */
/* >           MODE = 2 sets D(1:RANK-1)=1 and D(RANK)=1.0/COND */
/* >           MODE = 3 sets D(I)=COND**(-(I-1)/(RANK-1)) */
/* > */
/* >           MODE = 4 sets D(i)=1 - (i-1)/(N-1)*(1 - 1/COND) */
/* >           MODE = 5 sets D to random numbers in the range */
/* >                    ( 1/COND , 1 ) such that their logarithms */
/* >                    are uniformly distributed. */
/* >           MODE = 6 set D to random numbers from same distribution */
/* >                    as the rest of the matrix. */
/* >           MODE < 0 has the same meaning as ABS(MODE), except that */
/* >              the order of the elements of D is reversed. */
/* >           Thus if MODE is positive, D has entries ranging from */
/* >              1 to 1/COND, if negative, from 1/COND to 1, */
/* >           If SYM='S' or 'H', and MODE is neither 0, 6, nor -6, then */
/* >              the elements of D will also be multiplied by a random */
/* >              sign (i.e., +1 or -1.) */
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
/* >           DMAX / f2cmax(abs(D(i))); thus, the maximum absolute eigen- or */
/* >           singular value (which is to say the norm) will be abs(DMAX). */
/* >           Note that DMAX need not be positive: if DMAX is negative */
/* >           (or zero), D will be scaled by a negative number (or zero). */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] RANK */
/* > \verbatim */
/* >          RANK is INTEGER */
/* >           The rank of matrix to be generated for modes 1,2,3 only. */
/* >           D( RANK+1:N ) = 0. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >           This specifies the lower bandwidth of the  matrix. For */
/* >           example, KL=0 implies upper triangular, KL=1 implies upper */
/* >           Hessenberg, and KL being at least M-1 means that the matrix */
/* >           has full lower bandwidth.  KL must equal KU if the matrix */
/* >           is symmetric. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >           This specifies the upper bandwidth of the  matrix. For */
/* >           example, KU=0 implies lower triangular, KU=1 implies lower */
/* >           Hessenberg, and KU being at least N-1 means that the matrix */
/* >           has full upper bandwidth.  KL must equal KU if the matrix */
/* >           is symmetric. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] PACK */
/* > \verbatim */
/* >          PACK is CHARACTER*1 */
/* >           This specifies packing of matrix as follows: */
/* >           'N' => no packing */
/* >           'U' => zero out all subdiagonal entries (if symmetric) */
/* >           'L' => zero out all superdiagonal entries (if symmetric) */
/* >           'C' => store the upper triangle columnwise */
/* >                  (only if the matrix is symmetric or upper triangular) */
/* >           'R' => store the lower triangle columnwise */
/* >                  (only if the matrix is symmetric or lower triangular) */
/* >           'B' => store the lower triangle in band storage scheme */
/* >                  (only if matrix symmetric or lower triangular) */
/* >           'Q' => store the upper triangle in band storage scheme */
/* >                  (only if matrix symmetric or upper triangular) */
/* >           'Z' => store the entire matrix in band storage scheme */
/* >                      (pivoting can be provided for by using this */
/* >                      option to store A in the trailing rows of */
/* >                      the allocated storage) */
/* > */
/* >           Using these options, the various LAPACK packed and banded */
/* >           storage schemes can be obtained: */
/* >           GB               - use 'Z' */
/* >           PB, SB or TB     - use 'B' or 'Q' */
/* >           PP, SP or TP     - use 'C' or 'R' */
/* > */
/* >           If two calls to SLATMT differ only in the PACK parameter, */
/* >           they will generate mathematically equivalent matrices. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is REAL array, dimension ( LDA, N ) */
/* >           On exit A is the desired test matrix.  A is first generated */
/* >           in full (unpacked) form, and then packed, if so specified */
/* >           by PACK.  Thus, the first M elements of the first N */
/* >           columns will always be modified.  If PACK specifies a */
/* >           packed or banded storage scheme, all LDA elements of the */
/* >           first N columns will be modified; the elements of the */
/* >           array which do not correspond to elements of the generated */
/* >           matrix are set to zero. */
/* >           Modified. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >           LDA specifies the first dimension of A as declared in the */
/* >           calling program.  If PACK='N', 'U', 'L', 'C', or 'R', then */
/* >           LDA must be at least M.  If PACK='B' or 'Q', then LDA must */
/* >           be at least MIN( KL, M-1) (which is equal to MIN(KU,N-1)). */
/* >           If PACK='Z', LDA must be large enough to hold the packed */
/* >           array: MIN( KU, N-1) + MIN( KL, M-1) + 1. */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension ( 3*MAX( N , M ) ) */
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
/* >            -1 => M negative or unequal to N and SYM='S', 'H', or 'P' */
/* >            -2 => N negative */
/* >            -3 => DIST illegal string */
/* >            -5 => SYM illegal string */
/* >            -7 => MODE not in range -6 to 6 */
/* >            -8 => COND less than 1.0, and MODE neither -6, 0 nor 6 */
/* >           -10 => KL negative */
/* >           -11 => KU negative, or SYM='S' or 'H' and KU not equal to KL */
/* >           -12 => PACK illegal string, or PACK='U' or 'L', and SYM='N'; */
/* >                  or PACK='C' or 'Q' and SYM='N' and KL is not zero; */
/* >                  or PACK='R' or 'B' and SYM='N' and KU is not zero; */
/* >                  or PACK='U', 'L', 'C', 'R', 'B', or 'Q', and M is not */
/* >                  N. */
/* >           -14 => LDA is less than M, or PACK='Z' and LDA is less than */
/* >                  MIN(KU,N-1) + MIN(KL,M-1) + 1. */
/* >            1  => Error return from SLATM7 */
/* >            2  => Cannot scale to DMAX (f2cmax. sing. value is 0) */
/* >            3  => Error return from SLAGGE or SLAGSY */
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
/* Subroutine */ void slatmt_(integer *m, integer *n, char *dist, integer *
	iseed, char *sym, real *d__, integer *mode, real *cond, real *dmax__, 
	integer *rank, integer *kl, integer *ku, char *pack, real *a, integer 
	*lda, real *work, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5, i__6;
    real r__1, r__2, r__3;
    logical L__1;

    /* Local variables */
    integer ilda, icol;
    real temp;
    integer irow, isym;
    real c__;
    integer i__, j, k;
    real s, alpha, angle;
    integer ipack, ioffg;
    extern logical lsame_(char *, char *);
    integer iinfo;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    integer idist, mnmin, iskew;
    real extra, dummy;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *), slatm7_(integer *, real *, integer *, integer *, 
	    integer *, real *, integer *, integer *, integer *);
    integer ic, jc, nc, il, iendch, ir, jr, ipackg, mr;
    extern /* Subroutine */ void slagge_(integer *, integer *, integer *, 
	    integer *, real *, real *, integer *, integer *, real *, integer *
	    );
    integer minlda;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern real slarnd_(integer *, integer *);
    integer ioffst, irsign;
    logical givens, iltemp;
    extern /* Subroutine */ void slartg_(real *, real *, real *, real *, real *
	    ), slaset_(char *, integer *, integer *, real *, real *, real *, 
	    integer *), slagsy_(integer *, integer *, real *, real *, 
	    integer *, integer *, real *, integer *), slarot_(logical *, 
	    logical *, logical *, integer *, real *, real *, real *, integer *
	    , real *, real *);
    logical ilextr, topdwn;
    integer ir1, ir2, isympk, jch, llb, jkl, jku, uub;


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
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --work;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
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

/*     Decode SYM */

    if (lsame_(sym, "N")) {
	isym = 1;
	irsign = 0;
    } else if (lsame_(sym, "P")) {
	isym = 2;
	irsign = 0;
    } else if (lsame_(sym, "S")) {
	isym = 2;
	irsign = 1;
    } else if (lsame_(sym, "H")) {
	isym = 2;
	irsign = 1;
    } else {
	isym = -1;
    }

/*     Decode PACK */

    isympk = 0;
    if (lsame_(pack, "N")) {
	ipack = 0;
    } else if (lsame_(pack, "U")) {
	ipack = 1;
	isympk = 1;
    } else if (lsame_(pack, "L")) {
	ipack = 2;
	isympk = 1;
    } else if (lsame_(pack, "C")) {
	ipack = 3;
	isympk = 2;
    } else if (lsame_(pack, "R")) {
	ipack = 4;
	isympk = 3;
    } else if (lsame_(pack, "B")) {
	ipack = 5;
	isympk = 3;
    } else if (lsame_(pack, "Q")) {
	ipack = 6;
	isympk = 2;
    } else if (lsame_(pack, "Z")) {
	ipack = 7;
    } else {
	ipack = -1;
    }

/*     Set certain internal parameters */

    mnmin = f2cmin(*m,*n);
/* Computing MIN */
    i__1 = *kl, i__2 = *m - 1;
    llb = f2cmin(i__1,i__2);
/* Computing MIN */
    i__1 = *ku, i__2 = *n - 1;
    uub = f2cmin(i__1,i__2);
/* Computing MIN */
    i__1 = *m, i__2 = *n + llb;
    mr = f2cmin(i__1,i__2);
/* Computing MIN */
    i__1 = *n, i__2 = *m + uub;
    nc = f2cmin(i__1,i__2);

    if (ipack == 5 || ipack == 6) {
	minlda = uub + 1;
    } else if (ipack == 7) {
	minlda = llb + uub + 1;
    } else {
	minlda = *m;
    }

/*     Use Givens rotation method if bandwidth small enough, */
/*     or if LDA is too small to store the matrix unpacked. */

    givens = FALSE_;
    if (isym == 1) {
/* Computing MAX */
	i__1 = 1, i__2 = mr + nc;
	if ((real) (llb + uub) < (real) f2cmax(i__1,i__2) * .3f) {
	    givens = TRUE_;
	}
    } else {
	if (llb << 1 < *m) {
	    givens = TRUE_;
	}
    }
    if (*lda < *m && *lda >= minlda) {
	givens = TRUE_;
    }

/*     Set INFO if an error */

    if (*m < 0) {
	*info = -1;
    } else if (*m != *n && isym != 1) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (idist == -1) {
	*info = -3;
    } else if (isym == -1) {
	*info = -5;
    } else if (abs(*mode) > 6) {
	*info = -7;
    } else if (*mode != 0 && abs(*mode) != 6 && *cond < 1.f) {
	*info = -8;
    } else if (*kl < 0) {
	*info = -10;
    } else if (*ku < 0 || isym != 1 && *kl != *ku) {
	*info = -11;
    } else if (ipack == -1 || isympk == 1 && isym == 1 || isympk == 2 && isym 
	    == 1 && *kl > 0 || isympk == 3 && isym == 1 && *ku > 0 || isympk 
	    != 0 && *m != *n) {
	*info = -12;
    } else if (*lda < f2cmax(1,minlda)) {
	*info = -14;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SLATMT", &i__1, 6);
	return;
    }

/*     Initialize random number generator */

    for (i__ = 1; i__ <= 4; ++i__) {
	iseed[i__] = (i__1 = iseed[i__], abs(i__1)) % 4096;
/* L100: */
    }

    if (iseed[4] % 2 != 1) {
	++iseed[4];
    }

/*     2)      Set up D  if indicated. */

/*             Compute D according to COND and MODE */

    slatm7_(mode, cond, &irsign, &idist, &iseed[1], &d__[1], &mnmin, rank, &
	    iinfo);
    if (iinfo != 0) {
	*info = 1;
	return;
    }

/*     Choose Top-Down if D is (apparently) increasing, */
/*     Bottom-Up if D is (apparently) decreasing. */

    if (abs(d__[1]) <= (r__1 = d__[*rank], abs(r__1))) {
	topdwn = TRUE_;
    } else {
	topdwn = FALSE_;
    }

    if (*mode != 0 && abs(*mode) != 6) {

/*        Scale by DMAX */

	temp = abs(d__[1]);
	i__1 = *rank;
	for (i__ = 2; i__ <= i__1; ++i__) {
/* Computing MAX */
	    r__2 = temp, r__3 = (r__1 = d__[i__], abs(r__1));
	    temp = f2cmax(r__2,r__3);
/* L110: */
	}

	if (temp > 0.f) {
	    alpha = *dmax__ / temp;
	} else {
	    *info = 2;
	    return;
	}

	sscal_(rank, &alpha, &d__[1], &c__1);

    }

/*     3)      Generate Banded Matrix using Givens rotations. */
/*             Also the special case of UUB=LLB=0 */

/*               Compute Addressing constants to cover all */
/*               storage formats.  Whether GE, SY, GB, or SB, */
/*               upper or lower triangle or both, */
/*               the (i,j)-th element is in */
/*               A( i - ISKEW*j + IOFFST, j ) */

    if (ipack > 4) {
	ilda = *lda - 1;
	iskew = 1;
	if (ipack > 5) {
	    ioffst = uub + 1;
	} else {
	    ioffst = 1;
	}
    } else {
	ilda = *lda;
	iskew = 0;
	ioffst = 0;
    }

/*     IPACKG is the format that the matrix is generated in. If this is */
/*     different from IPACK, then the matrix must be repacked at the */
/*     end.  It also signals how to compute the norm, for scaling. */

    ipackg = 0;
    slaset_("Full", lda, n, &c_b22, &c_b22, &a[a_offset], lda);

/*     Diagonal Matrix -- We are done, unless it */
/*     is to be stored SP/PP/TP (PACK='R' or 'C') */

    if (llb == 0 && uub == 0) {
	i__1 = ilda + 1;
	scopy_(&mnmin, &d__[1], &c__1, &a[1 - iskew + ioffst + a_dim1], &i__1)
		;
	if (ipack <= 2 || ipack >= 5) {
	    ipackg = ipack;
	}

    } else if (givens) {

/*        Check whether to use Givens rotations, */
/*        Householder transformations, or nothing. */

	if (isym == 1) {

/*           Non-symmetric -- A = U D V */

	    if (ipack > 4) {
		ipackg = ipack;
	    } else {
		ipackg = 0;
	    }

	    i__1 = ilda + 1;
	    scopy_(&mnmin, &d__[1], &c__1, &a[1 - iskew + ioffst + a_dim1], &
		    i__1);

	    if (topdwn) {
		jkl = 0;
		i__1 = uub;
		for (jku = 1; jku <= i__1; ++jku) {

/*                 Transform from bandwidth JKL, JKU-1 to JKL, JKU */

/*                 Last row actually rotated is M */
/*                 Last column actually rotated is MIN( M+JKU, N ) */

/* Computing MIN */
		    i__3 = *m + jku;
		    i__2 = f2cmin(i__3,*n) + jkl - 1;
		    for (jr = 1; jr <= i__2; ++jr) {
			extra = 0.f;
			angle = slarnd_(&c__1, &iseed[1]) * 
				6.2831853071795864769252867663f;
			c__ = cos(angle);
			s = sin(angle);
/* Computing MAX */
			i__3 = 1, i__4 = jr - jkl;
			icol = f2cmax(i__3,i__4);
			if (jr < *m) {
/* Computing MIN */
			    i__3 = *n, i__4 = jr + jku;
			    il = f2cmin(i__3,i__4) + 1 - icol;
			    L__1 = jr > jkl;
			    slarot_(&c_true, &L__1, &c_false, &il, &c__, &s, &
				    a[jr - iskew * icol + ioffst + icol * 
				    a_dim1], &ilda, &extra, &dummy);
			}

/*                    Chase "EXTRA" back up */

			ir = jr;
			ic = icol;
			i__3 = -jkl - jku;
			for (jch = jr - jkl; i__3 < 0 ? jch >= 1 : jch <= 1; 
				jch += i__3) {
			    if (ir < *m) {
				slartg_(&a[ir + 1 - iskew * (ic + 1) + ioffst 
					+ (ic + 1) * a_dim1], &extra, &c__, &
					s, &dummy);
			    }
/* Computing MAX */
			    i__4 = 1, i__5 = jch - jku;
			    irow = f2cmax(i__4,i__5);
			    il = ir + 2 - irow;
			    temp = 0.f;
			    iltemp = jch > jku;
			    r__1 = -s;
			    slarot_(&c_false, &iltemp, &c_true, &il, &c__, &
				    r__1, &a[irow - iskew * ic + ioffst + ic *
				     a_dim1], &ilda, &temp, &extra);
			    if (iltemp) {
				slartg_(&a[irow + 1 - iskew * (ic + 1) + 
					ioffst + (ic + 1) * a_dim1], &temp, &
					c__, &s, &dummy);
/* Computing MAX */
				i__4 = 1, i__5 = jch - jku - jkl;
				icol = f2cmax(i__4,i__5);
				il = ic + 2 - icol;
				extra = 0.f;
				L__1 = jch > jku + jkl;
				r__1 = -s;
				slarot_(&c_true, &L__1, &c_true, &il, &c__, &
					r__1, &a[irow - iskew * icol + ioffst 
					+ icol * a_dim1], &ilda, &extra, &
					temp);
				ic = icol;
				ir = irow;
			    }
/* L120: */
			}
/* L130: */
		    }
/* L140: */
		}

		jku = uub;
		i__1 = llb;
		for (jkl = 1; jkl <= i__1; ++jkl) {

/*                 Transform from bandwidth JKL-1, JKU to JKL, JKU */

/* Computing MIN */
		    i__3 = *n + jkl;
		    i__2 = f2cmin(i__3,*m) + jku - 1;
		    for (jc = 1; jc <= i__2; ++jc) {
			extra = 0.f;
			angle = slarnd_(&c__1, &iseed[1]) * 
				6.2831853071795864769252867663f;
			c__ = cos(angle);
			s = sin(angle);
/* Computing MAX */
			i__3 = 1, i__4 = jc - jku;
			irow = f2cmax(i__3,i__4);
			if (jc < *n) {
/* Computing MIN */
			    i__3 = *m, i__4 = jc + jkl;
			    il = f2cmin(i__3,i__4) + 1 - irow;
			    L__1 = jc > jku;
			    slarot_(&c_false, &L__1, &c_false, &il, &c__, &s, 
				    &a[irow - iskew * jc + ioffst + jc * 
				    a_dim1], &ilda, &extra, &dummy);
			}

/*                    Chase "EXTRA" back up */

			ic = jc;
			ir = irow;
			i__3 = -jkl - jku;
			for (jch = jc - jku; i__3 < 0 ? jch >= 1 : jch <= 1; 
				jch += i__3) {
			    if (ic < *n) {
				slartg_(&a[ir + 1 - iskew * (ic + 1) + ioffst 
					+ (ic + 1) * a_dim1], &extra, &c__, &
					s, &dummy);
			    }
/* Computing MAX */
			    i__4 = 1, i__5 = jch - jkl;
			    icol = f2cmax(i__4,i__5);
			    il = ic + 2 - icol;
			    temp = 0.f;
			    iltemp = jch > jkl;
			    r__1 = -s;
			    slarot_(&c_true, &iltemp, &c_true, &il, &c__, &
				    r__1, &a[ir - iskew * icol + ioffst + 
				    icol * a_dim1], &ilda, &temp, &extra);
			    if (iltemp) {
				slartg_(&a[ir + 1 - iskew * (icol + 1) + 
					ioffst + (icol + 1) * a_dim1], &temp, 
					&c__, &s, &dummy);
/* Computing MAX */
				i__4 = 1, i__5 = jch - jkl - jku;
				irow = f2cmax(i__4,i__5);
				il = ir + 2 - irow;
				extra = 0.f;
				L__1 = jch > jkl + jku;
				r__1 = -s;
				slarot_(&c_false, &L__1, &c_true, &il, &c__, &
					r__1, &a[irow - iskew * icol + ioffst 
					+ icol * a_dim1], &ilda, &extra, &
					temp);
				ic = icol;
				ir = irow;
			    }
/* L150: */
			}
/* L160: */
		    }
/* L170: */
		}

	    } else {

/*              Bottom-Up -- Start at the bottom right. */

		jkl = 0;
		i__1 = uub;
		for (jku = 1; jku <= i__1; ++jku) {

/*                 Transform from bandwidth JKL, JKU-1 to JKL, JKU */

/*                 First row actually rotated is M */
/*                 First column actually rotated is MIN( M+JKU, N ) */

/* Computing MIN */
		    i__2 = *m, i__3 = *n + jkl;
		    iendch = f2cmin(i__2,i__3) - 1;
/* Computing MIN */
		    i__2 = *m + jku;
		    i__3 = 1 - jkl;
		    for (jc = f2cmin(i__2,*n) - 1; jc >= i__3; --jc) {
			extra = 0.f;
			angle = slarnd_(&c__1, &iseed[1]) * 
				6.2831853071795864769252867663f;
			c__ = cos(angle);
			s = sin(angle);
/* Computing MAX */
			i__2 = 1, i__4 = jc - jku + 1;
			irow = f2cmax(i__2,i__4);
			if (jc > 0) {
/* Computing MIN */
			    i__2 = *m, i__4 = jc + jkl + 1;
			    il = f2cmin(i__2,i__4) + 1 - irow;
			    L__1 = jc + jkl < *m;
			    slarot_(&c_false, &c_false, &L__1, &il, &c__, &s, 
				    &a[irow - iskew * jc + ioffst + jc * 
				    a_dim1], &ilda, &dummy, &extra);
			}

/*                    Chase "EXTRA" back down */

			ic = jc;
			i__2 = iendch;
			i__4 = jkl + jku;
			for (jch = jc + jkl; i__4 < 0 ? jch >= i__2 : jch <= 
				i__2; jch += i__4) {
			    ilextr = ic > 0;
			    if (ilextr) {
				slartg_(&a[jch - iskew * ic + ioffst + ic * 
					a_dim1], &extra, &c__, &s, &dummy);
			    }
			    ic = f2cmax(1,ic);
/* Computing MIN */
			    i__5 = *n - 1, i__6 = jch + jku;
			    icol = f2cmin(i__5,i__6);
			    iltemp = jch + jku < *n;
			    temp = 0.f;
			    i__5 = icol + 2 - ic;
			    slarot_(&c_true, &ilextr, &iltemp, &i__5, &c__, &
				    s, &a[jch - iskew * ic + ioffst + ic * 
				    a_dim1], &ilda, &extra, &temp);
			    if (iltemp) {
				slartg_(&a[jch - iskew * icol + ioffst + icol 
					* a_dim1], &temp, &c__, &s, &dummy);
/* Computing MIN */
				i__5 = iendch, i__6 = jch + jkl + jku;
				il = f2cmin(i__5,i__6) + 2 - jch;
				extra = 0.f;
				L__1 = jch + jkl + jku <= iendch;
				slarot_(&c_false, &c_true, &L__1, &il, &c__, &
					s, &a[jch - iskew * icol + ioffst + 
					icol * a_dim1], &ilda, &temp, &extra);
				ic = icol;
			    }
/* L180: */
			}
/* L190: */
		    }
/* L200: */
		}

		jku = uub;
		i__1 = llb;
		for (jkl = 1; jkl <= i__1; ++jkl) {

/*                 Transform from bandwidth JKL-1, JKU to JKL, JKU */

/*                 First row actually rotated is MIN( N+JKL, M ) */
/*                 First column actually rotated is N */

/* Computing MIN */
		    i__3 = *n, i__4 = *m + jku;
		    iendch = f2cmin(i__3,i__4) - 1;
/* Computing MIN */
		    i__3 = *n + jkl;
		    i__4 = 1 - jku;
		    for (jr = f2cmin(i__3,*m) - 1; jr >= i__4; --jr) {
			extra = 0.f;
			angle = slarnd_(&c__1, &iseed[1]) * 
				6.2831853071795864769252867663f;
			c__ = cos(angle);
			s = sin(angle);
/* Computing MAX */
			i__3 = 1, i__2 = jr - jkl + 1;
			icol = f2cmax(i__3,i__2);
			if (jr > 0) {
/* Computing MIN */
			    i__3 = *n, i__2 = jr + jku + 1;
			    il = f2cmin(i__3,i__2) + 1 - icol;
			    L__1 = jr + jku < *n;
			    slarot_(&c_true, &c_false, &L__1, &il, &c__, &s, &
				    a[jr - iskew * icol + ioffst + icol * 
				    a_dim1], &ilda, &dummy, &extra);
			}

/*                    Chase "EXTRA" back down */

			ir = jr;
			i__3 = iendch;
			i__2 = jkl + jku;
			for (jch = jr + jku; i__2 < 0 ? jch >= i__3 : jch <= 
				i__3; jch += i__2) {
			    ilextr = ir > 0;
			    if (ilextr) {
				slartg_(&a[ir - iskew * jch + ioffst + jch * 
					a_dim1], &extra, &c__, &s, &dummy);
			    }
			    ir = f2cmax(1,ir);
/* Computing MIN */
			    i__5 = *m - 1, i__6 = jch + jkl;
			    irow = f2cmin(i__5,i__6);
			    iltemp = jch + jkl < *m;
			    temp = 0.f;
			    i__5 = irow + 2 - ir;
			    slarot_(&c_false, &ilextr, &iltemp, &i__5, &c__, &
				    s, &a[ir - iskew * jch + ioffst + jch * 
				    a_dim1], &ilda, &extra, &temp);
			    if (iltemp) {
				slartg_(&a[irow - iskew * jch + ioffst + jch *
					 a_dim1], &temp, &c__, &s, &dummy);
/* Computing MIN */
				i__5 = iendch, i__6 = jch + jkl + jku;
				il = f2cmin(i__5,i__6) + 2 - jch;
				extra = 0.f;
				L__1 = jch + jkl + jku <= iendch;
				slarot_(&c_true, &c_true, &L__1, &il, &c__, &
					s, &a[irow - iskew * jch + ioffst + 
					jch * a_dim1], &ilda, &temp, &extra);
				ir = irow;
			    }
/* L210: */
			}
/* L220: */
		    }
/* L230: */
		}
	    }

	} else {

/*           Symmetric -- A = U D U' */

	    ipackg = ipack;
	    ioffg = ioffst;

	    if (topdwn) {

/*              Top-Down -- Generate Upper triangle only */

		if (ipack >= 5) {
		    ipackg = 6;
		    ioffg = uub + 1;
		} else {
		    ipackg = 1;
		}
		i__1 = ilda + 1;
		scopy_(&mnmin, &d__[1], &c__1, &a[1 - iskew + ioffg + a_dim1],
			 &i__1);

		i__1 = uub;
		for (k = 1; k <= i__1; ++k) {
		    i__4 = *n - 1;
		    for (jc = 1; jc <= i__4; ++jc) {
/* Computing MAX */
			i__2 = 1, i__3 = jc - k;
			irow = f2cmax(i__2,i__3);
/* Computing MIN */
			i__2 = jc + 1, i__3 = k + 2;
			il = f2cmin(i__2,i__3);
			extra = 0.f;
			temp = a[jc - iskew * (jc + 1) + ioffg + (jc + 1) * 
				a_dim1];
			angle = slarnd_(&c__1, &iseed[1]) * 
				6.2831853071795864769252867663f;
			c__ = cos(angle);
			s = sin(angle);
			L__1 = jc > k;
			slarot_(&c_false, &L__1, &c_true, &il, &c__, &s, &a[
				irow - iskew * jc + ioffg + jc * a_dim1], &
				ilda, &extra, &temp);
/* Computing MIN */
			i__3 = k, i__5 = *n - jc;
			i__2 = f2cmin(i__3,i__5) + 1;
			slarot_(&c_true, &c_true, &c_false, &i__2, &c__, &s, &
				a[(1 - iskew) * jc + ioffg + jc * a_dim1], &
				ilda, &temp, &dummy);

/*                    Chase EXTRA back up the matrix */

			icol = jc;
			i__2 = -k;
			for (jch = jc - k; i__2 < 0 ? jch >= 1 : jch <= 1; 
				jch += i__2) {
			    slartg_(&a[jch + 1 - iskew * (icol + 1) + ioffg + 
				    (icol + 1) * a_dim1], &extra, &c__, &s, &
				    dummy);
			    temp = a[jch - iskew * (jch + 1) + ioffg + (jch + 
				    1) * a_dim1];
			    i__3 = k + 2;
			    r__1 = -s;
			    slarot_(&c_true, &c_true, &c_true, &i__3, &c__, &
				    r__1, &a[(1 - iskew) * jch + ioffg + jch *
				     a_dim1], &ilda, &temp, &extra);
/* Computing MAX */
			    i__3 = 1, i__5 = jch - k;
			    irow = f2cmax(i__3,i__5);
/* Computing MIN */
			    i__3 = jch + 1, i__5 = k + 2;
			    il = f2cmin(i__3,i__5);
			    extra = 0.f;
			    L__1 = jch > k;
			    r__1 = -s;
			    slarot_(&c_false, &L__1, &c_true, &il, &c__, &
				    r__1, &a[irow - iskew * jch + ioffg + jch 
				    * a_dim1], &ilda, &extra, &temp);
			    icol = jch;
/* L240: */
			}
/* L250: */
		    }
/* L260: */
		}

/*              If we need lower triangle, copy from upper. Note that */
/*              the order of copying is chosen to work for 'q' -> 'b' */

		if (ipack != ipackg && ipack != 3) {
		    i__1 = *n;
		    for (jc = 1; jc <= i__1; ++jc) {
			irow = ioffst - iskew * jc;
/* Computing MIN */
			i__2 = *n, i__3 = jc + uub;
			i__4 = f2cmin(i__2,i__3);
			for (jr = jc; jr <= i__4; ++jr) {
			    a[jr + irow + jc * a_dim1] = a[jc - iskew * jr + 
				    ioffg + jr * a_dim1];
/* L270: */
			}
/* L280: */
		    }
		    if (ipack == 5) {
			i__1 = *n;
			for (jc = *n - uub + 1; jc <= i__1; ++jc) {
			    i__4 = uub + 1;
			    for (jr = *n + 2 - jc; jr <= i__4; ++jr) {
				a[jr + jc * a_dim1] = 0.f;
/* L290: */
			    }
/* L300: */
			}
		    }
		    if (ipackg == 6) {
			ipackg = ipack;
		    } else {
			ipackg = 0;
		    }
		}
	    } else {

/*              Bottom-Up -- Generate Lower triangle only */

		if (ipack >= 5) {
		    ipackg = 5;
		    if (ipack == 6) {
			ioffg = 1;
		    }
		} else {
		    ipackg = 2;
		}
		i__1 = ilda + 1;
		scopy_(&mnmin, &d__[1], &c__1, &a[1 - iskew + ioffg + a_dim1],
			 &i__1);

		i__1 = uub;
		for (k = 1; k <= i__1; ++k) {
		    for (jc = *n - 1; jc >= 1; --jc) {
/* Computing MIN */
			i__4 = *n + 1 - jc, i__2 = k + 2;
			il = f2cmin(i__4,i__2);
			extra = 0.f;
			temp = a[(1 - iskew) * jc + 1 + ioffg + jc * a_dim1];
			angle = slarnd_(&c__1, &iseed[1]) * 
				6.2831853071795864769252867663f;
			c__ = cos(angle);
			s = -sin(angle);
			L__1 = *n - jc > k;
			slarot_(&c_false, &c_true, &L__1, &il, &c__, &s, &a[(
				1 - iskew) * jc + ioffg + jc * a_dim1], &ilda,
				 &temp, &extra);
/* Computing MAX */
			i__4 = 1, i__2 = jc - k + 1;
			icol = f2cmax(i__4,i__2);
			i__4 = jc + 2 - icol;
			slarot_(&c_true, &c_false, &c_true, &i__4, &c__, &s, &
				a[jc - iskew * icol + ioffg + icol * a_dim1], 
				&ilda, &dummy, &temp);

/*                    Chase EXTRA back down the matrix */

			icol = jc;
			i__4 = *n - 1;
			i__2 = k;
			for (jch = jc + k; i__2 < 0 ? jch >= i__4 : jch <= 
				i__4; jch += i__2) {
			    slartg_(&a[jch - iskew * icol + ioffg + icol * 
				    a_dim1], &extra, &c__, &s, &dummy);
			    temp = a[(1 - iskew) * jch + 1 + ioffg + jch * 
				    a_dim1];
			    i__3 = k + 2;
			    slarot_(&c_true, &c_true, &c_true, &i__3, &c__, &
				    s, &a[jch - iskew * icol + ioffg + icol * 
				    a_dim1], &ilda, &extra, &temp);
/* Computing MIN */
			    i__3 = *n + 1 - jch, i__5 = k + 2;
			    il = f2cmin(i__3,i__5);
			    extra = 0.f;
			    L__1 = *n - jch > k;
			    slarot_(&c_false, &c_true, &L__1, &il, &c__, &s, &
				    a[(1 - iskew) * jch + ioffg + jch * 
				    a_dim1], &ilda, &temp, &extra);
			    icol = jch;
/* L310: */
			}
/* L320: */
		    }
/* L330: */
		}

/*              If we need upper triangle, copy from lower. Note that */
/*              the order of copying is chosen to work for 'b' -> 'q' */

		if (ipack != ipackg && ipack != 4) {
		    for (jc = *n; jc >= 1; --jc) {
			irow = ioffst - iskew * jc;
/* Computing MAX */
			i__2 = 1, i__4 = jc - uub;
			i__1 = f2cmax(i__2,i__4);
			for (jr = jc; jr >= i__1; --jr) {
			    a[jr + irow + jc * a_dim1] = a[jc - iskew * jr + 
				    ioffg + jr * a_dim1];
/* L340: */
			}
/* L350: */
		    }
		    if (ipack == 6) {
			i__1 = uub;
			for (jc = 1; jc <= i__1; ++jc) {
			    i__2 = uub + 1 - jc;
			    for (jr = 1; jr <= i__2; ++jr) {
				a[jr + jc * a_dim1] = 0.f;
/* L360: */
			    }
/* L370: */
			}
		    }
		    if (ipackg == 5) {
			ipackg = ipack;
		    } else {
			ipackg = 0;
		    }
		}
	    }
	}

    } else {

/*        4)      Generate Banded Matrix by first */
/*                Rotating by random Unitary matrices, */
/*                then reducing the bandwidth using Householder */
/*                transformations. */

/*                Note: we should get here only if LDA .ge. N */

	if (isym == 1) {

/*           Non-symmetric -- A = U D V */

	    slagge_(&mr, &nc, &llb, &uub, &d__[1], &a[a_offset], lda, &iseed[
		    1], &work[1], &iinfo);
	} else {

/*           Symmetric -- A = U D U' */

	    slagsy_(m, &llb, &d__[1], &a[a_offset], lda, &iseed[1], &work[1], 
		    &iinfo);

	}
	if (iinfo != 0) {
	    *info = 3;
	    return;
	}
    }

/*     5)      Pack the matrix */

    if (ipack != ipackg) {
	if (ipack == 1) {

/*           'U' -- Upper triangular, not packed */

	    i__1 = *m;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = *m;
		for (i__ = j + 1; i__ <= i__2; ++i__) {
		    a[i__ + j * a_dim1] = 0.f;
/* L380: */
		}
/* L390: */
	    }

	} else if (ipack == 2) {

/*           'L' -- Lower triangular, not packed */

	    i__1 = *m;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j - 1;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    a[i__ + j * a_dim1] = 0.f;
/* L400: */
		}
/* L410: */
	    }

	} else if (ipack == 3) {

/*           'C' -- Upper triangle packed Columnwise. */

	    icol = 1;
	    irow = 0;
	    i__1 = *m;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = j;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    ++irow;
		    if (irow > *lda) {
			irow = 1;
			++icol;
		    }
		    a[irow + icol * a_dim1] = a[i__ + j * a_dim1];
/* L420: */
		}
/* L430: */
	    }

	} else if (ipack == 4) {

/*           'R' -- Lower triangle packed Columnwise. */

	    icol = 1;
	    irow = 0;
	    i__1 = *m;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = *m;
		for (i__ = j; i__ <= i__2; ++i__) {
		    ++irow;
		    if (irow > *lda) {
			irow = 1;
			++icol;
		    }
		    a[irow + icol * a_dim1] = a[i__ + j * a_dim1];
/* L440: */
		}
/* L450: */
	    }

	} else if (ipack >= 5) {

/*           'B' -- The lower triangle is packed as a band matrix. */
/*           'Q' -- The upper triangle is packed as a band matrix. */
/*           'Z' -- The whole matrix is packed as a band matrix. */

	    if (ipack == 5) {
		uub = 0;
	    }
	    if (ipack == 6) {
		llb = 0;
	    }

	    i__1 = uub;
	    for (j = 1; j <= i__1; ++j) {
/* Computing MIN */
		i__2 = j + llb;
		for (i__ = f2cmin(i__2,*m); i__ >= 1; --i__) {
		    a[i__ - j + uub + 1 + j * a_dim1] = a[i__ + j * a_dim1];
/* L460: */
		}
/* L470: */
	    }

	    i__1 = *n;
	    for (j = uub + 2; j <= i__1; ++j) {
/* Computing MIN */
		i__4 = j + llb;
		i__2 = f2cmin(i__4,*m);
		for (i__ = j - uub; i__ <= i__2; ++i__) {
		    a[i__ - j + uub + 1 + j * a_dim1] = a[i__ + j * a_dim1];
/* L480: */
		}
/* L490: */
	    }
	}

/*        If packed, zero out extraneous elements. */

/*        Symmetric/Triangular Packed -- */
/*        zero out everything after A(IROW,ICOL) */

	if (ipack == 3 || ipack == 4) {
	    i__1 = *m;
	    for (jc = icol; jc <= i__1; ++jc) {
		i__2 = *lda;
		for (jr = irow + 1; jr <= i__2; ++jr) {
		    a[jr + jc * a_dim1] = 0.f;
/* L500: */
		}
		irow = 0;
/* L510: */
	    }

	} else if (ipack >= 5) {

/*           Packed Band -- */
/*              1st row is now in A( UUB+2-j, j), zero above it */
/*              m-th row is now in A( M+UUB-j,j), zero below it */
/*              last non-zero diagonal is now in A( UUB+LLB+1,j ), */
/*                 zero below it, too. */

	    ir1 = uub + llb + 2;
	    ir2 = uub + *m + 2;
	    i__1 = *n;
	    for (jc = 1; jc <= i__1; ++jc) {
		i__2 = uub + 1 - jc;
		for (jr = 1; jr <= i__2; ++jr) {
		    a[jr + jc * a_dim1] = 0.f;
/* L520: */
		}
/* Computing MAX */
/* Computing MIN */
		i__3 = ir1, i__5 = ir2 - jc;
		i__2 = 1, i__4 = f2cmin(i__3,i__5);
		i__6 = *lda;
		for (jr = f2cmax(i__2,i__4); jr <= i__6; ++jr) {
		    a[jr + jc * a_dim1] = 0.f;
/* L530: */
		}
/* L540: */
	    }
	}
    }

    return;

/*     End of SLATMT */

} /* slatmt_ */


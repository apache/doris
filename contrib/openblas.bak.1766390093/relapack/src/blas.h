#ifndef BLAS_H
#define BLAS_H

extern void BLAS(sswap)(const blasint *, float *, const blasint *, float *, const blasint *);
extern void BLAS(dswap)(const blasint *, double *, const blasint *, double *, const blasint *);
extern void BLAS(cswap)(const blasint *, float *, const blasint *, float *, const blasint *);
extern void BLAS(zswap)(const blasint *, double *, const blasint *, double *, const blasint *);

extern void BLAS(sscal)(const blasint *, const float *, float *, const blasint *);
extern void BLAS(dscal)(const blasint *, const double *, double *, const blasint *);
extern void BLAS(cscal)(const blasint *, const float *, float *, const blasint *);
extern void BLAS(zscal)(const blasint *, const double *, double *, const blasint *);

extern void BLAS(saxpy)(const blasint *, const float *, const float *, const blasint *, float *, const blasint *);
extern void BLAS(daxpy)(const blasint *, const double *, const double *, const blasint *, double *, const blasint *);
extern void BLAS(caxpy)(const blasint *, const float *, const float *, const blasint *, float *, const blasint *);
extern void BLAS(zaxpy)(const blasint *, const double *, const double *, const blasint *, double *, const blasint *);

extern void BLAS(sgemv)(const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, const float *, const blasint*);
extern void BLAS(dgemv)(const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, const double *, const blasint*);
extern void BLAS(cgemv)(const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, const float *, const blasint*);
extern void BLAS(zgemv)(const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, const double *, const blasint*);

extern void BLAS(sgemm)(const char *, const char *, const blasint *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, const float *, const blasint*);
extern void BLAS(dgemm)(const char *, const char *, const blasint *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, const double *, const blasint*);
extern void BLAS(cgemm)(const char *, const char *, const blasint *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, const float *, const blasint*);
extern void BLAS(zgemm)(const char *, const char *, const blasint *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, const double *, const blasint*);

extern void BLAS(strsm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, float *, const blasint *);
extern void BLAS(dtrsm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, double *, const blasint *);
extern void BLAS(ctrsm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, float *, const blasint *);
extern void BLAS(ztrsm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, double *, const blasint *);

extern void BLAS(strmm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, float *, const blasint *);
extern void BLAS(dtrmm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, double *, const blasint *);
extern void BLAS(ctrmm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, float *, const blasint *);
extern void BLAS(ztrmm)(const char *, const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, double *, const blasint *);

extern void BLAS(ssyrk)(const char *, const char *, const blasint *, const blasint *, const float *, float *, const blasint *, const float *, float *, const blasint *);
extern void BLAS(dsyrk)(const char *, const char *, const blasint *, const blasint *, const double *, double *, const blasint *, const double *, double *, const blasint *);
extern void BLAS(cherk)(const char *, const char *, const blasint *, const blasint *, const float *, float *, const blasint *, const float *, float *, const blasint *);
extern void BLAS(zherk)(const char *, const char *, const blasint *, const blasint *, const double *, double *, const blasint *, const double *, double *, const blasint *);

extern void BLAS(ssymm)(const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, float *, const blasint *);
extern void BLAS(dsymm)(const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, double *, const blasint *);
extern void BLAS(chemm)(const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, float *, const blasint *);
extern void BLAS(zhemm)(const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, double *, const blasint *);

extern void BLAS(ssyr2k)(const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, float *, const blasint *);
extern void BLAS(dsyr2k)(const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, double *, const blasint *);
extern void BLAS(cher2k)(const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, float *, const blasint *);
extern void BLAS(zher2k)(const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, double *, const blasint *);

#if HAVE_XGEMMT
extern void BLAS(sgemmt)(const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, const float *, const blasint*);
extern void BLAS(dgemmt)(const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, const double *, const blasint*);
extern void BLAS(cgemmt)(const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, const float *, const blasint*);
extern void BLAS(zgemmt)(const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, const double *, const blasint*);
#endif

#endif /* BLAS_H */

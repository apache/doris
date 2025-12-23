set(LAPACKE_INCLUDE lapack.h)

IF(LAPACKE)
    list(APPEND LAPACKE_INCLUDE lapacke.h lapacke_config.h lapacke_utils.h)
endif()

file(COPY ${LAPACKE_INCLUDE} DESTINATION ${LAPACK_BINARY_DIR}/include)

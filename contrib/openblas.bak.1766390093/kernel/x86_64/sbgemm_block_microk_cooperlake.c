#include <immintrin.h>

// Walk around those intrinsics that missed by compiler
#define MM256_LOADU_EPI16(addr)   \
            _mm256_maskz_loadu_epi16(~0, (addr))
#define MM256_STOREU_EPI16(addr, reg)  \
            _mm256_mask_storeu_epi16((addr), ~0, (reg))

// INCOPY Kernel, 16<M<=32, k can be any number
void COL_MAJOR_INCOPY_KERNEL_Kx32(BLASLONG k, BLASLONG m, bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    BLASLONG tag_k_2x = k & (~1);
    unsigned int tail_mask = (((unsigned int)0xffffffff) >> (32-m));

    __m512i array512_0, array512_1, array512_2, array512_3;

    bfloat16 * src_addr0, * src_addr1;
    bfloat16 * dst_addr0, * dst_addr1;

    BLASLONG LDA_2x = 2*lda;
    BLASLONG BF16_BLOCK_T_M_2x = 2*32;

    src_addr0 = A;
    src_addr1 = A + lda;
    dst_addr0 = block_A;
    dst_addr1 = block_A + 32;

    for (BLASLONG idx_k = 0; idx_k < tag_k_2x; idx_k += 2) {
        array512_0 = _mm512_maskz_loadu_epi16(tail_mask, src_addr0);
        array512_1 = _mm512_maskz_loadu_epi16(tail_mask, src_addr1);
        array512_2 = _mm512_unpacklo_epi16(array512_0, array512_1);
        array512_3 = _mm512_unpackhi_epi16(array512_0, array512_1);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);

        src_addr0 += LDA_2x;
        src_addr1 += LDA_2x;
        dst_addr0 += BF16_BLOCK_T_M_2x;
        dst_addr1 += BF16_BLOCK_T_M_2x;
    }

    if (tag_k_2x != k) {
        __m512i ZERO512 = _mm512_setzero_si512();
        array512_0 = _mm512_maskz_loadu_epi16(tail_mask, src_addr0);
        array512_2 = _mm512_unpacklo_epi16(array512_0, ZERO512);
        array512_3 = _mm512_unpackhi_epi16(array512_0, ZERO512);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
    }
}

// INCOPY Kernel, 0<M<=16, k can be any number
void COL_MAJOR_INCOPY_KERNEL_Kx16(BLASLONG k, BLASLONG m, bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    BLASLONG tag_k_2x = k & (~1);
    unsigned short tail_mask = (((unsigned short)0xffff) >> (16-m));

    __m256i array256_0, array256_1, array256_2, array256_3;

    bfloat16 * src_addr0, * src_addr1;
    bfloat16 * dst_addr0;

    BLASLONG LDA_2x = 2*lda;

    src_addr0 = A;
    src_addr1 = A + lda;
    dst_addr0 = block_A;

    for (BLASLONG idx_k = 0; idx_k < tag_k_2x; idx_k += 2) {
        array256_0 = _mm256_maskz_loadu_epi16(tail_mask, src_addr0);
        array256_1 = _mm256_maskz_loadu_epi16(tail_mask, src_addr1);
        array256_2 = _mm256_unpacklo_epi16(array256_0, array256_1);
        array256_3 = _mm256_unpackhi_epi16(array256_0, array256_1);
        // Store in one row of block_B
        MM256_STOREU_EPI16(dst_addr0,    array256_2);
        MM256_STOREU_EPI16(dst_addr0+16, array256_3);

        src_addr0 += LDA_2x;
        src_addr1 += LDA_2x;
        dst_addr0 += 32;
    }

    if (tag_k_2x != k) {
        __m256i ZERO256 = _mm256_setzero_si256();
        array256_0 = _mm256_maskz_loadu_epi16(tail_mask, src_addr0);
        array256_2 = _mm256_unpacklo_epi16(array256_0, ZERO256);
        array256_3 = _mm256_unpackhi_epi16(array256_0, ZERO256);
        // Store in one row of block_B
        MM256_STOREU_EPI16(dst_addr0,    array256_2);
        MM256_STOREU_EPI16(dst_addr0+16, array256_3);
    }
}

// K=32, M=16
void COL_MAJOR_ITCOPY_KERNEL_32x16(bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    bfloat16 * src_addr0, * src_addr1, * src_addr2, * src_addr3;
    bfloat16 * dst_addr0, * dst_addr1;

    BLASLONG LDA_4x = lda*4;

    src_addr0 = A;
    src_addr1 = A + lda;
    src_addr2 = A + lda*2;
    src_addr3 = A + lda*3;
    dst_addr0 = block_A;
    dst_addr1 = block_A + 32*8;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512_way0_0, array512_way0_1, array512_way0_2, array512_way0_3;
    __m512i array512_way1_0, array512_way1_1, array512_way1_2, array512_way1_3;
    __m512i array512_way2_0, array512_way2_1, array512_way2_2, array512_way2_3;
    __m512i array512_way3_0, array512_way3_1, array512_way3_2, array512_way3_3;

    __m512i M512_EPI64_2   = _mm512_set1_epi64(2);
    __m512i permute_lo_idx = _mm512_set_epi64(13, 12, 5, 4, 9, 8, 1, 0);
    __m512i permute_hi_idx = _mm512_add_epi64(permute_lo_idx, M512_EPI64_2);

    // Load and preprocess 1st 4 rows
    array512_way0_0 = _mm512_loadu_si512(src_addr0);
    array512_way0_1 = _mm512_loadu_si512(src_addr1);
    array512_way0_2 = _mm512_loadu_si512(src_addr2);
    array512_way0_3 = _mm512_loadu_si512(src_addr3);
    array512_0 = _mm512_unpacklo_epi32(array512_way0_0, array512_way0_1);
    array512_1 = _mm512_unpackhi_epi32(array512_way0_0, array512_way0_1);
    array512_2 = _mm512_unpacklo_epi32(array512_way0_2, array512_way0_3);
    array512_3 = _mm512_unpackhi_epi32(array512_way0_2, array512_way0_3);
    array512_way0_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
    array512_way0_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
    array512_way0_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
    array512_way0_3 = _mm512_unpackhi_epi64(array512_1, array512_3);
    src_addr0 += LDA_4x;
    src_addr1 += LDA_4x;
    src_addr2 += LDA_4x;
    src_addr3 += LDA_4x;

    // Load and preprocess 2nd 4 rows
    array512_way1_0 = _mm512_loadu_si512(src_addr0);
    array512_way1_1 = _mm512_loadu_si512(src_addr1);
    array512_way1_2 = _mm512_loadu_si512(src_addr2);
    array512_way1_3 = _mm512_loadu_si512(src_addr3);
    array512_0 = _mm512_unpacklo_epi32(array512_way1_0, array512_way1_1);
    array512_1 = _mm512_unpackhi_epi32(array512_way1_0, array512_way1_1);
    array512_2 = _mm512_unpacklo_epi32(array512_way1_2, array512_way1_3);
    array512_3 = _mm512_unpackhi_epi32(array512_way1_2, array512_way1_3);
    array512_way1_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
    array512_way1_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
    array512_way1_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
    array512_way1_3 = _mm512_unpackhi_epi64(array512_1, array512_3);
    src_addr0 += LDA_4x;
    src_addr1 += LDA_4x;
    src_addr2 += LDA_4x;
    src_addr3 += LDA_4x;

    // Load and preprocess 3rd 4 rows
    array512_way2_0 = _mm512_loadu_si512(src_addr0);
    array512_way2_1 = _mm512_loadu_si512(src_addr1);
    array512_way2_2 = _mm512_loadu_si512(src_addr2);
    array512_way2_3 = _mm512_loadu_si512(src_addr3);
    array512_0 = _mm512_unpacklo_epi32(array512_way2_0, array512_way2_1);
    array512_1 = _mm512_unpackhi_epi32(array512_way2_0, array512_way2_1);
    array512_2 = _mm512_unpacklo_epi32(array512_way2_2, array512_way2_3);
    array512_3 = _mm512_unpackhi_epi32(array512_way2_2, array512_way2_3);
    array512_way2_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
    array512_way2_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
    array512_way2_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
    array512_way2_3 = _mm512_unpackhi_epi64(array512_1, array512_3);
    src_addr0 += LDA_4x;
    src_addr1 += LDA_4x;
    src_addr2 += LDA_4x;
    src_addr3 += LDA_4x;

    // Load and preprocess 4th 4 rows
    array512_way3_0 = _mm512_loadu_si512(src_addr0);
    array512_way3_1 = _mm512_loadu_si512(src_addr1);
    array512_way3_2 = _mm512_loadu_si512(src_addr2);
    array512_way3_3 = _mm512_loadu_si512(src_addr3);
    array512_0 = _mm512_unpacklo_epi32(array512_way3_0, array512_way3_1);
    array512_1 = _mm512_unpackhi_epi32(array512_way3_0, array512_way3_1);
    array512_2 = _mm512_unpacklo_epi32(array512_way3_2, array512_way3_3);
    array512_3 = _mm512_unpackhi_epi32(array512_way3_2, array512_way3_3);
    array512_way3_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
    array512_way3_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
    array512_way3_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
    array512_way3_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

    // Compose and store the 0/1 and 16/17 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_lo_idx, array512_way1_0);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_0, permute_lo_idx, array512_way3_0);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 2/3 and 18/19 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_1, permute_lo_idx, array512_way1_1);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_1, permute_lo_idx, array512_way3_1);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 4/5 and 20/21 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_2, permute_lo_idx, array512_way1_2);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_2, permute_lo_idx, array512_way3_2);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 6/7 and 22/23 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_3, permute_lo_idx, array512_way1_3);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_3, permute_lo_idx, array512_way3_3);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 8/9 and 24/25 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_hi_idx, array512_way1_0);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_0, permute_hi_idx, array512_way3_0);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 10/11 and 26/27 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_1, permute_hi_idx, array512_way1_1);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_1, permute_hi_idx, array512_way3_1);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 12/13 and 28/29 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_2, permute_hi_idx, array512_way1_2);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_2, permute_hi_idx, array512_way3_2);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
    dst_addr0 += 32;
    dst_addr1 += 32;

    // Compose and store the 14/15 and 30/31 cols
    array512_0 = _mm512_permutex2var_epi64(array512_way0_3, permute_hi_idx, array512_way1_3);
    array512_1 = _mm512_permutex2var_epi64(array512_way2_3, permute_hi_idx, array512_way3_3);
    array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
    array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_1, 0x1), 0x0);
    _mm512_storeu_si512(dst_addr0, array512_2);
    _mm512_storeu_si512(dst_addr1, array512_3);
}

// K=Any number but will be processed based on 32, M=32
void COL_MAJOR_ITCOPY_KERNEL_Kx32(BLASLONG k, bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    bfloat16 * src_addr0, * src_addr1, * src_addr2, * src_addr3;
    bfloat16 * dst_addr0, * dst_addr1;

    BLASLONG tag_k_32x = k & (~31);

    BLASLONG LDA_4x  = lda*4;
    BLASLONG LDA_8x  = lda*8;
    BLASLONG LDA_12x = lda*12;
    BLASLONG LDA_16x = lda*16;

    src_addr0 = A;
    src_addr1 = A + lda;
    src_addr2 = A + lda*2;
    src_addr3 = A + lda*3;
    dst_addr0 = block_A;
    dst_addr1 = block_A + 32*16;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512_way0_0, array512_way0_1, array512_way0_2, array512_way0_3;
    __m512i array512_way1_0, array512_way1_1, array512_way1_2, array512_way1_3;
    __m512i array512_way2_0, array512_way2_1, array512_way2_2, array512_way2_3;
    __m512i array512_way3_0, array512_way3_1, array512_way3_2, array512_way3_3;

    __m512i M512_EPI64_2   = _mm512_set1_epi64(2);
    __m512i permute_lo_idx = _mm512_set_epi64(13, 12, 5, 4, 9, 8, 1, 0);
    __m512i permute_hi_idx = _mm512_add_epi64(permute_lo_idx, M512_EPI64_2);

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        for (int i = 0; i < 2; i++) {
            // Load and preprocess 1st 4 rows
            array512_way0_0 = _mm512_loadu_si512(src_addr0+idx_k);
            array512_way0_1 = _mm512_loadu_si512(src_addr1+idx_k);
            array512_way0_2 = _mm512_loadu_si512(src_addr2+idx_k);
            array512_way0_3 = _mm512_loadu_si512(src_addr3+idx_k);
            array512_0 = _mm512_unpacklo_epi32(array512_way0_0, array512_way0_1);
            array512_1 = _mm512_unpackhi_epi32(array512_way0_0, array512_way0_1);
            array512_2 = _mm512_unpacklo_epi32(array512_way0_2, array512_way0_3);
            array512_3 = _mm512_unpackhi_epi32(array512_way0_2, array512_way0_3);
            array512_way0_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512_way0_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512_way0_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512_way0_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

            // Load and preprocess 2nd 4 rows
            array512_way1_0 = _mm512_loadu_si512(src_addr0+LDA_4x+idx_k);
            array512_way1_1 = _mm512_loadu_si512(src_addr1+LDA_4x+idx_k);
            array512_way1_2 = _mm512_loadu_si512(src_addr2+LDA_4x+idx_k);
            array512_way1_3 = _mm512_loadu_si512(src_addr3+LDA_4x+idx_k);
            array512_0 = _mm512_unpacklo_epi32(array512_way1_0, array512_way1_1);
            array512_1 = _mm512_unpackhi_epi32(array512_way1_0, array512_way1_1);
            array512_2 = _mm512_unpacklo_epi32(array512_way1_2, array512_way1_3);
            array512_3 = _mm512_unpackhi_epi32(array512_way1_2, array512_way1_3);
            array512_way1_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512_way1_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512_way1_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512_way1_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

            // Load and preprocess 3rd 4 rows
            array512_way2_0 = _mm512_loadu_si512(src_addr0+LDA_8x+idx_k);
            array512_way2_1 = _mm512_loadu_si512(src_addr1+LDA_8x+idx_k);
            array512_way2_2 = _mm512_loadu_si512(src_addr2+LDA_8x+idx_k);
            array512_way2_3 = _mm512_loadu_si512(src_addr3+LDA_8x+idx_k);
            array512_0 = _mm512_unpacklo_epi32(array512_way2_0, array512_way2_1);
            array512_1 = _mm512_unpackhi_epi32(array512_way2_0, array512_way2_1);
            array512_2 = _mm512_unpacklo_epi32(array512_way2_2, array512_way2_3);
            array512_3 = _mm512_unpackhi_epi32(array512_way2_2, array512_way2_3);
            array512_way2_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512_way2_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512_way2_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512_way2_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

            // Load and preprocess 4th 4 rows
            array512_way3_0 = _mm512_loadu_si512(src_addr0+LDA_12x+idx_k);
            array512_way3_1 = _mm512_loadu_si512(src_addr1+LDA_12x+idx_k);
            array512_way3_2 = _mm512_loadu_si512(src_addr2+LDA_12x+idx_k);
            array512_way3_3 = _mm512_loadu_si512(src_addr3+LDA_12x+idx_k);
            array512_0 = _mm512_unpacklo_epi32(array512_way3_0, array512_way3_1);
            array512_1 = _mm512_unpackhi_epi32(array512_way3_0, array512_way3_1);
            array512_2 = _mm512_unpacklo_epi32(array512_way3_2, array512_way3_3);
            array512_3 = _mm512_unpackhi_epi32(array512_way3_2, array512_way3_3);
            array512_way3_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512_way3_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512_way3_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512_way3_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

            // Compose and store the 0/1 and 16/17 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_lo_idx, array512_way1_0);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_0, permute_lo_idx, array512_way3_0);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 2/3 and 18/19 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_1, permute_lo_idx, array512_way1_1);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_1, permute_lo_idx, array512_way3_1);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 4/5 and 20/21 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_2, permute_lo_idx, array512_way1_2);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_2, permute_lo_idx, array512_way3_2);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 6/7 and 22/23 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_3, permute_lo_idx, array512_way1_3);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_3, permute_lo_idx, array512_way3_3);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 8/9 and 24/25 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_hi_idx, array512_way1_0);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_0, permute_hi_idx, array512_way3_0);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 10/11 and 26/27 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_1, permute_hi_idx, array512_way1_1);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_1, permute_hi_idx, array512_way3_1);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 12/13 and 28/29 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_2, permute_hi_idx, array512_way1_2);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_2, permute_hi_idx, array512_way3_2);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;

            // Compose and store the 14/15 and 30/31 cols
            array512_0 = _mm512_permutex2var_epi64(array512_way0_3, permute_hi_idx, array512_way1_3);
            array512_1 = _mm512_permutex2var_epi64(array512_way2_3, permute_hi_idx, array512_way3_3);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);

            src_addr0 += LDA_16x;
            src_addr1 += LDA_16x;
            src_addr2 += LDA_16x;
            src_addr3 += LDA_16x;
            dst_addr0 -= (64*7 - 32);
            dst_addr1 -= (64*7 - 32);
        }
        src_addr0 -= (LDA_16x*2);
        src_addr1 -= (LDA_16x*2);
        src_addr2 -= (LDA_16x*2);
        src_addr3 -= (LDA_16x*2);
        dst_addr0 += (32*30);
        dst_addr1 += (32*30);
    }

    if (tag_k_32x != k) {
        int k_rem = k - tag_k_32x;
        unsigned int tail_mask = (((unsigned int)0xffffffff) >> (32-k_rem));
        __m512i array512[16];

        bfloat16 * dst_addr_tmp = dst_addr0;

        for (int i = 0; i < 2; i++) {
            // Load and preprocess 1st 4 rows
            array512[0] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
            array512[1] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
            array512[2] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
            array512[3] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
            array512_0 = _mm512_unpacklo_epi32(array512[0], array512[1]);
            array512_1 = _mm512_unpackhi_epi32(array512[0], array512[1]);
            array512_2 = _mm512_unpacklo_epi32(array512[2], array512[3]);
            array512_3 = _mm512_unpackhi_epi32(array512[2], array512[3]);
            array512[0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[3] = _mm512_unpackhi_epi64(array512_1, array512_3);
            src_addr0 += LDA_4x;
            src_addr1 += LDA_4x;
            src_addr2 += LDA_4x;
            src_addr3 += LDA_4x;

            // Load and preprocess 2nd 4 rows
            array512[4] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
            array512[5] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
            array512[6] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
            array512[7] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
            array512_0 = _mm512_unpacklo_epi32(array512[4], array512[5]);
            array512_1 = _mm512_unpackhi_epi32(array512[4], array512[5]);
            array512_2 = _mm512_unpacklo_epi32(array512[6], array512[7]);
            array512_3 = _mm512_unpackhi_epi32(array512[6], array512[7]);
            array512[4] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[5] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[6] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[7] = _mm512_unpackhi_epi64(array512_1, array512_3);
            src_addr0 += LDA_4x;
            src_addr1 += LDA_4x;
            src_addr2 += LDA_4x;
            src_addr3 += LDA_4x;

            // Load and preprocess 3rd 4 rows
            array512[8]  = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
            array512[9]  = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
            array512[10] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
            array512[11] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
            array512_0 = _mm512_unpacklo_epi32(array512[8],  array512[9]);
            array512_1 = _mm512_unpackhi_epi32(array512[8],  array512[9]);
            array512_2 = _mm512_unpacklo_epi32(array512[10], array512[11]);
            array512_3 = _mm512_unpackhi_epi32(array512[10], array512[11]);
            array512[8]  = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[9]  = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[10] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[11] = _mm512_unpackhi_epi64(array512_1, array512_3);
            src_addr0 += LDA_4x;
            src_addr1 += LDA_4x;
            src_addr2 += LDA_4x;
            src_addr3 += LDA_4x;

            // Load and preprocess 4th 4 rows
            array512[12] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
            array512[13] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
            array512[14] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
            array512[15] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
            array512_0 = _mm512_unpacklo_epi32(array512[12], array512[13]);
            array512_1 = _mm512_unpackhi_epi32(array512[12], array512[13]);
            array512_2 = _mm512_unpacklo_epi32(array512[14], array512[15]);
            array512_3 = _mm512_unpackhi_epi32(array512[14], array512[15]);
            array512[12] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[13] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[14] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[15] = _mm512_unpackhi_epi64(array512_1, array512_3);
            src_addr0 += LDA_4x;
            src_addr1 += LDA_4x;
            src_addr2 += LDA_4x;
            src_addr3 += LDA_4x;

            // array512_01_1617_0, array512_01_1617_1, array512_89_2425_0, array512_89_2425_1;
            // Half-compose of 0/1, 16/17, 8/9, 24/25 cols
            array512_0 = _mm512_permutex2var_epi64(array512[0], permute_lo_idx, array512[4]);
            array512_1 = _mm512_permutex2var_epi64(array512[8], permute_lo_idx, array512[12]);
            array512_2 = _mm512_permutex2var_epi64(array512[0], permute_hi_idx, array512[4]);
            array512_3 = _mm512_permutex2var_epi64(array512[8], permute_hi_idx, array512[12]);
            array512[0]  = array512_0;  // 1st 8 pairs of col 0/1,   and 1st 8 pairs of col 16/17
            array512[4]  = array512_1;  // 2nd 8 pairs of col 0/1,   and 2nd 8 pairs of col 16/17
            array512[8]  = array512_2;  // 1st 8 pairs of col 8/9,   and 1st 8 pairs of col 24/25
            array512[12] = array512_3;  // 2nd 8 pairs of col 8/9,   and 2nd 8 pairs of col 24/25

            // Half-compose of 2/3, 18/19, 10/11, 26/27 cols
            array512_0 = _mm512_permutex2var_epi64(array512[1], permute_lo_idx, array512[5]);
            array512_1 = _mm512_permutex2var_epi64(array512[9], permute_lo_idx, array512[13]);
            array512_2 = _mm512_permutex2var_epi64(array512[1], permute_hi_idx, array512[5]);
            array512_3 = _mm512_permutex2var_epi64(array512[9], permute_hi_idx, array512[13]);
            array512[1]  = array512_0;  // 1st 8 pairs of col 2/3,   and 1st 8 pairs of col 18/19
            array512[5]  = array512_1;  // 2nd 8 pairs of col 2/3,   and 2nd 8 pairs of col 18/19
            array512[9]  = array512_2;  // 1st 8 pairs of col 10/11, and 1st 8 pairs of col 26/27
            array512[13] = array512_3;  // 2nd 8 pairs of col 10/11, and 2nd 8 pairs of col 26/27

            // Half-compose of 4/5, 20/21, 12/13, 28/29 cols
            array512_0 = _mm512_permutex2var_epi64(array512[2],  permute_lo_idx, array512[6]);
            array512_1 = _mm512_permutex2var_epi64(array512[10], permute_lo_idx, array512[14]);
            array512_2 = _mm512_permutex2var_epi64(array512[2],  permute_hi_idx, array512[6]);
            array512_3 = _mm512_permutex2var_epi64(array512[10], permute_hi_idx, array512[14]);
            array512[2]  = array512_0;  // 1st 8 pairs of col 4/5,   and 1st 8 pairs of col 20/21
            array512[6]  = array512_1;  // 2nd 8 pairs of col 4/5,   and 2nd 8 pairs of col 20/21
            array512[10] = array512_2;  // 1st 8 pairs of col 12/13, and 1st 8 pairs of col 28/29
            array512[14] = array512_3;  // 2nd 8 pairs of col 12/13, and 2nd 8 pairs of col 28/29

            // Half-compose of 6/7, 22/23, 14/15, 30/31 cols
            array512_0 = _mm512_permutex2var_epi64(array512[3],  permute_lo_idx, array512[7]);
            array512_1 = _mm512_permutex2var_epi64(array512[11], permute_lo_idx, array512[15]);
            array512_2 = _mm512_permutex2var_epi64(array512[3],  permute_hi_idx, array512[7]);
            array512_3 = _mm512_permutex2var_epi64(array512[11], permute_hi_idx, array512[15]);
            array512[3]  = array512_0;  // 1st 8 pairs of col 6/7,   and 1st 8 pairs of col 22/23
            array512[7]  = array512_1;  // 2nd 8 pairs of col 6/7,   and 2nd 8 pairs of col 22/23
            array512[11] = array512_2;  // 1st 8 pairs of col 14/15, and 1st 8 pairs of col 30/31
            array512[15] = array512_3;  // 2nd 8 pairs of col 14/15, and 2nd 8 pairs of col 30/31

            // Compose and store the 0/1 cols
            array512_0 = _mm512_inserti64x4(array512[0], _mm512_castsi512_si256(array512[4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 2/3 cols
            array512_0 = _mm512_inserti64x4(array512[1], _mm512_castsi512_si256(array512[5]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 4/5 cols
            array512_0 = _mm512_inserti64x4(array512[2], _mm512_castsi512_si256(array512[6]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 6/7 cols
            array512_0 = _mm512_inserti64x4(array512[3], _mm512_castsi512_si256(array512[7]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 8/9 cols
            array512_0 = _mm512_inserti64x4(array512[8], _mm512_castsi512_si256(array512[12]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 10/11 cols
            array512_0 = _mm512_inserti64x4(array512[9], _mm512_castsi512_si256(array512[13]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 12/13 cols
            array512_0 = _mm512_inserti64x4(array512[10], _mm512_castsi512_si256(array512[14]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store the 14/15 cols
            array512_0 = _mm512_inserti64x4(array512[11], _mm512_castsi512_si256(array512[15]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;

            // Compose and store 16 ~ k_rem cols
            int idx_length = (k_rem + 1 - 16) >> 1;
            if (idx_length > 4) {
                for (int idx_k = 0; idx_k < 4; idx_k++) {
                    array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                    _mm512_storeu_si512(dst_addr0, array512_0);
                    dst_addr0 += 64;
                }

                for (int idx_k = 4; idx_k < idx_length; idx_k++) {
                    array512_0 = _mm512_inserti64x4(array512[idx_k+8], _mm512_extracti64x4_epi64(array512[idx_k+4], 0x1), 0x0);
                    _mm512_storeu_si512(dst_addr0, array512_0);
                    dst_addr0 += 64;
                }
            } else {
                for (int idx_k = 0; idx_k < idx_length; idx_k++) {
                    array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                    _mm512_storeu_si512(dst_addr0, array512_0);
                    dst_addr0 += 64;
                }
            }

            dst_addr0 = dst_addr_tmp + 32;
        }
    }
}

// K=Any number but will be processed based on 32, 16<M<32
void COL_MAJOR_ITCOPY_KERNEL_Kx32m(BLASLONG m, BLASLONG k, bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    bfloat16 * src_addr0, * src_addr1, * src_addr2, * src_addr3;
    bfloat16 * dst_addr0, * dst_addr1;

    BLASLONG tag_k_32x = k & (~31);

    BLASLONG LDA_4x  = lda*4;

    BLASLONG m_rem = m-16;

    src_addr0 = A;
    src_addr1 = A + lda;
    src_addr2 = A + lda*2;
    src_addr3 = A + lda*3;
    dst_addr0 = block_A;
    dst_addr1 = block_A + 32*16;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512[16];

    __m512i M512_EPI64_2   = _mm512_set1_epi64(2);
    __m512i permute_lo_idx = _mm512_set_epi64(13, 12, 5, 4, 9, 8, 1, 0);
    __m512i permute_hi_idx = _mm512_add_epi64(permute_lo_idx, M512_EPI64_2);

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        for (int j = 0; j < 4; j++) {
            int array_idx = j*4;
            // Load and preprocess 4 rows
            array512[array_idx+0] = _mm512_loadu_si512(src_addr0+idx_k);
            array512[array_idx+1] = _mm512_loadu_si512(src_addr1+idx_k);
            array512[array_idx+2] = _mm512_loadu_si512(src_addr2+idx_k);
            array512[array_idx+3] = _mm512_loadu_si512(src_addr3+idx_k);
            array512_0 = _mm512_unpacklo_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_1 = _mm512_unpackhi_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_2 = _mm512_unpacklo_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512_3 = _mm512_unpackhi_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512[array_idx+0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[array_idx+1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[array_idx+2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[array_idx+3] = _mm512_unpackhi_epi64(array512_1, array512_3);
            src_addr0 += LDA_4x;
            src_addr1 += LDA_4x;
            src_addr2 += LDA_4x;
            src_addr3 += LDA_4x;
        }

        // Compose and store the 0/1, 2/3, 4/5, 6/7 and 16/17, 18/19, 20/21, 22/23 cols
        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_lo_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_lo_idx, array512[j+12]);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;
        }

        // Compose and store the 8/9, 10/11, 12/13, 14/15 and 24/25, 26/27, 28/29, 30/31 cols
        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_hi_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_hi_idx, array512[j+12]);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;
        }

        dst_addr0 -= (64*8 - 32);
        dst_addr1 -= (64*8 - 32);

        for (int j = 0; j < m_rem; j++) {
            array512[j] = _mm512_loadu_si512(src_addr0+j*lda+idx_k);
        }
        for (int j = m_rem; j < 16; j++) {
            array512[j] = _mm512_setzero_si512();
        }

        for (int j = 0; j < 4; j++) {
            int array_idx = j*4;
            array512_0 = _mm512_unpacklo_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_1 = _mm512_unpackhi_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_2 = _mm512_unpacklo_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512_3 = _mm512_unpackhi_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512[array_idx+0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[array_idx+1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[array_idx+2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[array_idx+3] = _mm512_unpackhi_epi64(array512_1, array512_3);
        }

        // Compose and store the 0/1, 2/3, 4/5, 6/7 and 16/17, 18/19, 20/21, 22/23 cols
        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_lo_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_lo_idx, array512[j+12]);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;
        }

        // Compose and store the 8/9, 10/11, 12/13, 14/15 and 24/25, 26/27, 28/29, 30/31 cols
        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_hi_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_hi_idx, array512[j+12]);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 64;
            dst_addr1 += 64;
        }

        src_addr0 -= (LDA_4x*4);
        src_addr1 -= (LDA_4x*4);
        src_addr2 -= (LDA_4x*4);
        src_addr3 -= (LDA_4x*4);
        dst_addr0 += (32*15);
        dst_addr1 += (32*15);
    }

    if (tag_k_32x != k) {
        int k_rem = k - tag_k_32x;
        int idx_length = (k_rem + 1 - 16) >> 1;
        unsigned int tail_mask = (((unsigned int)0xffffffff) >> (32-k_rem));
        bfloat16 * dst_addr_tmp = dst_addr0;

        for (int j = 0; j < 4; j++) {
            int array_idx = j*4;
            // Load and preprocess 4 rows
            array512[array_idx+0] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
            array512[array_idx+1] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
            array512[array_idx+2] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
            array512[array_idx+3] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
            array512_0 = _mm512_unpacklo_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_1 = _mm512_unpackhi_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_2 = _mm512_unpacklo_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512_3 = _mm512_unpackhi_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512[array_idx+0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[array_idx+1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[array_idx+2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[array_idx+3] = _mm512_unpackhi_epi64(array512_1, array512_3);
            src_addr0 += LDA_4x;
            src_addr1 += LDA_4x;
            src_addr2 += LDA_4x;
            src_addr3 += LDA_4x;
        }

        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_lo_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_lo_idx, array512[j+12]);
            array512_2 = _mm512_permutex2var_epi64(array512[j+0], permute_hi_idx, array512[j+4]);
            array512_3 = _mm512_permutex2var_epi64(array512[j+8], permute_hi_idx, array512[j+12]);
            array512[j+0]  = array512_0;  // 1st 8 pairs of col 0/1|2/3|4/5|6/7,   and 1st 8 pairs of col 16/17|18/19|20/21|22/23
            array512[j+4]  = array512_1;  // 2nd 8 pairs of col 0/1|2/3|4/5|6/7,   and 2nd 8 pairs of col 16/17|18/19|20/21|22/23
            array512[j+8]  = array512_2;  // 1st 8 pairs of col 8/9|10/11|12/13|14/15,   and 1st 8 pairs of col 24/25|26/27|28/29|30/31
            array512[j+12] = array512_3;  // 2nd 8 pairs of col 8/9|10/11|12/13|14/15,   and 2nd 8 pairs of col 24/25|26/27|28/29|30/31
        }

        for (int j = 0; j < 4; j++) {
            // Compose and store the 0/1 cols
            array512_0 = _mm512_inserti64x4(array512[j], _mm512_castsi512_si256(array512[j+4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;
        }

        for (int j = 8; j < 12; j++) {
            array512_0 = _mm512_inserti64x4(array512[j], _mm512_castsi512_si256(array512[j+4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;
        }

        // Compose and store 16 ~ k_rem cols
        if (idx_length > 4) {
            for (int idx_k = 0; idx_k < 4; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 64;
            }

            for (int idx_k = 4; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+8], _mm512_extracti64x4_epi64(array512[idx_k+4], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 64;
            }
        } else {
            for (int idx_k = 0; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 64;
            }
        }

        dst_addr0 = dst_addr_tmp + 32;

        for (int j = 0; j < m_rem; j++) {
            array512[j] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+j*lda+tag_k_32x);
        }
        for (int j = m_rem; j < 16; j++) {
            array512[j] = _mm512_setzero_si512();
        }

        for (int j = 0; j < 4; j++) {
            int array_idx = j*4;
            array512_0 = _mm512_unpacklo_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_1 = _mm512_unpackhi_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_2 = _mm512_unpacklo_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512_3 = _mm512_unpackhi_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512[array_idx+0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[array_idx+1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[array_idx+2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[array_idx+3] = _mm512_unpackhi_epi64(array512_1, array512_3);
        }

        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_lo_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_lo_idx, array512[j+12]);
            array512_2 = _mm512_permutex2var_epi64(array512[j+0], permute_hi_idx, array512[j+4]);
            array512_3 = _mm512_permutex2var_epi64(array512[j+8], permute_hi_idx, array512[j+12]);
            array512[j+0]  = array512_0;  // 1st 8 pairs of col 0/1|2/3|4/5|6/7,   and 1st 8 pairs of col 16/17|18/19|20/21|22/23
            array512[j+4]  = array512_1;  // 2nd 8 pairs of col 0/1|2/3|4/5|6/7,   and 2nd 8 pairs of col 16/17|18/19|20/21|22/23
            array512[j+8]  = array512_2;  // 1st 8 pairs of col 8/9|10/11|12/13|14/15,   and 1st 8 pairs of col 24/25|26/27|28/29|30/31
            array512[j+12] = array512_3;  // 2nd 8 pairs of col 8/9|10/11|12/13|14/15,   and 2nd 8 pairs of col 24/25|26/27|28/29|30/31
        }

        for (int j = 0; j < 4; j++) {
            // Compose and store the 0/1 cols
            array512_0 = _mm512_inserti64x4(array512[j], _mm512_castsi512_si256(array512[j+4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;
        }

        for (int j = 8; j < 12; j++) {
            array512_0 = _mm512_inserti64x4(array512[j], _mm512_castsi512_si256(array512[j+4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 64;
        }

        // Compose and store 16 ~ k_rem cols
        if (idx_length > 4) {
            for (int idx_k = 0; idx_k < 4; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 64;
            }

            for (int idx_k = 4; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+8], _mm512_extracti64x4_epi64(array512[idx_k+4], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 64;
            }
        } else {
            for (int idx_k = 0; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 64;
            }
        }
    }
}

// K=Any number but will be processed based on 32, M=16
void COL_MAJOR_ITCOPY_KERNEL_Kx16(BLASLONG k, bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    bfloat16 * src_addr0, * src_addr1, * src_addr2, * src_addr3;
    bfloat16 * dst_addr0, * dst_addr1;

    BLASLONG tag_k_32x = k & (~31);

    BLASLONG LDA_4x  = lda*4;
    BLASLONG LDA_8x  = lda*8;
    BLASLONG LDA_12x = lda*12;

    src_addr0 = A;
    src_addr1 = A + lda;
    src_addr2 = A + lda*2;
    src_addr3 = A + lda*3;
    dst_addr0 = block_A;
    dst_addr1 = block_A + 32*8;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512_way0_0, array512_way0_1, array512_way0_2, array512_way0_3;
    __m512i array512_way1_0, array512_way1_1, array512_way1_2, array512_way1_3;
    __m512i array512_way2_0, array512_way2_1, array512_way2_2, array512_way2_3;
    __m512i array512_way3_0, array512_way3_1, array512_way3_2, array512_way3_3;

    __m512i M512_EPI64_2   = _mm512_set1_epi64(2);
    __m512i permute_lo_idx = _mm512_set_epi64(13, 12, 5, 4, 9, 8, 1, 0);
    __m512i permute_hi_idx = _mm512_add_epi64(permute_lo_idx, M512_EPI64_2);

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        // Load and preprocess 1st 4 rows
        array512_way0_0 = _mm512_loadu_si512(src_addr0+idx_k);
        array512_way0_1 = _mm512_loadu_si512(src_addr1+idx_k);
        array512_way0_2 = _mm512_loadu_si512(src_addr2+idx_k);
        array512_way0_3 = _mm512_loadu_si512(src_addr3+idx_k);
        array512_0 = _mm512_unpacklo_epi32(array512_way0_0, array512_way0_1);
        array512_1 = _mm512_unpackhi_epi32(array512_way0_0, array512_way0_1);
        array512_2 = _mm512_unpacklo_epi32(array512_way0_2, array512_way0_3);
        array512_3 = _mm512_unpackhi_epi32(array512_way0_2, array512_way0_3);
        array512_way0_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512_way0_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512_way0_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512_way0_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

        // Load and preprocess 2nd 4 rows
        array512_way1_0 = _mm512_loadu_si512(src_addr0+LDA_4x+idx_k);
        array512_way1_1 = _mm512_loadu_si512(src_addr1+LDA_4x+idx_k);
        array512_way1_2 = _mm512_loadu_si512(src_addr2+LDA_4x+idx_k);
        array512_way1_3 = _mm512_loadu_si512(src_addr3+LDA_4x+idx_k);
        array512_0 = _mm512_unpacklo_epi32(array512_way1_0, array512_way1_1);
        array512_1 = _mm512_unpackhi_epi32(array512_way1_0, array512_way1_1);
        array512_2 = _mm512_unpacklo_epi32(array512_way1_2, array512_way1_3);
        array512_3 = _mm512_unpackhi_epi32(array512_way1_2, array512_way1_3);
        array512_way1_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512_way1_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512_way1_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512_way1_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

        // Load and preprocess 3rd 4 rows
        array512_way2_0 = _mm512_loadu_si512(src_addr0+LDA_8x+idx_k);
        array512_way2_1 = _mm512_loadu_si512(src_addr1+LDA_8x+idx_k);
        array512_way2_2 = _mm512_loadu_si512(src_addr2+LDA_8x+idx_k);
        array512_way2_3 = _mm512_loadu_si512(src_addr3+LDA_8x+idx_k);
        array512_0 = _mm512_unpacklo_epi32(array512_way2_0, array512_way2_1);
        array512_1 = _mm512_unpackhi_epi32(array512_way2_0, array512_way2_1);
        array512_2 = _mm512_unpacklo_epi32(array512_way2_2, array512_way2_3);
        array512_3 = _mm512_unpackhi_epi32(array512_way2_2, array512_way2_3);
        array512_way2_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512_way2_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512_way2_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512_way2_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

        // Load and preprocess 4th 4 rows
        array512_way3_0 = _mm512_loadu_si512(src_addr0+LDA_12x+idx_k);
        array512_way3_1 = _mm512_loadu_si512(src_addr1+LDA_12x+idx_k);
        array512_way3_2 = _mm512_loadu_si512(src_addr2+LDA_12x+idx_k);
        array512_way3_3 = _mm512_loadu_si512(src_addr3+LDA_12x+idx_k);
        array512_0 = _mm512_unpacklo_epi32(array512_way3_0, array512_way3_1);
        array512_1 = _mm512_unpackhi_epi32(array512_way3_0, array512_way3_1);
        array512_2 = _mm512_unpacklo_epi32(array512_way3_2, array512_way3_3);
        array512_3 = _mm512_unpackhi_epi32(array512_way3_2, array512_way3_3);
        array512_way3_0 = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512_way3_1 = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512_way3_2 = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512_way3_3 = _mm512_unpackhi_epi64(array512_1, array512_3);

        // Compose and store the 0/1 and 16/17 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_lo_idx, array512_way1_0);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_0, permute_lo_idx, array512_way3_0);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 2/3 and 18/19 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_1, permute_lo_idx, array512_way1_1);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_1, permute_lo_idx, array512_way3_1);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 4/5 and 20/21 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_2, permute_lo_idx, array512_way1_2);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_2, permute_lo_idx, array512_way3_2);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 6/7 and 22/23 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_3, permute_lo_idx, array512_way1_3);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_3, permute_lo_idx, array512_way3_3);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 8/9 and 24/25 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_hi_idx, array512_way1_0);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_0, permute_hi_idx, array512_way3_0);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 10/11 and 26/27 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_1, permute_hi_idx, array512_way1_1);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_1, permute_hi_idx, array512_way3_1);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 12/13 and 28/29 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_2, permute_hi_idx, array512_way1_2);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_2, permute_hi_idx, array512_way3_2);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32;
        dst_addr1 += 32;

        // Compose and store the 14/15 and 30/31 cols
        array512_0 = _mm512_permutex2var_epi64(array512_way0_3, permute_hi_idx, array512_way1_3);
        array512_1 = _mm512_permutex2var_epi64(array512_way2_3, permute_hi_idx, array512_way3_3);
        array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
        array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
        _mm512_storeu_si512(dst_addr0, array512_2);
        _mm512_storeu_si512(dst_addr1, array512_3);
        dst_addr0 += 32*9;
        dst_addr1 += 32*9;
    }

    if (tag_k_32x != k) {
        int k_rem = k - tag_k_32x;
        unsigned int tail_mask = (((unsigned int)0xffffffff) >> (32-k_rem));
        __m512i array512[16];

        // Load and preprocess 1st 4 rows
        array512[0] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
        array512[1] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
        array512[2] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
        array512[3] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
        array512_0 = _mm512_unpacklo_epi32(array512[0], array512[1]);
        array512_1 = _mm512_unpackhi_epi32(array512[0], array512[1]);
        array512_2 = _mm512_unpacklo_epi32(array512[2], array512[3]);
        array512_3 = _mm512_unpackhi_epi32(array512[2], array512[3]);
        array512[0] = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512[1] = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512[2] = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512[3] = _mm512_unpackhi_epi64(array512_1, array512_3);
        src_addr0 += LDA_4x;
        src_addr1 += LDA_4x;
        src_addr2 += LDA_4x;
        src_addr3 += LDA_4x;

        // Load and preprocess 2nd 4 rows
        array512[4] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
        array512[5] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
        array512[6] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
        array512[7] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
        array512_0 = _mm512_unpacklo_epi32(array512[4], array512[5]);
        array512_1 = _mm512_unpackhi_epi32(array512[4], array512[5]);
        array512_2 = _mm512_unpacklo_epi32(array512[6], array512[7]);
        array512_3 = _mm512_unpackhi_epi32(array512[6], array512[7]);
        array512[4] = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512[5] = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512[6] = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512[7] = _mm512_unpackhi_epi64(array512_1, array512_3);
        src_addr0 += LDA_4x;
        src_addr1 += LDA_4x;
        src_addr2 += LDA_4x;
        src_addr3 += LDA_4x;

        // Load and preprocess 3rd 4 rows
        array512[8]  = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
        array512[9]  = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
        array512[10] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
        array512[11] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
        array512_0 = _mm512_unpacklo_epi32(array512[8],  array512[9]);
        array512_1 = _mm512_unpackhi_epi32(array512[8],  array512[9]);
        array512_2 = _mm512_unpacklo_epi32(array512[10], array512[11]);
        array512_3 = _mm512_unpackhi_epi32(array512[10], array512[11]);
        array512[8]  = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512[9]  = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512[10] = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512[11] = _mm512_unpackhi_epi64(array512_1, array512_3);
        src_addr0 += LDA_4x;
        src_addr1 += LDA_4x;
        src_addr2 += LDA_4x;
        src_addr3 += LDA_4x;

        // Load and preprocess 4th 4 rows
        array512[12] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
        array512[13] = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
        array512[14] = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
        array512[15] = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);
        array512_0 = _mm512_unpacklo_epi32(array512[12], array512[13]);
        array512_1 = _mm512_unpackhi_epi32(array512[12], array512[13]);
        array512_2 = _mm512_unpacklo_epi32(array512[14], array512[15]);
        array512_3 = _mm512_unpackhi_epi32(array512[14], array512[15]);
        array512[12] = _mm512_unpacklo_epi64(array512_0, array512_2);
        array512[13] = _mm512_unpackhi_epi64(array512_0, array512_2);
        array512[14] = _mm512_unpacklo_epi64(array512_1, array512_3);
        array512[15] = _mm512_unpackhi_epi64(array512_1, array512_3);

        // array512_01_1617_0, array512_01_1617_1, array512_89_2425_0, array512_89_2425_1;
        // Half-compose of 0/1, 16/17, 8/9, 24/25 cols
        array512_0 = _mm512_permutex2var_epi64(array512[0], permute_lo_idx, array512[4]);
        array512_1 = _mm512_permutex2var_epi64(array512[8], permute_lo_idx, array512[12]);
        array512_2 = _mm512_permutex2var_epi64(array512[0], permute_hi_idx, array512[4]);
        array512_3 = _mm512_permutex2var_epi64(array512[8], permute_hi_idx, array512[12]);
        array512[0]  = array512_0;  // 1st 8 pairs of col 0/1,   and 1st 8 pairs of col 16/17
        array512[4]  = array512_1;  // 2nd 8 pairs of col 0/1,   and 2nd 8 pairs of col 16/17
        array512[8]  = array512_2;  // 1st 8 pairs of col 8/9,   and 1st 8 pairs of col 24/25
        array512[12] = array512_3;  // 2nd 8 pairs of col 8/9,   and 2nd 8 pairs of col 24/25

        // Half-compose of 2/3, 18/19, 10/11, 26/27 cols
        array512_0 = _mm512_permutex2var_epi64(array512[1], permute_lo_idx, array512[5]);
        array512_1 = _mm512_permutex2var_epi64(array512[9], permute_lo_idx, array512[13]);
        array512_2 = _mm512_permutex2var_epi64(array512[1], permute_hi_idx, array512[5]);
        array512_3 = _mm512_permutex2var_epi64(array512[9], permute_hi_idx, array512[13]);
        array512[1]  = array512_0;  // 1st 8 pairs of col 2/3,   and 1st 8 pairs of col 18/19
        array512[5]  = array512_1;  // 2nd 8 pairs of col 2/3,   and 2nd 8 pairs of col 18/19
        array512[9]  = array512_2;  // 1st 8 pairs of col 10/11, and 1st 8 pairs of col 26/27
        array512[13] = array512_3;  // 2nd 8 pairs of col 10/11, and 2nd 8 pairs of col 26/27

        // Half-compose of 4/5, 20/21, 12/13, 28/29 cols
        array512_0 = _mm512_permutex2var_epi64(array512[2],  permute_lo_idx, array512[6]);
        array512_1 = _mm512_permutex2var_epi64(array512[10], permute_lo_idx, array512[14]);
        array512_2 = _mm512_permutex2var_epi64(array512[2],  permute_hi_idx, array512[6]);
        array512_3 = _mm512_permutex2var_epi64(array512[10], permute_hi_idx, array512[14]);
        array512[2]  = array512_0;  // 1st 8 pairs of col 4/5,   and 1st 8 pairs of col 20/21
        array512[6]  = array512_1;  // 2nd 8 pairs of col 4/5,   and 2nd 8 pairs of col 20/21
        array512[10] = array512_2;  // 1st 8 pairs of col 12/13, and 1st 8 pairs of col 28/29
        array512[14] = array512_3;  // 2nd 8 pairs of col 12/13, and 2nd 8 pairs of col 28/29

        // Half-compose of 6/7, 22/23, 14/15, 30/31 cols
        array512_0 = _mm512_permutex2var_epi64(array512[3],  permute_lo_idx, array512[7]);
        array512_1 = _mm512_permutex2var_epi64(array512[11], permute_lo_idx, array512[15]);
        array512_2 = _mm512_permutex2var_epi64(array512[3],  permute_hi_idx, array512[7]);
        array512_3 = _mm512_permutex2var_epi64(array512[11], permute_hi_idx, array512[15]);
        array512[3]  = array512_0;  // 1st 8 pairs of col 6/7,   and 1st 8 pairs of col 22/23
        array512[7]  = array512_1;  // 2nd 8 pairs of col 6/7,   and 2nd 8 pairs of col 22/23
        array512[11] = array512_2;  // 1st 8 pairs of col 14/15, and 1st 8 pairs of col 30/31
        array512[15] = array512_3;  // 2nd 8 pairs of col 14/15, and 2nd 8 pairs of col 30/31

        // Compose and store the 0/1 cols
        array512_0 = _mm512_inserti64x4(array512[0], _mm512_castsi512_si256(array512[4]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 2/3 cols
        array512_0 = _mm512_inserti64x4(array512[1], _mm512_castsi512_si256(array512[5]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 4/5 cols
        array512_0 = _mm512_inserti64x4(array512[2], _mm512_castsi512_si256(array512[6]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 6/7 cols
        array512_0 = _mm512_inserti64x4(array512[3], _mm512_castsi512_si256(array512[7]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 8/9 cols
        array512_0 = _mm512_inserti64x4(array512[8], _mm512_castsi512_si256(array512[12]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 10/11 cols
        array512_0 = _mm512_inserti64x4(array512[9], _mm512_castsi512_si256(array512[13]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 12/13 cols
        array512_0 = _mm512_inserti64x4(array512[10], _mm512_castsi512_si256(array512[14]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store the 14/15 cols
        array512_0 = _mm512_inserti64x4(array512[11], _mm512_castsi512_si256(array512[15]), 0x1);
        _mm512_storeu_si512(dst_addr0, array512_0);
        dst_addr0 += 32;

        // Compose and store 16 ~ k_rem cols
        int idx_length = (k_rem + 1 - 16) >> 1;
        if (idx_length > 4) {
            for (int idx_k = 0; idx_k < 4; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 32;
            }

            for (int idx_k = 4; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+8], _mm512_extracti64x4_epi64(array512[idx_k+4], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 32;
            }
        } else {
            for (int idx_k = 0; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 32;
            }
        }
    }
}

// K=Any number but will be processed based on 32, M<=16
void COL_MAJOR_ITCOPY_KERNEL_Kx16m(BLASLONG m, BLASLONG k, bfloat16 * A, BLASLONG lda, bfloat16 * block_A)
{
    bfloat16 * src_addr0;
    bfloat16 * dst_addr0, * dst_addr1;

    BLASLONG tag_k_32x = k & (~31);

    src_addr0 = A;
    dst_addr0 = block_A;
    dst_addr1 = block_A + 32*8;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512[16];

    __m512i M512_EPI64_2   = _mm512_set1_epi64(2);
    __m512i permute_lo_idx = _mm512_set_epi64(13, 12, 5, 4, 9, 8, 1, 0);
    __m512i permute_hi_idx = _mm512_add_epi64(permute_lo_idx, M512_EPI64_2);

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        for (int j = 0; j < m; j++) {
            array512[j] = _mm512_loadu_si512(src_addr0+j*lda+idx_k);
        }
        for (int j = m; j < 16; j++) {
            array512[j] = _mm512_setzero_si512();
        }

        for (int j = 0; j < 4; j++) {
            int array_idx = j*4;
            array512_0 = _mm512_unpacklo_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_1 = _mm512_unpackhi_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_2 = _mm512_unpacklo_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512_3 = _mm512_unpackhi_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512[array_idx+0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[array_idx+1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[array_idx+2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[array_idx+3] = _mm512_unpackhi_epi64(array512_1, array512_3);
        }

        // Compose and store the 0/1, 2/3, 4/5, 6/7 and 16/17, 18/19, 20/21, 22/23 cols
        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_lo_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_lo_idx, array512[j+12]);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 32;
            dst_addr1 += 32;
        }

        // Compose and store the 8/9, 10/11, 12/13, 14/15 and 24/25, 26/27, 28/29, 30/31 cols
        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_hi_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_hi_idx, array512[j+12]);
            array512_2 = _mm512_inserti64x4(array512_0, _mm512_castsi512_si256(array512_1), 0x1);
            array512_3 = _mm512_inserti64x4(array512_1, _mm512_extracti64x4_epi64(array512_0, 0x1), 0x0);
            _mm512_storeu_si512(dst_addr0, array512_2);
            _mm512_storeu_si512(dst_addr1, array512_3);
            dst_addr0 += 32;
            dst_addr1 += 32;
        }

        dst_addr0 += 32*8;
        dst_addr1 += 32*8;
    }

    if (tag_k_32x != k) {
        int k_rem = k - tag_k_32x;
        unsigned int tail_mask = (((unsigned int)0xffffffff) >> (32-k_rem));

        for (int j = 0; j < m; j++) {
            array512[j] = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+j*lda+tag_k_32x);
        }
        for (int j = m; j < 16; j++) {
            array512[j] = _mm512_setzero_si512();
        }

        for (int j = 0; j < 4; j++) {
            int array_idx = j*4;
            array512_0 = _mm512_unpacklo_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_1 = _mm512_unpackhi_epi32(array512[array_idx+0], array512[array_idx+1]);
            array512_2 = _mm512_unpacklo_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512_3 = _mm512_unpackhi_epi32(array512[array_idx+2], array512[array_idx+3]);
            array512[array_idx+0] = _mm512_unpacklo_epi64(array512_0, array512_2);
            array512[array_idx+1] = _mm512_unpackhi_epi64(array512_0, array512_2);
            array512[array_idx+2] = _mm512_unpacklo_epi64(array512_1, array512_3);
            array512[array_idx+3] = _mm512_unpackhi_epi64(array512_1, array512_3);
        }

        for (int j = 0; j < 4; j++) {
            array512_0 = _mm512_permutex2var_epi64(array512[j+0], permute_lo_idx, array512[j+4]);
            array512_1 = _mm512_permutex2var_epi64(array512[j+8], permute_lo_idx, array512[j+12]);
            array512_2 = _mm512_permutex2var_epi64(array512[j+0], permute_hi_idx, array512[j+4]);
            array512_3 = _mm512_permutex2var_epi64(array512[j+8], permute_hi_idx, array512[j+12]);
            array512[j+0]  = array512_0;  // 1st 8 pairs of col 0/1|2/3|4/5|6/7,   and 1st 8 pairs of col 16/17|18/19|20/21|22/23
            array512[j+4]  = array512_1;  // 2nd 8 pairs of col 0/1|2/3|4/5|6/7,   and 2nd 8 pairs of col 16/17|18/19|20/21|22/23
            array512[j+8]  = array512_2;  // 1st 8 pairs of col 8/9|10/11|12/13|14/15,   and 1st 8 pairs of col 24/25|26/27|28/29|30/31
            array512[j+12] = array512_3;  // 2nd 8 pairs of col 8/9|10/11|12/13|14/15,   and 2nd 8 pairs of col 24/25|26/27|28/29|30/31
        }

        for (int j = 0; j < 4; j++) {
            // Compose and store the 0/1 cols
            array512_0 = _mm512_inserti64x4(array512[j], _mm512_castsi512_si256(array512[j+4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 32;
        }

        for (int j = 8; j < 12; j++) {
            array512_0 = _mm512_inserti64x4(array512[j], _mm512_castsi512_si256(array512[j+4]), 0x1);
            _mm512_storeu_si512(dst_addr0, array512_0);
            dst_addr0 += 32;
        }

        // Compose and store 16 ~ k_rem cols
        int idx_length = (k_rem + 1 - 16) >> 1;
        if (idx_length > 4) {
            for (int idx_k = 0; idx_k < 4; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 32;
            }

            for (int idx_k = 4; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+8], _mm512_extracti64x4_epi64(array512[idx_k+4], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 32;
            }
        } else {
            for (int idx_k = 0; idx_k < idx_length; idx_k++) {
                array512_0 = _mm512_inserti64x4(array512[idx_k+4], _mm512_extracti64x4_epi64(array512[idx_k], 0x1), 0x0);
                _mm512_storeu_si512(dst_addr0, array512_0);
                dst_addr0 += 32;
            }
        }
    }
}

// COL_MAJOR_ONCOPY_KERNEL_16x32 behaves exactly the same as COL_MAJOR_ITCOPY_KERNEL_Kx16
#define COL_MAJOR_ONCOPY_KERNEL_16x32 COL_MAJOR_ITCOPY_KERNEL_Kx16

void COL_MAJOR_ONCOPY_KERNEL_8x32(BLASLONG k, bfloat16 * B, BLASLONG ldb, bfloat16 * block_B)
{
    BLASLONG tag_k_32x = k & (~31);

    bfloat16 * src_addr0, * src_addr1, * src_addr2, * src_addr3, * src_addr4, * src_addr5, * src_addr6, * src_addr7;
    bfloat16 * dst_addr0;

    unsigned char blend_mask = (((unsigned char)0xcc));
    __m512i permute_idx = _mm512_set_epi64(13, 12, 7, 6, 9, 8, 3, 2);

    src_addr0 = B;
    src_addr1 = src_addr0 + 1*ldb;
    src_addr2 = src_addr0 + 2*ldb;
    src_addr3 = src_addr0 + 3*ldb;
    src_addr4 = src_addr0 + 4*ldb;
    src_addr5 = src_addr0 + 5*ldb;
    src_addr6 = src_addr0 + 6*ldb;
    src_addr7 = src_addr0 + 7*ldb;
    dst_addr0 = block_B;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512_way0_0, array512_way0_1, array512_way0_2, array512_way0_3;
    __m512i array512_way1_0, array512_way1_1, array512_way1_2, array512_way1_3;

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        array512_0 = _mm512_loadu_si512(src_addr0+idx_k);
        array512_1 = _mm512_loadu_si512(src_addr1+idx_k);
        array512_2 = _mm512_loadu_si512(src_addr2+idx_k);
        array512_3 = _mm512_loadu_si512(src_addr3+idx_k);

        array512_way0_0 = _mm512_unpacklo_epi32(array512_0, array512_1);
        array512_way0_1 = _mm512_unpackhi_epi32(array512_0, array512_1);
        array512_way0_2 = _mm512_unpacklo_epi32(array512_2, array512_3);
        array512_way0_3 = _mm512_unpackhi_epi32(array512_2, array512_3);

        array512_0 = _mm512_unpacklo_epi64(array512_way0_0, array512_way0_2);
        array512_1 = _mm512_unpackhi_epi64(array512_way0_0, array512_way0_2);
        array512_2 = _mm512_unpacklo_epi64(array512_way0_1, array512_way0_3);
        array512_3 = _mm512_unpackhi_epi64(array512_way0_1, array512_way0_3);

        array512_way0_0 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x88);
        array512_way0_2 = _mm512_shuffle_i32x4(array512_0, array512_1, 0xdd);
        array512_way0_1 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x88);
        array512_way0_3 = _mm512_shuffle_i32x4(array512_2, array512_3, 0xdd);

        array512_0 = _mm512_loadu_si512(src_addr4+idx_k);
        array512_1 = _mm512_loadu_si512(src_addr5+idx_k);
        array512_2 = _mm512_loadu_si512(src_addr6+idx_k);
        array512_3 = _mm512_loadu_si512(src_addr7+idx_k);

        array512_way1_0 = _mm512_unpacklo_epi32(array512_0, array512_1);
        array512_way1_1 = _mm512_unpackhi_epi32(array512_0, array512_1);
        array512_way1_2 = _mm512_unpacklo_epi32(array512_2, array512_3);
        array512_way1_3 = _mm512_unpackhi_epi32(array512_2, array512_3);

        array512_0 = _mm512_unpacklo_epi64(array512_way1_0, array512_way1_2);
        array512_1 = _mm512_unpackhi_epi64(array512_way1_0, array512_way1_2);
        array512_2 = _mm512_unpacklo_epi64(array512_way1_1, array512_way1_3);
        array512_3 = _mm512_unpackhi_epi64(array512_way1_1, array512_way1_3);

        array512_way1_0 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x22);
        array512_way1_2 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x77);
        array512_way1_1 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x22);
        array512_way1_3 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x77);

        array512_0 = _mm512_mask_blend_epi64(blend_mask, array512_way0_0, array512_way1_0);
        array512_1 = _mm512_mask_blend_epi64(blend_mask, array512_way0_1, array512_way1_1);
        array512_2 = _mm512_mask_blend_epi64(blend_mask, array512_way0_2, array512_way1_2);
        array512_3 = _mm512_mask_blend_epi64(blend_mask, array512_way0_3, array512_way1_3);
        _mm512_storeu_si512(dst_addr0,    array512_0);
        _mm512_storeu_si512(dst_addr0+32, array512_1);
        _mm512_storeu_si512(dst_addr0+64, array512_2);
        _mm512_storeu_si512(dst_addr0+96, array512_3);

        array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_idx, array512_way1_0);
        array512_1 = _mm512_permutex2var_epi64(array512_way0_1, permute_idx, array512_way1_1);
        array512_2 = _mm512_permutex2var_epi64(array512_way0_2, permute_idx, array512_way1_2);
        array512_3 = _mm512_permutex2var_epi64(array512_way0_3, permute_idx, array512_way1_3);
        _mm512_storeu_si512(dst_addr0+128, array512_0);
        _mm512_storeu_si512(dst_addr0+160, array512_1);
        _mm512_storeu_si512(dst_addr0+192, array512_2);
        _mm512_storeu_si512(dst_addr0+224, array512_3);

        dst_addr0 += 256;
    }

    if (tag_k_32x != k) {
        unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-(k-tag_k_32x)));
        __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);
        array512_0 = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
        array512_1 = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
        array512_2 = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
        array512_3 = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);

        array512_way0_0 = _mm512_unpacklo_epi32(array512_0, array512_1);
        array512_way0_1 = _mm512_unpackhi_epi32(array512_0, array512_1);
        array512_way0_2 = _mm512_unpacklo_epi32(array512_2, array512_3);
        array512_way0_3 = _mm512_unpackhi_epi32(array512_2, array512_3);

        array512_0 = _mm512_unpacklo_epi64(array512_way0_0, array512_way0_2);
        array512_1 = _mm512_unpackhi_epi64(array512_way0_0, array512_way0_2);
        array512_2 = _mm512_unpacklo_epi64(array512_way0_1, array512_way0_3);
        array512_3 = _mm512_unpackhi_epi64(array512_way0_1, array512_way0_3);

        array512_way0_0 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x88);
        array512_way0_2 = _mm512_shuffle_i32x4(array512_0, array512_1, 0xdd);
        array512_way0_1 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x88);
        array512_way0_3 = _mm512_shuffle_i32x4(array512_2, array512_3, 0xdd);

        array512_0 = _mm512_maskz_loadu_epi16(tail_mask, src_addr4+tag_k_32x);
        array512_1 = _mm512_maskz_loadu_epi16(tail_mask, src_addr5+tag_k_32x);
        array512_2 = _mm512_maskz_loadu_epi16(tail_mask, src_addr6+tag_k_32x);
        array512_3 = _mm512_maskz_loadu_epi16(tail_mask, src_addr7+tag_k_32x);

        array512_way1_0 = _mm512_unpacklo_epi32(array512_0, array512_1);
        array512_way1_1 = _mm512_unpackhi_epi32(array512_0, array512_1);
        array512_way1_2 = _mm512_unpacklo_epi32(array512_2, array512_3);
        array512_way1_3 = _mm512_unpackhi_epi32(array512_2, array512_3);

        array512_0 = _mm512_unpacklo_epi64(array512_way1_0, array512_way1_2);
        array512_1 = _mm512_unpackhi_epi64(array512_way1_0, array512_way1_2);
        array512_2 = _mm512_unpacklo_epi64(array512_way1_1, array512_way1_3);
        array512_3 = _mm512_unpackhi_epi64(array512_way1_1, array512_way1_3);

        array512_way1_0 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x22);
        array512_way1_2 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x77);
        array512_way1_1 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x22);
        array512_way1_3 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x77);


        array512_0 = _mm512_mask_blend_epi64(blend_mask, array512_way0_0, array512_way1_0);
        array512_1 = _mm512_mask_blend_epi64(blend_mask, array512_way0_1, array512_way1_1);
        array512_2 = _mm512_mask_blend_epi64(blend_mask, array512_way0_2, array512_way1_2);
        array512_3 = _mm512_mask_blend_epi64(blend_mask, array512_way0_3, array512_way1_3);
        _mm512_storeu_si512(dst_addr0,    array512_0);
        _mm512_storeu_si512(dst_addr0+32, array512_1);
        _mm512_storeu_si512(dst_addr0+64, array512_2);
        _mm512_storeu_si512(dst_addr0+96, array512_3);

        array512_0 = _mm512_permutex2var_epi64(array512_way0_0, permute_idx, array512_way1_0);
        array512_1 = _mm512_permutex2var_epi64(array512_way0_1, permute_idx, array512_way1_1);
        array512_2 = _mm512_permutex2var_epi64(array512_way0_2, permute_idx, array512_way1_2);
        array512_3 = _mm512_permutex2var_epi64(array512_way0_3, permute_idx, array512_way1_3);
        _mm512_storeu_si512(dst_addr0+128, array512_0);
        _mm512_storeu_si512(dst_addr0+160, array512_1);
        _mm512_storeu_si512(dst_addr0+192, array512_2);
        _mm512_storeu_si512(dst_addr0+224, array512_3);
    }
}

void COL_MAJOR_ONCOPY_KERNEL_4x32(BLASLONG k, bfloat16 * B, BLASLONG ldb, bfloat16 * block_B)
{
    BLASLONG tag_k_32x = k & (~31);

    bfloat16 * src_addr0, * src_addr1, * src_addr2, * src_addr3;
    bfloat16 * dst_addr0;

    src_addr0 = B;
    src_addr1 = src_addr0 + 1*ldb;
    src_addr2 = src_addr0 + 2*ldb;
    src_addr3 = src_addr0 + 3*ldb;
    dst_addr0 = block_B;

    __m512i array512_0, array512_1, array512_2, array512_3;
    __m512i array512_way0_0, array512_way0_1, array512_way0_2, array512_way0_3;

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        array512_0 = _mm512_loadu_si512(src_addr0+idx_k);
        array512_1 = _mm512_loadu_si512(src_addr1+idx_k);
        array512_2 = _mm512_loadu_si512(src_addr2+idx_k);
        array512_3 = _mm512_loadu_si512(src_addr3+idx_k);

        array512_way0_0 = _mm512_unpacklo_epi32(array512_0, array512_1);
        array512_way0_1 = _mm512_unpackhi_epi32(array512_0, array512_1);
        array512_way0_2 = _mm512_unpacklo_epi32(array512_2, array512_3);
        array512_way0_3 = _mm512_unpackhi_epi32(array512_2, array512_3);

        array512_0 = _mm512_unpacklo_epi64(array512_way0_0, array512_way0_2);
        array512_1 = _mm512_unpackhi_epi64(array512_way0_0, array512_way0_2);
        array512_2 = _mm512_unpacklo_epi64(array512_way0_1, array512_way0_3);
        array512_3 = _mm512_unpackhi_epi64(array512_way0_1, array512_way0_3);

        array512_way0_0 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x88);
        array512_way0_2 = _mm512_shuffle_i32x4(array512_0, array512_1, 0xdd);
        array512_way0_1 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x88);
        array512_way0_3 = _mm512_shuffle_i32x4(array512_2, array512_3, 0xdd);

        array512_0 = _mm512_shuffle_i32x4(array512_way0_0, array512_way0_1, 0x88);
        array512_1 = _mm512_shuffle_i32x4(array512_way0_2, array512_way0_3, 0x88);
        array512_2 = _mm512_shuffle_i32x4(array512_way0_0, array512_way0_1, 0xdd);
        array512_3 = _mm512_shuffle_i32x4(array512_way0_2, array512_way0_3, 0xdd);

        _mm512_storeu_si512(dst_addr0,    array512_0);
        _mm512_storeu_si512(dst_addr0+32, array512_1);
        _mm512_storeu_si512(dst_addr0+64, array512_2);
        _mm512_storeu_si512(dst_addr0+96, array512_3);

        dst_addr0 += 128;
    }

    if (tag_k_32x != k) {
        unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-(k-tag_k_32x)));
        __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);
        array512_0 = _mm512_maskz_loadu_epi16(tail_mask, src_addr0+tag_k_32x);
        array512_1 = _mm512_maskz_loadu_epi16(tail_mask, src_addr1+tag_k_32x);
        array512_2 = _mm512_maskz_loadu_epi16(tail_mask, src_addr2+tag_k_32x);
        array512_3 = _mm512_maskz_loadu_epi16(tail_mask, src_addr3+tag_k_32x);

        array512_way0_0 = _mm512_unpacklo_epi32(array512_0, array512_1);
        array512_way0_1 = _mm512_unpackhi_epi32(array512_0, array512_1);
        array512_way0_2 = _mm512_unpacklo_epi32(array512_2, array512_3);
        array512_way0_3 = _mm512_unpackhi_epi32(array512_2, array512_3);

        array512_0 = _mm512_unpacklo_epi64(array512_way0_0, array512_way0_2);
        array512_1 = _mm512_unpackhi_epi64(array512_way0_0, array512_way0_2);
        array512_2 = _mm512_unpacklo_epi64(array512_way0_1, array512_way0_3);
        array512_3 = _mm512_unpackhi_epi64(array512_way0_1, array512_way0_3);

        array512_way0_0 = _mm512_shuffle_i32x4(array512_0, array512_1, 0x88);
        array512_way0_2 = _mm512_shuffle_i32x4(array512_0, array512_1, 0xdd);
        array512_way0_1 = _mm512_shuffle_i32x4(array512_2, array512_3, 0x88);
        array512_way0_3 = _mm512_shuffle_i32x4(array512_2, array512_3, 0xdd);

        array512_0 = _mm512_shuffle_i32x4(array512_way0_0, array512_way0_1, 0x88);
        array512_1 = _mm512_shuffle_i32x4(array512_way0_2, array512_way0_3, 0x88);
        array512_2 = _mm512_shuffle_i32x4(array512_way0_0, array512_way0_1, 0xdd);
        array512_3 = _mm512_shuffle_i32x4(array512_way0_2, array512_way0_3, 0xdd);

        _mm512_storeu_si512(dst_addr0,    array512_0);
        _mm512_storeu_si512(dst_addr0+32, array512_1);
        _mm512_storeu_si512(dst_addr0+64, array512_2);
        _mm512_storeu_si512(dst_addr0+96, array512_3);
    }
}

void COL_MAJOR_ONCOPY_KERNEL_Nx32(BLASLONG n, BLASLONG k, bfloat16 * B, BLASLONG ldb, bfloat16 * block_B)
{
    BLASLONG tag_k_32x = k & (~31);
    BLASLONG tag_n_2x  = n & (~1);

    bfloat16 * src_addr0;
    bfloat16 * dst_addr0;

    BLASLONG LDB_2x = 2*ldb;

    src_addr0 = B;
    dst_addr0 = block_B;

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        src_addr0 = B;
        for (BLASLONG idx_n = 0; idx_n < tag_n_2x; idx_n += 2) {
            _mm512_storeu_si512(dst_addr0,      _mm512_loadu_si512(src_addr0 + idx_k));
            _mm512_storeu_si512(dst_addr0 + 32, _mm512_loadu_si512(src_addr0 + ldb + idx_k));
            src_addr0 += LDB_2x;
            dst_addr0 += 64;
        }
        
        if (tag_n_2x != n) {
            _mm512_storeu_si512(dst_addr0,  _mm512_loadu_si512(src_addr0 + idx_k));
            dst_addr0 += 32;
        }
    }

    if (tag_k_32x != k) {
        unsigned int tail_mask_value = (((unsigned int)0xffffffff) >> (32-(k-tag_k_32x)));
        __mmask32 tail_mask = *((__mmask32*) &tail_mask_value);
        src_addr0 = B;
        for (BLASLONG idx_n = 0; idx_n < tag_n_2x; idx_n += 2) {
            _mm512_storeu_si512(dst_addr0,      _mm512_maskz_loadu_epi16(tail_mask, src_addr0 + tag_k_32x));
            _mm512_storeu_si512(dst_addr0 + 32, _mm512_maskz_loadu_epi16(tail_mask, src_addr0 + ldb + tag_k_32x));
            src_addr0 += LDB_2x;
            dst_addr0 += 64;
        }
        
        if (tag_n_2x != n) {
            _mm512_storeu_si512(dst_addr0,  _mm512_maskz_loadu_epi16(tail_mask, src_addr0 + tag_k_32x));
        }
    }
}

void COL_MAJOR_OTCOPY_KERNEL_Kx8(BLASLONG k, bfloat16 * B, BLASLONG ldb, bfloat16 * block_B)
{
    BLASLONG tag_k_2x = k & (~1);
    unsigned char tail_mask_value = (unsigned char) 0xff;
    __mmask8 tail_mask = *((__mmask8*) &tail_mask_value);

    __m128i array128_0, array128_1, array128_2, array128_3;

    BLASLONG idx_src_base0, idx_src_base1;
    BLASLONG idx_target_base0, idx_target_base1;

    BLASLONG LDA_2x = 2*ldb;
    BLASLONG BF16_BLOCK_T_M_2x = 2*8;
    idx_src_base0 = 0;
    idx_src_base1 = ldb;
    idx_target_base0 = 0;
    idx_target_base1 = 8;
    for (BLASLONG idx_k = 0; idx_k < tag_k_2x; idx_k += 2) {
        array128_0 = _mm_maskz_loadu_epi16(tail_mask, &B[idx_src_base0]);
        array128_1 = _mm_maskz_loadu_epi16(tail_mask, &B[idx_src_base1]);
        array128_2 = _mm_unpacklo_epi16(array128_0, array128_1);
        array128_3 = _mm_unpackhi_epi16(array128_0, array128_1);
        _mm_storeu_epi32(&block_B[idx_target_base0], array128_2);
        _mm_storeu_epi32(&block_B[idx_target_base1], array128_3);

        idx_src_base0 += LDA_2x;
        idx_src_base1 += LDA_2x;
        idx_target_base0 += BF16_BLOCK_T_M_2x;
        idx_target_base1 += BF16_BLOCK_T_M_2x;
    }

    if (tag_k_2x != k) {
        __m128i ZERO128 = _mm_setzero_si128();
        array128_0 = _mm_maskz_loadu_epi16(tail_mask, &B[idx_src_base0]);
        array128_2 = _mm_unpacklo_epi16(array128_0, ZERO128);
        array128_3 = _mm_unpackhi_epi16(array128_0, ZERO128);
        _mm_storeu_epi32(&block_B[idx_target_base0], array128_2);
        _mm_storeu_epi32(&block_B[idx_target_base1], array128_3);
   }
}

void COL_MAJOR_OTCOPY_KERNEL_Kx8m(BLASLONG k, BLASLONG n, bfloat16 * B, BLASLONG ldb, bfloat16 * block_B)
{
    BLASLONG tag_k_2x = k & (~1);
    unsigned char tail_mask = (((unsigned char)0xff) >> (8-n));

    __m128i array128_0, array128_1, array128_2, array128_3;

    BLASLONG idx_src_base0, idx_src_base1;
    BLASLONG idx_target_base0, idx_target_base1;

    BLASLONG LDA_2x = 2*ldb;
    BLASLONG BF16_BLOCK_T_M_2x = 2*8;
    idx_src_base0 = 0;
    idx_src_base1 = ldb;
    idx_target_base0 = 0;
    idx_target_base1 = 8;
    for (BLASLONG idx_k = 0; idx_k < tag_k_2x; idx_k += 2) {
        array128_0 = _mm_maskz_loadu_epi16(tail_mask, &B[idx_src_base0]);
        array128_1 = _mm_maskz_loadu_epi16(tail_mask, &B[idx_src_base1]);
        array128_2 = _mm_unpacklo_epi16(array128_0, array128_1);
        array128_3 = _mm_unpackhi_epi16(array128_0, array128_1);
        _mm_storeu_epi32(&block_B[idx_target_base0], array128_2);
        _mm_storeu_epi32(&block_B[idx_target_base1], array128_3);

        idx_src_base0 += LDA_2x;
        idx_src_base1 += LDA_2x;
        idx_target_base0 += BF16_BLOCK_T_M_2x;
        idx_target_base1 += BF16_BLOCK_T_M_2x;
    }

    if (tag_k_2x != k) {
        __m128i ZERO128 = _mm_setzero_si128();
        array128_0 = _mm_maskz_loadu_epi16(tail_mask, &B[idx_src_base0]);
        array128_2 = _mm_unpacklo_epi16(array128_0, ZERO128);
        array128_3 = _mm_unpackhi_epi16(array128_0, ZERO128);
        _mm_storeu_epi32(&block_B[idx_target_base0], array128_2);
        _mm_storeu_epi32(&block_B[idx_target_base1], array128_3);
   }
}

// Scale matrix C when beta is not ZERO or ONE
void sbgemm_scal_operation(BLASLONG M, BLASLONG N, float beta, float *C, BLASLONG ldc)
{
    float * C_addr0 = C;
    float * C_addr1 = C + ldc;
    float * C_addr2 = C + ldc*2;
    float * C_addr3 = C + ldc*3;

    BLASLONG LDC4x = ldc*4;

    __m512 array_512_0, array_512_1, array_512_2, array_512_3;
    __m512 BETAVECTOR  = _mm512_set1_ps(beta);

    BLASLONG tag_n_Nx = N & (~3);
    BLASLONG tag_n_Mx = M & (~15);
    unsigned short tail_mask = (((unsigned short)0xffff) >> (16-M+tag_n_Mx));
    for (BLASLONG idx_n = 0; idx_n < tag_n_Nx; idx_n += 4) {
        for (BLASLONG idx_m = 0; idx_m < tag_n_Mx; idx_m += 16) {
            array_512_0 = _mm512_loadu_ps(C_addr0 + idx_m);
            array_512_1 = _mm512_loadu_ps(C_addr1 + idx_m);
            array_512_2 = _mm512_loadu_ps(C_addr2 + idx_m);
            array_512_3 = _mm512_loadu_ps(C_addr3 + idx_m);

            array_512_0 = _mm512_mul_ps(BETAVECTOR, array_512_0);
            array_512_1 = _mm512_mul_ps(BETAVECTOR, array_512_1);
            array_512_2 = _mm512_mul_ps(BETAVECTOR, array_512_2);
            array_512_3 = _mm512_mul_ps(BETAVECTOR, array_512_3);

            _mm512_storeu_ps(C_addr0 + idx_m, array_512_0);
            _mm512_storeu_ps(C_addr1 + idx_m, array_512_1);
            _mm512_storeu_ps(C_addr2 + idx_m, array_512_2);
            _mm512_storeu_ps(C_addr3 + idx_m, array_512_3);
        }

        if (tag_n_Mx != M) {
            array_512_0 = _mm512_maskz_loadu_ps(tail_mask, C_addr0 + tag_n_Mx);
            array_512_1 = _mm512_maskz_loadu_ps(tail_mask, C_addr1 + tag_n_Mx);
            array_512_2 = _mm512_maskz_loadu_ps(tail_mask, C_addr2 + tag_n_Mx);
            array_512_3 = _mm512_maskz_loadu_ps(tail_mask, C_addr3 + tag_n_Mx);

            array_512_0 = _mm512_mul_ps(BETAVECTOR, array_512_0);
            array_512_1 = _mm512_mul_ps(BETAVECTOR, array_512_1);
            array_512_2 = _mm512_mul_ps(BETAVECTOR, array_512_2);
            array_512_3 = _mm512_mul_ps(BETAVECTOR, array_512_3);

            _mm512_mask_storeu_ps(C_addr0 + tag_n_Mx, tail_mask, array_512_0);
            _mm512_mask_storeu_ps(C_addr1 + tag_n_Mx, tail_mask, array_512_1);
            _mm512_mask_storeu_ps(C_addr2 + tag_n_Mx, tail_mask, array_512_2);
            _mm512_mask_storeu_ps(C_addr3 + tag_n_Mx, tail_mask, array_512_3);
        }

        C_addr0 += LDC4x;
        C_addr1 += LDC4x;
        C_addr2 += LDC4x;
        C_addr3 += LDC4x;
    }

    if (tag_n_Nx != N) {
        for (BLASLONG idx_n = tag_n_Nx; idx_n < N; idx_n++) {
            for (BLASLONG idx_m = 0; idx_m < tag_n_Mx; idx_m += 16) {
                array_512_0 = _mm512_loadu_ps(C_addr0 + idx_m);
                array_512_0 = _mm512_mul_ps(BETAVECTOR, array_512_0);
                _mm512_storeu_ps(C_addr0 + idx_m, array_512_0);
            }

            if (tag_n_Mx != M) {
                array_512_0 = _mm512_maskz_loadu_ps(tail_mask, C_addr0 + tag_n_Mx);
                array_512_0 = _mm512_mul_ps(BETAVECTOR, array_512_0);
                _mm512_mask_storeu_ps(C_addr0 + tag_n_Mx, tail_mask, array_512_0);
            }
            C_addr0 += ldc;
        }
    }
}

// Zero C matrix when Beta is 0
void sbgemm_zero_operation(BLASLONG M, BLASLONG N, float *C, BLASLONG ldc)
{
    float * C_addr0 = C;
    float * C_addr1 = C + ldc;
    float * C_addr2 = C + ldc*2;
    float * C_addr3 = C + ldc*3;

    BLASLONG LDC4x = ldc*4;

    __m512  ZEROVECTOR  = _mm512_setzero_ps();

    BLASLONG tag_n_Nx = N & (~3);
    BLASLONG tag_n_Mx = M & (~15);
    unsigned short tail_mask = (((unsigned short)0xffff) >> (16-M+tag_n_Mx));
    for (BLASLONG idx_n = 0; idx_n < tag_n_Nx; idx_n += 4) {
        for (BLASLONG idx_m = 0; idx_m < tag_n_Mx; idx_m += 16) {
            _mm512_storeu_ps(C_addr0 + idx_m, ZEROVECTOR);
            _mm512_storeu_ps(C_addr1 + idx_m, ZEROVECTOR);
            _mm512_storeu_ps(C_addr2 + idx_m, ZEROVECTOR);
            _mm512_storeu_ps(C_addr3 + idx_m, ZEROVECTOR);
        }

        if (tag_n_Mx != M) {
            _mm512_mask_storeu_ps(C_addr0 + tag_n_Mx, tail_mask, ZEROVECTOR);
            _mm512_mask_storeu_ps(C_addr1 + tag_n_Mx, tail_mask, ZEROVECTOR);
            _mm512_mask_storeu_ps(C_addr2 + tag_n_Mx, tail_mask, ZEROVECTOR);
            _mm512_mask_storeu_ps(C_addr3 + tag_n_Mx, tail_mask, ZEROVECTOR);
        }

        C_addr0 += LDC4x;
        C_addr1 += LDC4x;
        C_addr2 += LDC4x;
        C_addr3 += LDC4x;
    }

    if (tag_n_Nx != N) {
        for (BLASLONG idx_n = tag_n_Nx; idx_n < N; idx_n++) {
            for (BLASLONG idx_m = 0; idx_m < tag_n_Mx; idx_m += 16) {
                _mm512_storeu_ps(C_addr0 + idx_m, ZEROVECTOR);
            }

            if (tag_n_Mx != M) {
                _mm512_mask_storeu_ps(C_addr0 + tag_n_Mx, tail_mask, ZEROVECTOR);
            }
            C_addr0 += ldc;
        }
    }
}

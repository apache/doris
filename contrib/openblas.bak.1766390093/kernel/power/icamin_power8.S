/*	.file	"icamin.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl icamin_k
	.type	icamin_k, @function
*/
#define ASSEMBLER
#include "common.h"

	PROLOGUE
#if _CALL_ELF ==2
icamin_k:
#endif
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
#if _CALL_ELF ==2
	.localentry	icamin_k,.-icamin_k
#endif
	mr. 9,3
	ble 0,.L25
	cmpdi 7,5,0
	li 3,0
	blelr 7
	lfs 11,0(4)
	lfs 0,4(4)
	cmpdi 7,5,1
	fabs 11,11
	fabs 0,0
	fadds 11,11,0
	beq 7,.L54
	cmpdi 7,9,1
	beq 7,.L29
	addi 9,9,-1
	sldi 5,5,3
	mtctr 9
	add 4,4,5
	li 3,0
	li 9,1
	.p2align 4,,15
.L24:
	lfs 0,4(4)
	lfs 12,0(4)
	add 4,4,5
	fabs 0,0
	fabs 12,12
	fadds 0,0,12
	fcmpu 7,0,11
	bnl 7,.L23
	fmr 11,0
	mr 3,9
.L23:
	addi 9,9,1
	bdnz .L24
.L52:
	addi 3,3,1
	blr
	.p2align 4,,15
.L25:
	li 3,0
	blr
	.p2align 4,,15
.L54:
	rldicr. 8,9,0,58
	bne 0,.L55
	addi 7,8,1
	li 10,0
	cmpd 7,7,9
	sldi 10,10,2
	add 4,4,10
	subf 10,8,9
	mtctr 10
	li 3,0
	bgt 7,.L43
	li 10,-1
	rldicr 10,10,0,0
	cmpd 7,9,10
	beq 7,.L43
	.p2align 4,,15
.L44:
	lfs 0,0(4)
	lfs 12,4(4)
	addi 4,4,8
	fabs 0,0
	fabs 12,12
	fadds 0,0,12
	fcmpu 7,11,0
	bng 7,.L46
	fmr 11,0
	mr 3,8
.L46:
	addi 8,8,1
	bdnz .L44
	b .L52
	.p2align 4,,15
.L55:
	li 0,-128
	std 31,-8(1)
	addis 5,2,.LC2@toc@ha
	xscvdpspn 11,11
	vspltisw 19,0
	addis 6,2,.LC3@toc@ha
	addi 5,5,.LC2@toc@l
	stvx 25,1,0
	li 0,-112
	addi 6,6,.LC3@toc@l
	xxlor 50,51,51
	addis 7,2,.LC4@toc@ha
	lxvd2x 44,0,5
	addis 10,2,.LC5@toc@ha
	stvx 26,1,0
	li 0,-96
	addi 7,7,.LC4@toc@l
	lxvd2x 45,0,6
	addis 5,2,.LC6@toc@ha
	addis 6,2,.LC7@toc@ha
	stvx 27,1,0
	li 0,-80
	addi 10,10,.LC5@toc@l
	xxspltw 5,11,0
	addi 6,6,.LC7@toc@l
	addi 5,5,.LC6@toc@l
	stvx 28,1,0
	li 0,-64
	lxvd2x 47,0,10
	xxpermdi 44,44,44,2
	mr 10,4
	lxvd2x 49,0,6
	lxvd2x 48,0,5
	xxpermdi 45,45,45,2
	li 6,0
	stvx 29,1,0
	li 0,-48
	xxlnand 44,44,44
	xxlnand 45,45,45
	stvx 30,1,0
	lxvd2x 62,0,7
	addis 7,2,.LC8@toc@ha
	li 0,-32
	addi 7,7,.LC8@toc@l
	xxpermdi 47,47,47,2
	stvx 31,1,0
	vspltisw 31,8
	xxpermdi 48,48,48,2
	lxvd2x 46,0,7
	vadduwm 31,31,31
	xxpermdi 49,49,49,2
	xxpermdi 62,62,62,2
	.p2align 4,,15
.L5:
	addi 3,10,16
	addi 5,10,32
	lxvd2x 34,0,10
	addi 7,10,64
	addi 31,10,48
	addi 12,10,80
	addi 11,10,96
	lxvd2x 36,0,3
	lxvd2x 37,0,5
	addi 3,10,112
	addi 5,10,128
	lxvd2x 38,0,7
	lxvd2x 6,0,31
	addi 7,10,160
	addi 31,10,144
	lxvd2x 33,0,12
	lxvd2x 39,0,11
	addi 12,10,176
	addi 11,10,192
	lxvd2x 7,0,3
	lxvd2x 40,0,5
	xxpermdi 34,34,34,2
	addi 3,10,208
	addi 5,10,224
	lxvd2x 41,0,7
	lxvd2x 8,0,31
	addi 7,10,240
	lxvd2x 9,0,12
	lxvd2x 42,0,11
	xxpermdi 37,37,37,2
	xxpermdi 36,36,36,2
	addi 6,6,32
	lxvd2x 32,0,3
	lxvd2x 43,0,5
	xxpermdi 6,6,6,2
	xxpermdi 38,38,38,2
	cmpd 7,8,6
	addi 10,10,256
	lxvd2x 10,0,7
	xxpermdi 39,39,39,2
	xxpermdi 33,33,33,2
	xxpermdi 40,40,40,2
	xxpermdi 7,7,7,2
	xxpermdi 41,41,41,2
	xxpermdi 8,8,8,2
	xxpermdi 9,9,9,2
	xxpermdi 42,42,42,2
	xxpermdi 43,43,43,2
	xxpermdi 32,32,32,2
	xxpermdi 10,10,10,2
	xvabssp 58,37
	xvabssp 59,39
	xvabssp 35,40
	xvabssp 60,41
	xvabssp 34,34
	xvabssp 33,33
	xvabssp 32,32
	xvabssp 61,43
	xvabssp 36,36
	xvabssp 37,6
	xvabssp 38,38
	xvabssp 39,7
	xvabssp 40,8
	xvabssp 41,9
	xvabssp 42,42
	xvabssp 43,10
	vperm 25,4,2,12
	vperm 4,4,2,13
	vperm 2,5,26,12
	vperm 5,5,26,13
	vperm 26,1,6,12
	vperm 6,1,6,13
	vperm 1,7,27,12
	vperm 7,7,27,13
	vperm 27,8,3,12
	vperm 8,8,3,13
	vperm 3,9,28,12
	vperm 9,9,28,13
	vperm 28,0,10,12
	vperm 10,0,10,13
	vperm 0,11,29,12
	vperm 11,11,29,13
	xvaddsp 12,33,39
	xvaddsp 38,58,38
	xvaddsp 0,32,43
	xvaddsp 42,60,42
	xvaddsp 36,57,36
	xvaddsp 37,34,37
	xvaddsp 40,59,40
	xvaddsp 41,35,41
	xvcmpgtsp 32,38,12
	xvcmpgtsp 33,42,0
	xvcmpgtsp 43,36,37
	xvcmpgtsp 39,40,41
	xxsel 12,38,12,32
	xxsel 38,48,49,32
	xxsel 0,42,0,33
	xxsel 42,48,49,33
	xxsel 37,36,37,43
	xxsel 43,62,47,43
	xxsel 41,40,41,39
	xxsel 39,62,47,39
	xvcmpgtsp 32,37,12
	xvcmpgtsp 33,41,0
	xxsel 12,37,12,32
	xxsel 43,43,38,32
	xxsel 0,41,0,33
	xxsel 33,39,42,33
	xvcmpgtsp 32,12,0
	vadduwm 1,1,31
	xxsel 0,12,0,32
	xxsel 32,43,33,32
	xvcmpgtsp 33,5,0
	vadduwm 0,0,18
	vadduwm 18,18,14
	xxsel 51,51,32,33
	xxsel 5,5,0,33
	bgt 7,.L5
	xxsldwi 11,5,5,3
	xxsldwi 12,5,5,2
	vspltw 0,19,3
	xxsldwi 0,5,5,1
	xscvspdp 11,11
	xscvspdp 12,12
	mfvsrwz 6,32
	vspltw 0,19,2
	xscvspdp 0,0
	mfvsrwz 7,51
	mfvsrwz 5,32
	vspltw 0,19,0
	xscvspdp 5,5
	mfvsrwz 10,32
	fcmpu 7,11,12
	rldicl 3,6,0,32
	fmr 10,0
	rldicl 11,7,0,32
	rldicl 31,5,0,32
	rldicl 0,10,0,32
	beq 7,.L56
	bng 7,.L8
	fmr 11,12
	mr 3,31
.L8:
	fcmpu 7,0,5
	bne 7,.L11
	cmplw 7,7,10
	ble 7,.L12
	mr 7,10
.L12:
	rldicl 11,7,0,32
.L13:
	fcmpu 7,11,10
	beq 7,.L57
	bgt 7,.L58
.L17:
	cmpd 7,9,8
	ble 7,.L19
	addi 7,8,1
	sldi 10,8,1
	cmpd 7,7,9
	sldi 10,10,2
	add 4,4,10
	subf 10,8,9
	mtctr 10
	bgt 7,.L37
	li 10,-1
	rldicr 10,10,0,0
	cmpd 7,9,10
	beq 7,.L37
	.p2align 4,,15
.L21:
	lfs 0,0(4)
	lfs 12,4(4)
	addi 4,4,8
	fabs 0,0
	fabs 12,12
	fadds 0,0,12
	fcmpu 7,11,0
	bng 7,.L20
	fmr 11,0
	mr 3,8
.L20:
	addi 8,8,1
	bdnz .L21
.L19:
	li 0,-128
	ld 31,-8(1)
	addi 3,3,1
	lvx 25,1,0
	li 0,-112
	lvx 26,1,0
	li 0,-96
	lvx 27,1,0
	li 0,-80
	lvx 28,1,0
	li 0,-64
	lvx 29,1,0
	li 0,-48
	lvx 30,1,0
	li 0,-32
	lvx 31,1,0
	blr
	.p2align 4,,15
.L56:
	cmplw 7,6,5
	ble 7,.L7
	mr 6,5
.L7:
	rldicl 3,6,0,32
	b .L8
	.p2align 4,,15
.L29:
	li 3,1
	blr
	.p2align 4,,15
.L11:
	bng 7,.L13
	fmr 10,5
	mr 11,0
	b .L13
	.p2align 4,,15
.L57:
	cmpd 7,3,11
	ble 7,.L17
	mr 3,11
	b .L17
	.p2align 4,,15
.L58:
	fmr 11,10
	mr 3,11
	b .L17
.L43:
	li 9,1
	mtctr 9
	b .L44
.L37:
	li 9,1
	mtctr 9
	b .L21
	.long 0
	.byte 0,0,0,0,0,1,0,0
#if _CALL_ELF ==2
	.size	icamin_k,.-icamin_k
#endif
	.section	.rodata.cst16,"aM",@progbits,16
	.align 4
.LC2:
	.byte	0
	.byte	1
	.byte	2
	.byte	3
	.byte	8
	.byte	9
	.byte	10
	.byte	11
	.byte	16
	.byte	17
	.byte	18
	.byte	19
	.byte	24
	.byte	25
	.byte	26
	.byte	27
.LC3:
	.byte	4
	.byte	5
	.byte	6
	.byte	7
	.byte	12
	.byte	13
	.byte	14
	.byte	15
	.byte	20
	.byte	21
	.byte	22
	.byte	23
	.byte	28
	.byte	29
	.byte	30
	.byte	31
.LC4:
	.long	0
	.long	1
	.long	2
	.long	3
.LC5:
	.long	4
	.long	5
	.long	6
	.long	7
.LC6:
	.long	8
	.long	9
	.long	10
	.long	11
.LC7:
	.long	12
	.long	13
	.long	14
	.long	15
.LC8:
	.long	32
	.long	32
	.long	32
	.long	32
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.section	.note.GNU-stack,"",@progbits

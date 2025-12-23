/*
	.file	"icamin.c"
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

icamin_k:
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
	.localentry	icamin_k,.-icamin_k
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
	beq 7,.L53
	cmpdi 7,9,1
	beq 7,.L29
	addi 9,9,-1
	sldi 5,5,3
	li 3,0
	mtctr 9
	add 4,4,5
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
.L51:
	addi 3,3,1
	blr
	.p2align 4,,15
.L25:
	li 3,0
	blr
	.p2align 4,,15
.L53:
	rldicr. 8,9,0,58
	bne 0,.L54
	addi 7,8,1
	li 10,0
	subf 6,8,9
	li 3,0
	cmpd 7,7,9
	sldi 10,10,2
	mtctr 6
	add 4,4,10
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
	b .L51
	.p2align 4,,15
.L54:
	xscvdpspn 9,11
	addis 11,2,.LC2@toc@ha
	addis 3,2,.LC3@toc@ha
	addis 5,2,.LC6@toc@ha
	addis 6,2,.LC7@toc@ha
	addis 7,2,.LC4@toc@ha
	addis 10,2,.LC5@toc@ha
	xxspltib 48,0
	addi 11,11,.LC2@toc@l
	addi 3,3,.LC3@toc@l
	addi 5,5,.LC6@toc@l
	stxv 59,-80(1)
	addi 6,6,.LC7@toc@l
	stxv 60,-64(1)
	stxv 63,-16(1)
	addi 7,7,.LC4@toc@l
	xxspltib 59,16
	lxv 44,0(11)
	xxspltib 60,32
	lxv 45,0(3)
	lxv 63,0(5)
	xxlor 47,48,48
	lxv 46,0(6)
	addi 10,10,.LC5@toc@l
	stxv 61,-48(1)
	stxv 62,-32(1)
	xxspltw 9,9,0
	lxv 61,0(7)
	lxv 62,0(10)
	li 7,0
	mr 10,4
	vextsb2w 27,27
	vextsb2w 28,28
	stxv 57,-112(1)
	stxv 58,-96(1)
	.p2align 4,,15
.L5:
	lxv 0,0(10)
	addi 7,7,32
	addi 10,10,256
	cmpd 7,8,7
	xvabssp 34,0
	lxv 0,-240(10)
	xvabssp 42,0
	lxv 0,-224(10)
	xvabssp 49,0
	lxv 0,-208(10)
	vpermr 26,10,2,12
	vpermr 2,10,2,13
	xvabssp 35,0
	lxv 0,-192(10)
	xvaddsp 34,58,34
	xvabssp 36,0
	lxv 0,-176(10)
	vpermr 10,3,17,12
	vpermr 3,3,17,13
	xvabssp 33,0
	lxv 0,-160(10)
	xvaddsp 10,42,35
	xvabssp 50,0
	lxv 0,-144(10)
	vpermr 17,1,4,12
	vpermr 4,1,4,13
	xvabssp 37,0
	lxv 0,-128(10)
	xvaddsp 36,49,36
	xvabssp 38,0
	lxv 0,-112(10)
	vpermr 1,5,18,12
	vpermr 5,5,18,13
	xvabssp 43,0
	lxv 0,-96(10)
	xvaddsp 12,33,37
	xvabssp 51,0
	lxv 0,-80(10)
	vpermr 18,11,6,12
	vpermr 6,11,6,13
	xvabssp 39,0
	lxv 0,-64(10)
	xvaddsp 38,50,38
	xvabssp 40,0
	lxv 0,-48(10)
	vpermr 11,7,19,12
	vpermr 7,7,19,13
	xvabssp 32,0
	lxv 0,-32(10)
	xvaddsp 11,43,39
	xvcmpgtsp 39,34,10
	xvcmpgtsp 43,36,12
	xvabssp 57,0
	lxv 0,-16(10)
	vpermr 19,0,8,12
	vpermr 8,0,8,13
	xxsel 10,34,10,39
	xxsel 12,36,12,43
	xxsel 39,61,62,39
	xxsel 43,63,46,43
	xvabssp 41,0
	xvaddsp 40,51,40
	vpermr 0,9,25,12
	vpermr 9,9,25,13
	xvaddsp 0,32,41
	xvcmpgtsp 41,38,11
	xvcmpgtsp 32,10,12
	xvcmpgtsp 42,40,0
	xxsel 11,38,11,41
	xxsel 12,10,12,32
	xxsel 43,39,43,32
	xxsel 41,61,62,41
	xxsel 0,40,0,42
	xxsel 42,63,46,42
	xvcmpgtsp 33,11,0
	xxsel 0,11,0,33
	xxsel 33,41,42,33
	xvcmpgtsp 32,12,0
	vadduwm 1,1,27
	xxsel 0,12,0,32
	xxsel 32,43,33,32
	xvcmpgtsp 33,9,0
	vadduwm 0,0,15
	vadduwm 15,15,28
	xxsel 48,48,32,33
	xxsel 9,9,0,33
	bgt 7,.L5
	xxsldwi 11,9,9,3
	xxsldwi 12,9,9,2
	li 10,0
	li 3,12
	xxsldwi 0,9,9,1
	xscvspdp 9,9
	vextuwrx 6,10,16
	li 10,4
	xscvspdp 11,11
	xscvspdp 12,12
	xscvspdp 0,0
	vextuwrx 5,10,16
	li 10,8
	vextuwrx 7,10,16
	vextuwrx 10,3,16
	rldicl 12,5,0,32
	rldicl 3,6,0,32
	rldicl 11,7,0,32
	rldicl 0,10,0,32
	fcmpu 7,11,12
	fmr 10,0
	beq 7,.L55
	bng 7,.L8
	mr 3,12
	fmr 11,12
.L8:
	fcmpu 7,0,9
	bne 7,.L11
	cmplw 7,7,10
	ble 7,.L12
	mr 7,10
.L12:
	rldicl 11,7,0,32
.L13:
	fcmpu 7,11,10
	beq 7,.L56
	bng 7,.L17
	mr 3,11
	fmr 11,10
.L17:
	cmpd 7,9,8
	ble 7,.L19
	addi 7,8,1
	sldi 10,8,1
	subf 6,8,9
	cmpd 7,7,9
	sldi 10,10,2
	mtctr 6
	add 4,4,10
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
	lxv 57,-112(1)
	lxv 58,-96(1)
	addi 3,3,1
	lxv 59,-80(1)
	lxv 60,-64(1)
	lxv 61,-48(1)
	lxv 62,-32(1)
	lxv 63,-16(1)
	blr
	.p2align 4,,15
.L55:
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
	mr 11,0
	fmr 10,9
	b .L13
	.p2align 4,,15
.L56:
	cmpd 7,3,11
	ble 7,.L17
	mr 3,11
	b .L17
.L37:
	li 9,1
	mtctr 9
	b .L21
.L43:
	li 9,1
	mtctr 9
	b .L44
	.long 0
	.byte 0,0,0,0,0,0,0,0
	.size	icamin_k,.-icamin_k
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
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.section	.note.GNU-stack,"",@progbits

#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


FT2_GEN_WRAPPER = r"""
program ft2_ref_gen
  use wavhdr
  implicit none
  interface
     subroutine ft2_iwave(msg37, f0, snrdb, iwave)
       character(len=37), intent(in) :: msg37
       real, intent(in) :: f0
       real, intent(in) :: snrdb
       integer*2, intent(out) :: iwave(23040)
     end subroutine ft2_iwave
  end interface
  character(len=256) :: outwav
  character(len=37) :: message
  character(len=32) :: arg
  real :: f0
  integer*2 iwave(23040)
  type(hdr) :: h

  call get_command_argument(1, outwav)
  call get_command_argument(2, message)
  call get_command_argument(3, arg)
  read(arg, *) f0

  call ft2_iwave(message, f0, 99.0, iwave)
  h = default_header(12000, 23040)
  open(10, file=trim(outwav), status='replace', access='stream')
  write(10) h, iwave
  close(10)
end program ft2_ref_gen
"""


FT4_GEN_WRAPPER = r"""
program ft4_ref_gen
  use wavhdr
  implicit none
  interface
     subroutine genft4(msg0, ichk, msgsent, msgbits, i4tone)
       character(len=37), intent(in) :: msg0
       integer, intent(in) :: ichk
       character(len=37), intent(out) :: msgsent
       integer*1, intent(out) :: msgbits(77)
       integer*4, intent(out) :: i4tone(103)
     end subroutine genft4
     subroutine gen_ft4wave(itone, nsym, nsps, fsample, f0, cwave, wave, icmplx, nwave)
       integer, intent(in) :: nsym, nsps, icmplx, nwave
       integer, intent(in) :: itone(nsym)
       real, intent(in) :: fsample, f0
       complex, intent(out) :: cwave(nwave)
       real, intent(out) :: wave(nwave)
     end subroutine gen_ft4wave
  end interface
  character(len=256) :: outwav
  character(len=37) :: message
  character(len=37) :: msgsent
  character(len=32) :: arg
  integer*1 :: msgbits(77)
  integer*4 :: i4tone(103)
  real :: f0
  real :: wave(60480)
  complex :: cwave(60480)
  integer*2 :: iwave(60480)
  type(hdr) :: h

  call get_command_argument(1, outwav)
  call get_command_argument(2, message)
  call get_command_argument(3, arg)
  read(arg, *) f0

  call genft4(message, 0, msgsent, msgbits, i4tone)
  call gen_ft4wave(i4tone, 103, 576, 12000.0, f0, cwave, wave, 0, 60480)
  iwave = nint(32767.0 * wave)
  h = default_header(12000, 60480)
  open(10, file=trim(outwav), status='replace', access='stream')
  write(10) h, iwave
  close(10)
end program ft4_ref_gen
"""


FT2_DECODE_WRAPPER = r"""
program ft2_ref_decode
  use wavhdr
  use fftw3
  implicit none
  interface
     subroutine ft2_decode(cdatetime0, nfqso, iwave, ndecodes, mycall, hiscall, nrx, line)
       character(len=17), intent(in) :: cdatetime0
       integer, intent(inout) :: nfqso
       integer*2, intent(in) :: iwave(30000)
       integer, intent(out) :: ndecodes
       character(len=6), intent(inout) :: mycall
       character(len=6), intent(inout) :: hiscall
       integer, intent(out) :: nrx
       character(len=61), intent(out) :: line
     end subroutine ft2_decode
  end interface
  character(len=256) :: wav_path, replay_line
  character(len=17) :: stamp
  character(len=61) :: line
  integer*2 iwave(30000)
  integer :: ndecodes, nrx, ios, nsamp, iret, nfqso
  integer :: npatience, nthreads
  character(len=6) :: mycall, hiscall
  type(hdr) :: h
  common /patience/ npatience, nthreads

  mycall = 'K1ABC '
  hiscall = 'W9XYZ '
  stamp = '000000_000000000'
  nfqso = -1
  npatience = 1
  nthreads = 1
  iret = fftwf_init_threads()
  call fftwf_plan_with_nthreads(1)
  open(24, file='all_ft2.txt', status='replace')
  close(24)

  call get_command_argument(1, wav_path)
  open(10, file=trim(wav_path), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, size(iwave))
  read(10) iwave(1:nsamp)
  close(10)
  call ft2_decode(stamp, nfqso, iwave, ndecodes, mycall, hiscall, nrx, line)
  open(24, file='all_ft2.txt', status='old', iostat=ios)
  if (ios .eq. 0) then
     do
        read(24, '(a)', iostat=ios) replay_line
        if (ios .ne. 0) exit
        if (replay_line(1:17) .eq. stamp) then
           print '(a)', trim(replay_line(19:))
        endif
     end do
     close(24)
  endif
end program ft2_ref_decode
"""


FT4_FRAME_WRAPPER = r"""
program ft4_ref_frame
  use packjt77
  implicit none
  interface
     subroutine genft4(msg0, ichk, msgsent, msgbits, i4tone)
       character(len=37), intent(in) :: msg0
       integer, intent(in) :: ichk
       character(len=37), intent(out) :: msgsent
       integer*1, intent(out) :: msgbits(77)
       integer*4, intent(out) :: i4tone(103)
     end subroutine genft4
     subroutine encode174_91(message77, codeword)
       integer*1, intent(in) :: message77(77)
       integer*1, intent(out) :: codeword(174)
     end subroutine encode174_91
  end interface
  character(len=37) :: message
  character(len=37) :: msgsent
  integer*1 :: msgbits(77), codeword(174)
  integer*4 :: i4tone(103)
  integer :: i

  call get_command_argument(1, message)
  call genft4(message, 0, msgsent, msgbits, i4tone)
  call encode174_91(msgbits, codeword)

  write(*, '(a)', advance='no') 'message_bits='
  do i = 1, 77
     write(*, '(i1)', advance='no') msgbits(i)
  end do
  write(*, *)

  write(*, '(a)', advance='no') 'codeword_bits='
  do i = 1, 174
     write(*, '(i1)', advance='no') codeword(i)
  end do
  write(*, *)

  write(*, '(a)', advance='no') 'channel_symbols='
  do i = 1, 103
     write(*, '(i1)', advance='no') i4tone(i)
  end do
  write(*, *)
end program ft4_ref_frame
"""


FT4_DEBUG_WRAPPER = r"""
program ft4_stock_debug
  use wavhdr
  use packjt77
  implicit none
  integer, parameter :: KK=91
  integer, parameter :: ND=87
  integer, parameter :: NS=16
  integer, parameter :: NN=NS+ND
  integer, parameter :: NSPS=576
  integer, parameter :: NMAX=21*3456
  integer, parameter :: NFFT1=2304
  integer, parameter :: NH1=NFFT1/2
  integer, parameter :: NDOWN=18
  integer, parameter :: NSS=NSPS/NDOWN
  integer, parameter :: NDMAX=NMAX/NDOWN
  integer, parameter :: MAXCAND=200

  interface
     subroutine getcandidates4(dd, fa, fb, syncmin, nfqso, maxcand, savg, candidate, ncand, sbase)
       real, intent(in) :: dd(72576), fa, fb, syncmin, nfqso
       integer, intent(in) :: maxcand
       real, intent(out) :: savg(1152), candidate(2,maxcand), sbase(1152)
       integer, intent(out) :: ncand
     end subroutine getcandidates4
     subroutine ft4_downsample(dd, newdata, f0, c)
       real, intent(in) :: dd(72576), f0
       logical, intent(in) :: newdata
       complex, intent(out) :: c(0:4031)
     end subroutine ft4_downsample
     subroutine sync4d(cd2, istart, ctwk, ncoh, sync)
       complex, intent(in) :: cd2(0:4031), ctwk(64)
       integer, intent(in) :: istart, ncoh
       real, intent(out) :: sync
     end subroutine sync4d
     subroutine get_ft4_bitmetrics(cd, bitmetrics, badsync)
       complex, intent(in) :: cd(0:3295)
       real, intent(out) :: bitmetrics(206,3)
       logical, intent(out) :: badsync
     end subroutine get_ft4_bitmetrics
     subroutine decode174_91(llr, Keff, maxosd, norder, apmask, message91, cw, ntype, nharderror, dmin)
       integer, intent(in) :: Keff, maxosd, norder
       real, intent(in) :: llr(174)
       integer*1, intent(in) :: apmask(174)
       integer*1, intent(out) :: message91(91), cw(174)
       integer, intent(out) :: ntype, nharderror
       real, intent(out) :: dmin
     end subroutine decode174_91
     subroutine twkfreq1(c, npts, fs, a, ctwk)
       integer, intent(in) :: npts
       real, intent(in) :: fs, a(5)
       complex, intent(in) :: c(npts)
       complex, intent(out) :: ctwk(npts)
     end subroutine twkfreq1
  end interface

  character(len=256) :: wav_path, arg
  integer*2 :: iwave(NMAX)
  real :: dd(NMAX)
  real :: savg(NH1), sbase(NH1)
  real :: candidate(2,MAXCAND)
  real :: syncmin, nfqso, fa, fb, f0, f1, sync, smax, smax1, sum2
  real :: bitmetrics(2*NN,3)
  real :: llr(174), llra(174), llrb(174), llrc(174), llrd(174), dmin, apmag
  real :: fs, a(5)
  integer :: ncand, i, ios, nsamp, ipass, Keff, maxosd, norder, ntype, nharderror
  integer :: idf, iseg, isync, idfmin, idfmax, idfstp
  integer :: ibmin, ibmax, ibstp, ibest, idfbest, istart, it, np
  integer :: hbits(2*NN), ns1, ns2, ns3, ns4, nsync_qual
  integer :: mcq(29)
  integer*1 :: apmask(174), message91(91), cw(174), message77(77), rvec(77)
  logical :: badsync
  logical :: unpk77_success
  character(len=37) :: decoded
  character(len=77) :: c77
  complex :: ctwk(2*NSS), ctwk2(2*NSS,-16:16)
  complex :: cd2(0:NDMAX-1), cb(0:NDMAX-1), cd(0:NN*NSS-1)
  type(hdr) :: h
  data mcq/0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0/
  data rvec/0,1,0,0,1,0,1,0,0,1,0,1,1,1,1,0,1,0,0,0,1,0,0,1,1,0,1,1,0, &
     1,0,0,1,0,1,1,0,0,0,0,1,0,0,0,1,0,1,0,0,1,1,1,1,0,0,1,0,1, &
     0,1,0,1,0,1,1,0,1,1,1,1,1,0,0,0,1,0,1/

  mcq = 2*mod(mcq + rvec(1:29), 2) - 1

  call get_command_argument(1, wav_path)
  call get_command_argument(2, arg)
  read(arg, *) f0

  open(10, file=trim(wav_path), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, NMAX)
  read(10) iwave(1:nsamp)
  close(10)
  dd = 0.0
  dd(1:nsamp) = iwave(1:nsamp)

  fs = 12000.0 / NDOWN
  do idf=-16,16
     a = 0.0
     a(1) = real(idf)
     ctwk = 1.0
     call twkfreq1(ctwk, 2*NSS, fs/2.0, a, ctwk2(:,idf))
  end do

  syncmin = 1.2
  nfqso = 1500.0
  fa = 200.0
  fb = 4000.0
  savg = 0.0
  sbase = 0.0
  candidate = 0.0
  call getcandidates4(dd, fa, fb, syncmin, nfqso, MAXCAND, savg, candidate, ncand, sbase)
  print '(a,i0)', 'ncand=', ncand
  do i = 1, min(ncand, 16)
     print '(a,i0,a,f10.4,a,f10.4)', 'cand(', i, ')=freq:', candidate(1,i), ' score:', candidate(2,i)
  end do

  call ft4_downsample(dd, .true., f0, cd2)
  sum2 = sum(cd2*conjg(cd2)) / (real(NMAX) / real(NDOWN))
  if (sum2 .gt. 0.0) cd2 = cd2 / sqrt(sum2)

  do iseg=1,3
     do isync=1,2
        if (isync .eq. 1) then
           idfmin = -12
           idfmax = 12
           idfstp = 3
           ibmin = -344
           ibmax = 1012
           if (iseg .eq. 1) then
              ibmin = 108
              ibmax = 560
           elseif (iseg .eq. 2) then
              ibmin = 560
              ibmax = 1012
           elseif (iseg .eq. 3) then
              ibmin = -344
              ibmax = 108
           endif
           ibstp = 4
        else
           idfmin = idfbest - 4
           idfmax = idfbest + 4
           idfstp = 1
           ibmin = ibest - 5
           ibmax = ibest + 5
           ibstp = 1
        endif
        ibest = -1
        idfbest = 0
        smax = -99.0
        do idf=idfmin,idfmax,idfstp
           do istart=ibmin,ibmax,ibstp
              call sync4d(cd2, istart, ctwk2(:,idf), 1, sync)
              if (sync .gt. smax) then
                 smax = sync
                 ibest = istart
                 idfbest = idf
              endif
           enddo
        enddo
     enddo

     if (iseg .eq. 1) smax1 = smax
     print '(a,i0,a,f10.4,a,i0,a,i0)', 'segment=', iseg, ' smax=', smax, ' ibest=', ibest, ' idfbest=', idfbest
     if (smax .lt. 1.2) cycle
     if (iseg .gt. 1 .and. smax .lt. smax1) cycle

     f1 = f0 + real(idfbest)
     call ft4_downsample(dd, .false., f1, cb)
     sum2 = sum(abs(cb)**2) / (real(NSS)*NN)
     if (sum2 .gt. 0.0) cb = cb / sqrt(sum2)
     cd = 0.0
     if (ibest .ge. 0) then
        it = min(NDMAX-1, ibest + NN*NSS - 1)
        np = it - ibest + 1
        cd(0:np-1) = cb(ibest:it)
     else
        cd(-ibest:ibest+NN*NSS-1) = cb(0:NN*NSS+2*ibest-1)
     endif

     call get_ft4_bitmetrics(cd, bitmetrics, badsync)
     if (badsync) then
        print '(a)', 'variant_badsync=1'
        cycle
     endif
     hbits = 0
     where(bitmetrics(:,1) .ge. 0.0) hbits = 1
     ns1 = count(hbits(  1:  8) .eq. (/0,0,0,1,1,0,1,1/))
     ns2 = count(hbits( 67: 74) .eq. (/0,1,0,0,1,1,1,0/))
     ns3 = count(hbits(133:140) .eq. (/1,1,1,0,0,1,0,0/))
     ns4 = count(hbits(199:206) .eq. (/1,0,1,1,0,0,0,1/))
     nsync_qual = ns1 + ns2 + ns3 + ns4
     print '(a,i0,a,f10.4,a,f10.4,a,i0,a,i0)', 'variant_segment=', iseg, ' f1=', f1, ' smax=', smax, ' nsync_qual=', nsync_qual, ' ibest=', ibest

     llra(  1: 58)=bitmetrics(  9: 66, 1)
     llra( 59:116)=bitmetrics( 75:132, 1)
     llra(117:174)=bitmetrics(141:198, 1)
     llrb(  1: 58)=bitmetrics(  9: 66, 2)
     llrb( 59:116)=bitmetrics( 75:132, 2)
     llrb(117:174)=bitmetrics(141:198, 2)
     llrc(  1: 58)=bitmetrics(  9: 66, 3)
     llrc( 59:116)=bitmetrics( 75:132, 3)
     llrc(117:174)=bitmetrics(141:198, 3)
     llra = 2.83 * llra
     llrb = 2.83 * llrb
     llrc = 2.83 * llrc
     apmag = maxval(abs(llra)) * 1.1
     Keff = 91
     maxosd = -1
     norder = 2

     do ipass = 1, 4
        if (ipass .eq. 1) llr = llra
        if (ipass .eq. 2) llr = llrb
        if (ipass .eq. 3) llr = llrc
        apmask = 0
        if (ipass .eq. 4) then
           llrd = llrc
           apmask(1:29) = 1
           llrd(1:29) = apmag * mcq(1:29)
           llr = llrd
        endif
        message91 = 0
        cw = 0
        dmin = 0.0
        call decode174_91(llr, Keff, maxosd, norder, apmask, message91, cw, ntype, nharderror, dmin)
        if (sum(message91) .eq. 0) cycle
        if (nharderror .lt. 0) cycle
        message77 = mod(message91(1:77) + rvec, 2)
        write(c77,'(77i1)') message77
        call unpack77(c77, 1, decoded, unpk77_success)
        if (.not. unpk77_success) cycle
        print '(a,i0,a,a)', 'variant_pass=', ipass, ' decoded=', trim(decoded)
     end do
  end do
end program ft4_stock_debug
"""


FT4_SUBTRACT_WRAPPER = r"""
program ft4_stock_subtract
  use wavhdr
  implicit none
  interface
     subroutine genft4(msg0, ichk, msgsent, msgbits, i4tone)
       character(len=37), intent(in) :: msg0
       integer, intent(in) :: ichk
       character(len=37), intent(out) :: msgsent
       integer*1, intent(out) :: msgbits(77)
       integer*4, intent(out) :: i4tone(103)
     end subroutine genft4
     subroutine subtractft4(dd, itone, f0, dt)
       real, intent(inout) :: dd(72576)
       integer, intent(in) :: itone(103)
       real, intent(in) :: f0, dt
     end subroutine subtractft4
  end interface

  character(len=256) :: inwav, outwav, message, arg
  character(len=37) :: msgsent
  integer*2 :: iwave(72576), owave(72576)
  integer*1 :: msgbits(77)
  integer*4 :: i4tone(103)
  real :: dd(72576), f0, dt
  integer :: ios, nsamp
  type(hdr) :: h

  call get_command_argument(1, inwav)
  call get_command_argument(2, outwav)
  call get_command_argument(3, message)
  call get_command_argument(4, arg)
  read(arg, *) f0
  call get_command_argument(5, arg)
  read(arg, *) dt

  open(10, file=trim(inwav), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, 72576)
  read(10) iwave(1:nsamp)
  close(10)

  dd = 0.0
  dd(1:nsamp) = iwave(1:nsamp)
  call genft4(message, 0, msgsent, msgbits, i4tone)
  call subtractft4(dd, i4tone, f0, dt)

  owave = nint(max(-32767.0, min(32767.0, dd)))
  h = default_header(12000, nsamp)
  open(20, file=trim(outwav), status='replace', access='stream')
  write(20) h, owave(1:nsamp)
  close(20)
end program ft4_stock_subtract
"""


FT4_TRACE_WRAPPER = r"""
program ft4_stock_trace
  use wavhdr
  use packjt77
  implicit none
  integer, parameter :: KK=91
  integer, parameter :: ND=87
  integer, parameter :: NS=16
  integer, parameter :: NN=NS+ND
  integer, parameter :: NSPS=576
  integer, parameter :: NMAX=21*3456
  integer, parameter :: NFFT1=2304
  integer, parameter :: NH1=NFFT1/2
  integer, parameter :: NDOWN=18
  integer, parameter :: NSS=NSPS/NDOWN
  integer, parameter :: NDMAX=NMAX/NDOWN
  integer, parameter :: MAXCAND=200

  interface
     subroutine getcandidates4(dd, fa, fb, syncmin, nfqso, maxcand, savg, candidate, ncand, sbase)
       real, intent(in) :: dd(72576), fa, fb, syncmin, nfqso
       integer, intent(in) :: maxcand
       real, intent(out) :: savg(1152), candidate(2,maxcand), sbase(1152)
       integer, intent(out) :: ncand
     end subroutine getcandidates4
     subroutine ft4_downsample(dd, newdata, f0, c)
       real, intent(in) :: dd(72576), f0
       logical, intent(in) :: newdata
       complex, intent(out) :: c(0:4031)
     end subroutine ft4_downsample
     subroutine sync4d(cd2, istart, ctwk, ncoh, sync)
       complex, intent(in) :: cd2(0:4031), ctwk(64)
       integer, intent(in) :: istart, ncoh
       real, intent(out) :: sync
     end subroutine sync4d
     subroutine get_ft4_bitmetrics(cd, bitmetrics, badsync)
       complex, intent(in) :: cd(0:3295)
       real, intent(out) :: bitmetrics(206,3)
       logical, intent(out) :: badsync
     end subroutine get_ft4_bitmetrics
     subroutine decode174_91(llr, Keff, maxosd, norder, apmask, message91, cw, ntype, nharderror, dmin)
       integer, intent(in) :: Keff, maxosd, norder
       real, intent(in) :: llr(174)
       integer*1, intent(in) :: apmask(174)
       integer*1, intent(out) :: message91(91), cw(174)
       integer, intent(out) :: ntype, nharderror
       real, intent(out) :: dmin
     end subroutine decode174_91
     subroutine twkfreq1(c, npts, fs, a, ctwk)
       integer, intent(in) :: npts
       real, intent(in) :: fs, a(5)
       complex, intent(in) :: c(npts)
       complex, intent(out) :: ctwk(npts)
     end subroutine twkfreq1
     subroutine subtractft4(dd, itone, f0, dt)
       real, intent(inout) :: dd(72576)
       integer, intent(in) :: itone(103)
       real, intent(in) :: f0, dt
     end subroutine subtractft4
     subroutine get_ft4_tones_from_77bits(msgbits, i4tone)
       integer*1, intent(in) :: msgbits(77)
       integer*4, intent(out) :: i4tone(103)
     end subroutine get_ft4_tones_from_77bits
  end interface

  character(len=256) :: wav_path, arg
  integer*2 :: iwave(NMAX)
  real :: dd(NMAX)
  real :: savg(NH1), sbase(NH1)
  real :: candidate(2,MAXCAND)
  real :: syncmin, nfqso, fa, fb, f0, f1, sync, smax, smax1, sum2
  real :: bitmetrics(2*NN,3)
  real :: llr(174), llra(174), llrb(174), llrc(174), llrd(174), dmin, apmag
  real :: fs, a(5), dt
  integer :: ncand, i, ios, nsamp, ipass, Keff, maxosd, norder, ntype, nharderror
  integer :: idf, iseg, isync, idfmin, idfmax, idfstp
  integer :: ibmin, ibmax, ibstp, ibest, idfbest, istart, it, np
  integer :: hbits(2*NN), ns1, ns2, ns3, ns4, nsync_qual
  integer :: mcq(29), nappasses(0:5), naptypes(0:5,4), ndepth, nsp, isp, iaptype
  integer :: i3, n3, l1
  integer*1 :: apbits(174), apmask(174), message91(91), cw(174), message77(77), rvec(77)
  integer*4 :: i4tone(103)
  logical :: badsync, doosd, dosubtract
  logical :: unpk77_success
  character(len=37) :: decoded, msgsent
  character(len=77) :: c77
  character(len=12) :: mycall, hiscall, hiscall0
  complex :: ctwk(2*NSS), ctwk2(2*NSS,-16:16)
  complex :: cd2(0:NDMAX-1), cb(0:NDMAX-1), cd(0:NN*NSS-1)
  type(hdr) :: h
  data mcq/0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0/
  data rvec/0,1,0,0,1,0,1,0,0,1,0,1,1,1,1,0,1,0,0,0,1,0,0,1,1,0,1,1,0, &
     1,0,0,1,0,1,1,0,0,0,0,1,0,0,0,1,0,1,0,0,1,1,1,1,0,0,1,0,1, &
     0,1,0,1,0,1,1,0,1,1,1,1,1,0,0,0,1,0,1/

  call get_command_argument(1, wav_path)
  call get_command_argument(2, arg)
  read(arg, *) ndepth

  mcq = 2*mod(mcq + rvec(1:29), 2) - 1
  nappasses = 0
  naptypes = 0
  nappasses(0)=2
  naptypes(0,1:4)=(/1,2,0,0/)
  apbits = 0
  apbits(1) = 99
  apbits(30) = 99
  mycall = 'K1ABC'
  hiscall = 'W9XYZ'
  hiscall0 = hiscall
  dxcall13 = hiscall
  mycall13 = mycall
  l1 = index(mycall, char(0))
  if (l1 .ne. 0) mycall(l1:) = ' '
  l1 = index(hiscall, char(0))
  if (l1 .ne. 0) hiscall(l1:) = ' '
  if (len(trim(mycall)) .ge. 3) then
     if (len(trim(hiscall0)) .lt. 3) hiscall0 = mycall
     decoded = trim(mycall) // ' ' // trim(hiscall0) // ' RR73'
     i3 = -1
     n3 = -1
     call pack77(decoded, i3, n3, c77)
     call unpack77(c77, 1, msgsent, unpk77_success)
     if (i3 .eq. 1 .and. decoded .eq. msgsent .and. unpk77_success) then
        read(c77, '(77i1)') message77
        message77 = mod(message77 + rvec, 2)
        call encode174_91(message77, cw)
        apbits = 2*cw - 1
     endif
  endif

  open(10, file=trim(wav_path), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, NMAX)
  read(10) iwave(1:nsamp)
  close(10)
  dd = 0.0
  dd(1:nsamp) = iwave(1:nsamp)

  fs = 12000.0 / NDOWN
  do idf=-16,16
     a = 0.0
     a(1) = real(idf)
     ctwk = 1.0
     call twkfreq1(ctwk, 2*NSS, fs/2.0, a, ctwk2(:,idf))
  end do

  syncmin = 1.2
  nfqso = 1500.0
  fa = 200.0
  fb = 4000.0
  dosubtract = .true.
  doosd = .true.
  nsp = 3
  if (ndepth .eq. 2) doosd = .false.
  if (ndepth .eq. 1) then
     nsp = 1
     dosubtract = .false.
     doosd = .false.
  endif

  do isp = 1, nsp
     savg = 0.0
     sbase = 0.0
     candidate = 0.0
     call getcandidates4(dd, fa, fb, syncmin, nfqso, MAXCAND, savg, candidate, ncand, sbase)
     print '(a,i0,a,i0)', 'pass=', isp, ' ncand=', ncand
     do i = 1, min(ncand, 16)
        print '(a,i0,a,i0,a,f10.4,a,f10.4)', 'pass=', isp, ' cand=', i, ' freq=', candidate(1,i), ' score=', candidate(2,i)
     end do
     if (ncand .eq. 0) exit

     do i = 1, ncand
        f0 = candidate(1,i)
        call ft4_downsample(dd, i .eq. 1, f0, cd2)
        sum2 = sum(cd2*conjg(cd2)) / (real(NMAX) / real(NDOWN))
        if (sum2 .gt. 0.0) cd2 = cd2 / sqrt(sum2)

        smax1 = 0.0
        do iseg=1,3
           do isync=1,2
              if (isync .eq. 1) then
                 idfmin = -12
                 idfmax = 12
                 idfstp = 3
                 ibmin = -344
                 ibmax = 1012
                 if (iseg .eq. 1) then
                    ibmin = 108
                    ibmax = 560
                 elseif (iseg .eq. 2) then
                    ibmin = 560
                    ibmax = 1012
                 elseif (iseg .eq. 3) then
                    ibmin = -344
                    ibmax = 108
                 endif
                 ibstp = 4
              else
                 idfmin = idfbest - 4
                 idfmax = idfbest + 4
                 idfstp = 1
                 ibmin = ibest - 5
                 ibmax = ibest + 5
                 ibstp = 1
              endif
              ibest = -1
              idfbest = 0
              smax = -99.0
              do idf=idfmin,idfmax,idfstp
                 do istart=ibmin,ibmax,ibstp
                    call sync4d(cd2, istart, ctwk2(:,idf), 1, sync)
                    if (sync .gt. smax) then
                       smax = sync
                       ibest = istart
                       idfbest = idf
                    endif
                 enddo
              enddo
           enddo

           if (iseg .eq. 1) smax1 = smax
           if (smax .lt. 1.2) cycle
           if (iseg .gt. 1 .and. smax .lt. smax1) cycle

           f1 = f0 + real(idfbest)
           call ft4_downsample(dd, .false., f1, cb)
           sum2 = sum(abs(cb)**2) / (real(NSS) * NN)
           if (sum2 .gt. 0.0) cb = cb / sqrt(sum2)
           cd = 0.0
           if (ibest .ge. 0) then
              it = min(NDMAX-1, ibest + NN*NSS - 1)
              np = it - ibest + 1
              cd(0:np-1) = cb(ibest:it)
           else
              cd(-ibest:ibest+NN*NSS-1) = cb(0:NN*NSS+2*ibest-1)
           endif

           call get_ft4_bitmetrics(cd, bitmetrics, badsync)
           if (badsync) cycle
           hbits = 0
           where(bitmetrics(:,1) .ge. 0.0) hbits = 1
           ns1 = count(hbits(  1:  8) .eq. (/0,0,0,1,1,0,1,1/))
           ns2 = count(hbits( 67: 74) .eq. (/0,1,0,0,1,1,1,0/))
           ns3 = count(hbits(133:140) .eq. (/1,1,1,0,0,1,0,0/))
           ns4 = count(hbits(199:206) .eq. (/1,0,1,1,0,0,0,1/))
           nsync_qual = ns1 + ns2 + ns3 + ns4
           if (nsync_qual .lt. 20) cycle

           llra(  1: 58)=bitmetrics(  9: 66, 1)
           llra( 59:116)=bitmetrics( 75:132, 1)
           llra(117:174)=bitmetrics(141:198, 1)
           llrb(  1: 58)=bitmetrics(  9: 66, 2)
           llrb( 59:116)=bitmetrics( 75:132, 2)
           llrb(117:174)=bitmetrics(141:198, 2)
           llrc(  1: 58)=bitmetrics(  9: 66, 3)
           llrc( 59:116)=bitmetrics( 75:132, 3)
           llrc(117:174)=bitmetrics(141:198, 3)
           llra = 2.83 * llra
           llrb = 2.83 * llrb
           llrc = 2.83 * llrc
           apmag = maxval(abs(llra)) * 1.1

           do ipass = 1, 3 + nappasses(0)
              if (ipass .eq. 1) llr = llra
              if (ipass .eq. 2) llr = llrb
              if (ipass .eq. 3) llr = llrc
              if (ipass .le. 3) then
                 apmask = 0
                 iaptype = 0
              else
                 llrd = llrc
                 iaptype = naptypes(0, ipass - 3)
                 apmask = 0
                 if (iaptype .eq. 1) then
                    apmask(1:29) = 1
                    llrd(1:29) = apmag * mcq(1:29)
                 else
                    cycle
                 endif
                 llr = llrd
              endif

              Keff = 91
              norder = 2
              maxosd = 2
              if (abs(nfqso-f1) .le. 80.0) maxosd = 3
              if (.not. doosd) maxosd = -1
              message91 = 0
              cw = 0
              dmin = 0.0
              call decode174_91(llr, Keff, maxosd, norder, apmask, message91, cw, ntype, nharderror, dmin)
              if (sum(message91) .eq. 0) cycle
              if (nharderror .lt. 0) cycle
              message77 = mod(message91(1:77) + rvec, 2)
              write(c77,'(77i1)') message77
              call unpack77(c77, 1, decoded, unpk77_success)
              if (.not. unpk77_success) cycle
              dt = real(ibest) / 666.67
              print '(a,i0,a,i0,a,i0,a,i0,a,i0,a,i0,a,f8.4,a,f8.4,a,a)', &
                 'decode pass=', isp, ' cand=', i, ' segment=', iseg, ' ipass=', ipass, &
                 ' ntype=', ntype, ' nharderror=', nharderror, &
                 ' dt=', dt - 0.5, ' freq=', f1, ' message=', trim(decoded)
              if (dosubtract) then
                 call get_ft4_tones_from_77bits(message77, i4tone)
                 call subtractft4(dd, i4tone, f1, dt)
              endif
              exit
           end do
           if (nharderror .ge. 0) exit
        end do
     end do
  end do
end program ft4_stock_trace
"""


FT4_METRICS_WRAPPER = r"""
program ft4_stock_metrics
  use wavhdr
  implicit none
  integer, parameter :: NS=16
  integer, parameter :: ND=87
  integer, parameter :: NN=NS+ND
  integer, parameter :: NSPS=576
  integer, parameter :: NMAX=21*3456
  integer, parameter :: NDOWN=18
  integer, parameter :: NSS=NSPS/NDOWN
  integer, parameter :: NDMAX=NMAX/NDOWN
  interface
     subroutine ft4_downsample(dd, newdata, f0, c)
       real, intent(in) :: dd(72576), f0
       logical, intent(in) :: newdata
       complex, intent(out) :: c(0:4031)
     end subroutine ft4_downsample
     subroutine get_ft4_bitmetrics(cd, bitmetrics, badsync)
       complex, intent(in) :: cd(0:3295)
       real, intent(out) :: bitmetrics(206,3)
       logical, intent(out) :: badsync
     end subroutine get_ft4_bitmetrics
  end interface

  character(len=256) :: wav_path, arg
  integer*2 :: iwave(NMAX)
  real :: dd(NMAX), bitmetrics(206,3), llra(174), llrb(174), llrc(174), sum2, f0, dt
  integer :: ios, nsamp, ibest, i
  logical :: badsync
  complex :: cb(0:NDMAX-1), cd(0:NN*NSS-1)
  type(hdr) :: h

  call get_command_argument(1, wav_path)
  call get_command_argument(2, arg)
  read(arg, *) f0
  call get_command_argument(3, arg)
  read(arg, *) dt

  open(10, file=trim(wav_path), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, NMAX)
  read(10) iwave(1:nsamp)
  close(10)

  dd = 0.0
  dd(1:nsamp) = iwave(1:nsamp)
  call ft4_downsample(dd, .true., f0, cb)
  sum2 = sum(abs(cb)**2) / (real(NSS) * NN)
  if (sum2 .gt. 0.0) cb = cb / sqrt(sum2)

  ibest = nint((dt + 0.5) * 666.67)
  cd = 0.0
  if (ibest .ge. 0) then
     cd(0:NN*NSS-1) = cb(ibest:ibest+NN*NSS-1)
  else
     cd(-ibest:ibest+NN*NSS-1) = cb(0:NN*NSS+2*ibest-1)
  endif

  call get_ft4_bitmetrics(cd, bitmetrics, badsync)
  print '(a,l1)', 'badsync=', badsync
  write(*,'(a)',advance='no') 'bitmetrics1='
  do i=1,206
     write(*,'(f0.6,1x)',advance='no') bitmetrics(i,1)
  enddo
  write(*,*)
  write(*,'(a)',advance='no') 'bitmetrics2='
  do i=1,206
     write(*,'(f0.6,1x)',advance='no') bitmetrics(i,2)
  enddo
  write(*,*)
  write(*,'(a)',advance='no') 'bitmetrics3='
  do i=1,206
     write(*,'(f0.6,1x)',advance='no') bitmetrics(i,3)
  enddo
  write(*,*)

  llra(  1: 58)=bitmetrics(  9: 66, 1)
  llra( 59:116)=bitmetrics( 75:132, 1)
  llra(117:174)=bitmetrics(141:198, 1)
  llrb(  1: 58)=bitmetrics(  9: 66, 2)
  llrb( 59:116)=bitmetrics( 75:132, 2)
  llrb(117:174)=bitmetrics(141:198, 2)
  llrc(  1: 58)=bitmetrics(  9: 66, 3)
  llrc( 59:116)=bitmetrics( 75:132, 3)
  llrc(117:174)=bitmetrics(141:198, 3)
  llra = 2.83 * llra
  llrb = 2.83 * llrb
  llrc = 2.83 * llrc
  write(*,'(a)',advance='no') 'llra='
  do i=1,174
     write(*,'(f0.6,1x)',advance='no') llra(i)
  enddo
  write(*,*)
  write(*,'(a)',advance='no') 'llrb='
  do i=1,174
     write(*,'(f0.6,1x)',advance='no') llrb(i)
  enddo
  write(*,*)
  write(*,'(a)',advance='no') 'llrc='
  do i=1,174
     write(*,'(f0.6,1x)',advance='no') llrc(i)
  enddo
  write(*,*)
end program ft4_stock_metrics
"""


FT2_FRAME_WRAPPER = r"""
program ft2_ref_frame
  use packjt77
  implicit none
  interface
     subroutine encode_128_90(msgbits, codeword)
       integer*1, intent(in) :: msgbits(77)
       integer*1, intent(out) :: codeword(128)
     end subroutine encode_128_90
     subroutine genft2(msg0, ichk, msgsent, i4tone, itype)
       character(len=37), intent(in) :: msg0
       integer, intent(in) :: ichk
       character(len=37), intent(out) :: msgsent
       integer*4, intent(out) :: i4tone(144)
       integer, intent(out) :: itype
     end subroutine genft2
  end interface
  character(len=37) :: message
  character(len=37) :: msgsent
  character(len=77) :: c77
  integer*1 :: msgbits(77), codeword(128)
  integer*4 :: i4tone(144)
  integer :: i3, n3, itype, i
  logical :: unpk77_success

  call get_command_argument(1, message)
  i3 = -1
  n3 = -1
  call pack77(message, i3, n3, c77)
  call unpack77(c77, 0, msgsent, unpk77_success)
  read(c77, '(77i1)') msgbits
  call encode_128_90(msgbits, codeword)
  call genft2(message, 0, msgsent, i4tone, itype)

  write(*, '(a)', advance='no') 'message_bits='
  do i = 1, 77
     write(*, '(i1)', advance='no') msgbits(i)
  end do
  write(*, *)

  write(*, '(a)', advance='no') 'codeword_bits='
  do i = 1, 128
     write(*, '(i1)', advance='no') codeword(i)
  end do
  write(*, *)

  write(*, '(a)', advance='no') 'channel_symbols='
  do i = 1, 144
     write(*, '(i1)', advance='no') i4tone(i)
  end do
  write(*, *)
end program ft2_ref_frame
"""


FT2_TRACE_WRAPPER = r"""
program ft2_ref_trace
  use wavhdr
  use packjt77
  implicit none
  integer, parameter :: KK=90
  integer, parameter :: ND=128
  integer, parameter :: NS=16
  integer, parameter :: NN=NS+ND
  integer, parameter :: NSPS=160
  integer, parameter :: NZ=NSPS*NN
  integer, parameter :: NMAX=30000
  integer, parameter :: NFFT1=400
  integer, parameter :: NH1=NFFT1/2
  integer, parameter :: NSTEP=NSPS/4
  integer, parameter :: NHSYM=NMAX/NSTEP-3
  integer, parameter :: NDOWN=16
  character message*37,c77*77
  character(len=256) :: wav_path
  character(len=6) :: mycall,hiscall
  complex c2(0:NMAX/16-1)
  complex cb(0:NMAX/16-1)
  complex cd(0:144*10-1)
  complex c1(0:9),c0(0:9)
  complex ccor(0:1,144)
  complex csum,cterm,cc0,cc1,csync1
  real a(5)
  real rxdata(128),llr(128),llr2(128)
  real sbits(144),sbits1(144),sbits3(144)
  real ps(0:8191),psbest(0:8191)
  real candidate(3,100)
  real savg(NH1)
  integer*2 iwave(NMAX)
  integer*1 message77(77),apmask(128),cw(128)
  integer*1 hbits(144),hbits1(144),hbits3(144)
  integer*1 s16(16)
  logical unpk77_success
  integer :: ios, nsamp, ncand, icand, ifreq, is, ib, i1, ibit, nseq
  integer :: numseq, iseq, k, i, ibb, ibflag, nbit, nsync_qual
  integer :: nharderror, niterations, max_iterations, ibias
  real :: fs, dt, tt, baud, txt, twopi, hh, dphi, dphi0, dphi1
  real :: phi0, phi1, the, fa, fb, syncmin, f0, df, dfbest, sybest
  real :: s2, rxav, rx2av, rxsig, sigma, pmax
  integer :: ibest
  type(hdr) :: h
  data s16/0,0,0,0,1,1,1,1,1,1,1,1,0,0,0,0/

  mycall = 'K1ABC '
  hiscall = 'W9XYZ '

  call get_command_argument(1, wav_path)
  open(10, file=trim(wav_path), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, size(iwave))
  read(10) iwave(1:nsamp)
  close(10)

  fs=12000.0/NDOWN
  dt=1/fs
  tt=NSPS*dt
  baud=1.0/tt
  txt=NZ*dt
  twopi=8.0*atan(1.0)
  hh=0.8
  dphi=twopi/2*baud*hh*dt*16
  dphi0=-1*dphi
  dphi1=+1*dphi
  phi0=0.0
  phi1=0.0
  do i=0,9
    c1(i)=cmplx(cos(phi1),sin(phi1))
    c0(i)=cmplx(cos(phi0),sin(phi0))
    phi1=mod(phi1+dphi1,twopi)
    phi0=mod(phi0+dphi0,twopi)
  enddo
  the=twopi*hh/2.0
  cc1=cmplx(cos(the),-sin(the))
  cc0=cmplx(cos(the),sin(the))

  candidate=0.0
  ncand=0
  fa=375.0
  fb=3000.0
  syncmin=0.2
  call getcandidates2a(iwave,fa,fb,100,savg,candidate,ncand)
  print '(a,i0)', 'ncand=', ncand

  do icand=1,ncand
     f0=candidate(1,icand)
     print '(a,f0.4)', 'candidate_f0=', f0
     if( f0.le.375.0 .or. f0.ge.(5000.0-375.0) ) cycle
     call ft2_downsample(iwave,f0,c2)
     ibest=-1
     sybest=-99.
     dfbest=-1.
     do ifreq=-30,30
        df=ifreq
        a=0.
        a(1)=-df
        call twkfreq1(c2,NMAX/16,fs,a,cb)
        do is=0,374
           csync1=0.
           cterm=1
           do ib=1,16
              i1=(ib-1)*10+is
              if(s16(ib).eq.1) then
                 csync1=csync1+sum(cb(i1:i1+9)*conjg(c1(0:9)))*cterm
                 cterm=cterm*cc1
              else
                 csync1=csync1+sum(cb(i1:i1+9)*conjg(c0(0:9)))*cterm
                 cterm=cterm*cc0
              endif
           enddo
           if(abs(csync1).gt.sybest) then
              ibest=is
              sybest=abs(csync1)
              dfbest=df
           endif
        enddo
     enddo
     print '(a,f0.4)', 'best_df=', dfbest
     print '(a,i0)', 'best_ibest=', ibest
     print '(a,f0.6)', 'best_sync=', sybest

     a=0.
     a(1)=-dfbest
     call twkfreq1(c2,NMAX/16,fs,a,cb)
     ib=ibest
     cd=cb(ib:ib+144*10-1)
     s2=sum(real(cd*conjg(cd)))/(10*144)
     cd=cd/sqrt(s2)
     do nseq=1,5
        if( nseq.eq.1 ) then
           sbits1=0.0
           do ibit=1,144
              ib=(ibit-1)*10
              ccor(1,ibit)=sum(cd(ib:ib+9)*conjg(c1(0:9)))
              ccor(0,ibit)=sum(cd(ib:ib+9)*conjg(c0(0:9)))
              sbits1(ibit)=abs(ccor(1,ibit))-abs(ccor(0,ibit))
              hbits1(ibit)=0
              if(sbits1(ibit).gt.0) hbits1(ibit)=1
           enddo
           sbits=sbits1
           hbits=hbits1
           sbits3=sbits1
           hbits3=hbits1
        else
           nbit=2*nseq-1
           numseq=2**(nbit)
           ps=0
           do ibit=nbit/2+1,144-nbit/2
              ps=0.0
              pmax=0.0
              do iseq=0,numseq-1
                 csum=0.0
                 cterm=1.0
                 k=1
                 do i=nbit-1,0,-1
                    ibb=iand(iseq/(2**i),1)
                    csum=csum+ccor(ibb,ibit-(nbit/2+1)+k)*cterm
                    if(ibb.eq.0) cterm=cterm*cc0
                    if(ibb.eq.1) cterm=cterm*cc1
                    k=k+1
                 enddo
                 ps(iseq)=abs(csum)
                 if( ps(iseq) .gt. pmax ) then
                    pmax=ps(iseq)
                    ibflag=1
                 endif
              enddo
              if( ibflag .eq. 1 ) then
                 psbest=ps
                 ibflag=0
              endif
              call getbitmetric(2**(nbit/2),psbest,numseq,sbits3(ibit))
              hbits3(ibit)=0
              if(sbits3(ibit).gt.0) hbits3(ibit)=1
           enddo
           sbits=sbits3
           hbits=hbits3
        endif
        nsync_qual=count(hbits(1:16).eq.s16)
        rxdata=sbits(17:144)
        rxav=sum(rxdata(1:128))/128.0
        rx2av=sum(rxdata(1:128)*rxdata(1:128))/128.0
        rxsig=sqrt(rx2av-rxav*rxav)
        sigma=0.80
        llr(1:128)=2*(rxdata/rxsig)/(sigma*sigma)
        apmask=0
        max_iterations=40
        nharderror=-1
        niterations=-1
        message=' '
        if(nsync_qual.ge.10) then
           do ibias=0,0
              llr2=llr
              call bpdecode128_90(llr2,apmask,max_iterations,message77,cw,nharderror,niterations)
              if(nharderror.ge.0) exit
           enddo
           if(nharderror.ge.0 .and. sum(message77).ne.0) then
              write(c77,'(77i1)') message77(1:77)
              call unpack77(c77,-1,message,unpk77_success)
           endif
        endif
        write(*,'(a,i0,a,i0,a,i0,a,a)') 'nseq=', nseq, ' sync_ok=', nsync_qual, &
             ' nharderror=', nharderror, ' decoded=', trim(message)
        write(*,'(a,i0)') 'iterations=', niterations
        write(*,'(a,f0.6)') 'mean=', rxav
        write(*,'(a,f0.6)') 'sigma=', rxsig
        write(*,'(a)', advance='no') 'llr_head='
        do i=1,8
           if (i.gt.1) write(*,'(a)', advance='no') ','
           write(*,'(f0.6)', advance='no') llr(i)
        enddo
        write(*,*)
        write(*,'(a)', advance='no') 'llrs='
        do i=1,128
           if (i.gt.1) write(*,'(a)', advance='no') ','
           write(*,'(f0.6)', advance='no') llr(i)
        enddo
        write(*,*)
        if(nsync_qual.lt.10) exit
        if(nharderror.ge.0 .and. sum(message77).ne.0) exit
     enddo
  enddo
end program ft2_ref_trace
"""


FFTW3_SHIM = r"""
module fftw3
  use, intrinsic :: iso_c_binding
  include 'fftw3.f03'
end module fftw3
"""


NORMALIZE_BMET = r"""
subroutine normalizebmet(bmet, n)
  real :: bmet(n)
  integer, intent(in) :: n
  real :: bmetav, bmet2av, var, bmetsig

  bmetav = sum(bmet) / real(n)
  bmet2av = sum(bmet*bmet) / real(n)
  var = bmet2av - bmetav*bmetav
  if (var .gt. 0.0) then
     bmetsig = sqrt(var)
  else
     bmetsig = sqrt(bmet2av)
  endif
  bmet = bmet / bmetsig
  return
end subroutine normalizebmet
"""


def run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def build_ft2_refs(source_root: Path, build_root: Path) -> tuple[Path, Path, Path, Path]:
    ft2_root = source_root / "lib" / "ft2"
    lib_root = source_root / "lib"
    build_root.mkdir(parents=True, exist_ok=True)

    gen_src = build_root / "ft2_ref_gen.f90"
    decode_src = build_root / "ft2_ref_decode.f90"
    frame_src = build_root / "ft2_ref_frame.f90"
    trace_src = build_root / "ft2_ref_trace.f90"
    fftw_shim_src = build_root / "fftw3_shim.f90"
    write_text(gen_src, FT2_GEN_WRAPPER)
    write_text(decode_src, FT2_DECODE_WRAPPER)
    write_text(frame_src, FT2_FRAME_WRAPPER)
    write_text(trace_src, FT2_TRACE_WRAPPER)
    write_text(fftw_shim_src, FFTW3_SHIM)

    common_cmd = [
        "gfortran",
        "-fno-second-underscore",
        "-fallow-argument-mismatch",
        "-std=legacy",
        "-g",
        "-fbacktrace",
        "-fcheck=all",
        "-fno-automatic",
    ]
    fftw_include = Path("/opt/homebrew/Cellar/fftw/3.3.10_2/include")
    fftw_lib = Path("/opt/homebrew/Cellar/fftw/3.3.10_2/lib")
    boost_include = Path("/opt/homebrew/opt/boost/include")
    module_dir = build_root / "mod"
    obj_dir = build_root / "obj"
    module_dir.mkdir(parents=True, exist_ok=True)
    obj_dir.mkdir(parents=True, exist_ok=True)
    include_flags = [
        "-J",
        str(module_dir),
        "-I",
        str(module_dir),
        "-I",
        str(lib_root),
        "-I",
        str(lib_root / "77bit"),
        "-I",
        str(ft2_root),
        "-I",
        str(fftw_include),
    ]

    ordered_sources = [
        fftw_shim_src,
        lib_root / "packjt.f90",
        lib_root / "77bit" / "packjt77.f90",
        lib_root / "crc.f90",
        lib_root / "wavhdr.f90",
        lib_root / "timer_module.f90",
        lib_root / "hashing.f90",
        lib_root / "hash.f90",
        lib_root / "chkcall.f90",
        lib_root / "deg2grid.f90",
        lib_root / "grid2deg.f90",
        lib_root / "fmtmsg.f90",
        lib_root / "encode_128_90.f90",
        lib_root / "encode_msk40.f90",
        lib_root / "genmsk40.f90",
        lib_root / "four2a.f90",
        lib_root / "db.f90",
        lib_root / "indexx.f90",
        lib_root / "platanh.f90",
        lib_root / "bpdecode128_90.f90",
        lib_root / "ft8" / "chkcrc13a.f90",
        lib_root / "ft8" / "twkfreq1.f90",
        ft2_root / "genft2.f90",
        ft2_root / "ft2_iwave.f90",
        ft2_root / "getcandidates2a.f90",
        ft2_root / "ft2_decode.f90",
    ]
    objects: list[str] = []
    for source in ordered_sources:
        obj = obj_dir / (source.stem + ".o")
        run(common_cmd + include_flags + ["-c", str(source), "-o", str(obj)])
        objects.append(str(obj))

    c_obj = obj_dir / "gran.o"
    run(["cc", "-c", str(lib_root / "gran.c"), "-o", str(c_obj)])
    objects.append(str(c_obj))
    nhash_obj = obj_dir / "nhash.o"
    run(["cc", "-c", str(lib_root / "wsprd" / "nhash.c"), "-o", str(nhash_obj)])
    objects.append(str(nhash_obj))
    cpp_obj = obj_dir / "crc13.o"
    run(
        [
            "c++",
            "-I",
            str(boost_include),
            "-c",
            str(lib_root / "crc13.cpp"),
            "-o",
            str(cpp_obj),
        ]
    )
    objects.append(str(cpp_obj))

    gen_bin = build_root / "ft2-ref-gen"
    decode_bin = build_root / "ft2-ref-decode"
    frame_bin = build_root / "ft2-ref-frame"
    trace_bin = build_root / "ft2-ref-trace"

    link_flags = [
        "-L",
        str(fftw_lib),
        "-lfftw3f_threads",
        "-lfftw3f",
        "-lfftw3_threads",
        "-lfftw3",
        "-lc++",
    ]
    run(common_cmd + include_flags + ["-o", str(gen_bin), str(gen_src)] + objects + link_flags)
    run(
        common_cmd + include_flags + ["-o", str(decode_bin), str(decode_src)] + objects + link_flags
    )
    run(common_cmd + include_flags + ["-o", str(frame_bin), str(frame_src)] + objects + link_flags)
    run(common_cmd + include_flags + ["-o", str(trace_bin), str(trace_src)] + objects + link_flags)
    return gen_bin, decode_bin, frame_bin, trace_bin


def build_ft4_refs(source_root: Path, build_root: Path) -> tuple[Path, Path, Path, Path, Path]:
    ft4_root = source_root / "lib" / "ft4"
    ft8_root = source_root / "lib" / "ft8"
    lib_root = source_root / "lib"
    build_root.mkdir(parents=True, exist_ok=True)

    gen_src = build_root / "ft4_ref_gen.f90"
    frame_src = build_root / "ft4_ref_frame.f90"
    debug_src = build_root / "ft4_stock_debug.f90"
    subtract_src = build_root / "ft4_stock_subtract.f90"
    trace_src = build_root / "ft4_stock_trace.f90"
    metrics_src = build_root / "ft4_stock_metrics.f90"
    normalize_src = build_root / "normalizebmet.f90"
    fftw_shim_src = build_root / "fftw3_shim.f90"
    write_text(gen_src, FT4_GEN_WRAPPER)
    write_text(frame_src, FT4_FRAME_WRAPPER)
    write_text(debug_src, FT4_DEBUG_WRAPPER)
    write_text(subtract_src, FT4_SUBTRACT_WRAPPER)
    write_text(trace_src, FT4_TRACE_WRAPPER)
    write_text(metrics_src, FT4_METRICS_WRAPPER)
    write_text(normalize_src, NORMALIZE_BMET)
    write_text(fftw_shim_src, FFTW3_SHIM)

    common_cmd = [
        "gfortran",
        "-fno-second-underscore",
        "-fallow-argument-mismatch",
        "-std=legacy",
    ]
    boost_include = Path("/opt/homebrew/opt/boost/include")
    fftw_include = Path("/opt/homebrew/Cellar/fftw/3.3.10_2/include")
    fftw_lib = Path("/opt/homebrew/Cellar/fftw/3.3.10_2/lib")
    obj_dir = build_root / "obj"
    mod_dir = build_root / "mod"
    obj_dir.mkdir(parents=True, exist_ok=True)
    mod_dir.mkdir(parents=True, exist_ok=True)
    include_flags = [
        "-J",
        str(mod_dir),
        "-I",
        str(mod_dir),
        "-I",
        str(lib_root),
        "-I",
        str(lib_root / "77bit"),
        "-I",
        str(ft4_root),
        "-I",
        str(ft8_root),
        "-I",
        str(fftw_include),
    ]

    ordered_sources = [
        fftw_shim_src,
        lib_root / "packjt.f90",
        lib_root / "77bit" / "packjt77.f90",
        lib_root / "crc.f90",
        lib_root / "wavhdr.f90",
        lib_root / "timer_module.f90",
        lib_root / "hashing.f90",
        lib_root / "hash.f90",
        lib_root / "chkcall.f90",
        lib_root / "deg2grid.f90",
        lib_root / "grid2deg.f90",
        lib_root / "fmtmsg.f90",
        lib_root / "four2a.f90",
        lib_root / "indexx.f90",
        lib_root / "platanh.f90",
        ft8_root / "encode174_91.f90",
        ft8_root / "encode174_91_nocrc.f90",
        ft8_root / "chkcrc14a.f90",
        ft8_root / "get_crc14.f90",
        ft8_root / "bpdecode174_91.f90",
        ft8_root / "osd174_91.f90",
        ft8_root / "decode174_91.f90",
        ft4_root / "genft4.f90",
        ft4_root / "gen_ft4wave.f90",
        ft4_root / "getcandidates4.f90",
        ft4_root / "ft4_downsample.f90",
        ft4_root / "sync4d.f90",
        ft4_root / "get_ft4_bitmetrics.f90",
        ft4_root / "ft4_baseline.f90",
        ft4_root / "subtractft4.f90",
        normalize_src,
        lib_root / "nuttal_window.f90",
        lib_root / "pctile.f90",
        lib_root / "polyfit.f90",
        lib_root / "shell.f90",
        lib_root / "determ.f90",
        ft8_root / "twkfreq1.f90",
        lib_root / "ft2" / "gfsk_pulse.f90",
    ]
    objects: list[str] = []
    for source in ordered_sources:
        obj = obj_dir / (source.stem + ".o")
        run(common_cmd + include_flags + ["-c", str(source), "-o", str(obj)])
        objects.append(str(obj))

    nhash_obj = obj_dir / "nhash.o"
    run(["cc", "-c", str(lib_root / "wsprd" / "nhash.c"), "-o", str(nhash_obj)])
    objects.append(str(nhash_obj))
    crc14_obj = obj_dir / "crc14.o"
    run(
        [
            "c++",
            "-I",
            str(boost_include),
            "-c",
            str(lib_root / "crc14.cpp"),
            "-o",
            str(crc14_obj),
        ]
    )
    objects.append(str(crc14_obj))

    gen_bin = build_root / "ft4-ref-gen"
    frame_bin = build_root / "ft4-ref-frame"
    debug_bin = build_root / "ft4-stock-debug"
    subtract_bin = build_root / "ft4-stock-subtract"
    trace_bin = build_root / "ft4-stock-trace"
    metrics_bin = build_root / "ft4-stock-metrics"
    link_flags = [
        "-L",
        str(fftw_lib),
        "-lfftw3f_threads",
        "-lfftw3f",
        "-lfftw3_threads",
        "-lfftw3",
        "-lc++",
    ]
    run(common_cmd + include_flags + ["-o", str(gen_bin), str(gen_src)] + objects + link_flags)
    run(common_cmd + include_flags + ["-o", str(frame_bin), str(frame_src)] + objects + link_flags)
    run(common_cmd + include_flags + ["-o", str(debug_bin), str(debug_src)] + objects + link_flags)
    run(
        common_cmd + include_flags + ["-o", str(subtract_bin), str(subtract_src)] + objects + link_flags
    )
    run(common_cmd + include_flags + ["-o", str(trace_bin), str(trace_src)] + objects + link_flags)
    run(common_cmd + include_flags + ["-o", str(metrics_bin), str(metrics_src)] + objects + link_flags)
    return gen_bin, frame_bin, debug_bin, subtract_bin, trace_bin, metrics_bin


def main() -> int:
    parser = argparse.ArgumentParser(description="Build transient mode reference helpers")
    parser.add_argument(
        "--wsjtx-root",
        default="../wsjtx",
        help="Path to a local WSJT-X / wsjt-x_improved source tree.",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory for built helpers. Defaults to a new temp directory.",
    )
    args = parser.parse_args()

    source_root = Path(args.wsjtx_root).resolve()
    if not source_root.exists():
        raise SystemExit(f"missing source tree: {source_root}")

    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="mode-refs-"))

    ft2_gen, ft2_decode, ft2_frame, ft2_trace = build_ft2_refs(source_root, output_dir / "ft2")
    ft4_gen, ft4_frame, ft4_debug, ft4_subtract, ft4_trace, ft4_metrics = build_ft4_refs(source_root, output_dir / "ft4")
    print(f"output_dir={output_dir}")
    print(f"ft4_ref_gen={ft4_gen}")
    print(f"ft4_ref_frame={ft4_frame}")
    print(f"ft4_stock_debug={ft4_debug}")
    print(f"ft4_stock_subtract={ft4_subtract}")
    print(f"ft4_stock_trace={ft4_trace}")
    print(f"ft4_stock_metrics={ft4_metrics}")
    print(f"ft2_ref_gen={ft2_gen}")
    print(f"ft2_ref_decode={ft2_decode}")
    print(f"ft2_ref_frame={ft2_frame}")
    print(f"ft2_ref_trace={ft2_trace}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

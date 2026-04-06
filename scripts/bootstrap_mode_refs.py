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
  character(len=256) :: wav_path
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

  call get_command_argument(1, wav_path)
  open(10, file=trim(wav_path), status='old', access='stream', iostat=ios)
  if (ios .ne. 0) stop 1
  read(10) h
  iwave = 0
  nsamp = min(h%ndata / 2, size(iwave))
  read(10) iwave(1:nsamp)
  close(10)
  call ft2_decode(stamp, nfqso, iwave, ndecodes, mycall, hiscall, nrx, line)
  if (ndecodes .ge. 1) then
     print '(a)', trim(line)
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


FFTW3_SHIM = r"""
module fftw3
  use, intrinsic :: iso_c_binding
  include 'fftw3.f03'
end module fftw3
"""


def run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def build_ft2_refs(source_root: Path, build_root: Path) -> tuple[Path, Path, Path]:
    ft2_root = source_root / "lib" / "ft2"
    lib_root = source_root / "lib"
    build_root.mkdir(parents=True, exist_ok=True)

    gen_src = build_root / "ft2_ref_gen.f90"
    decode_src = build_root / "ft2_ref_decode.f90"
    frame_src = build_root / "ft2_ref_frame.f90"
    fftw_shim_src = build_root / "fftw3_shim.f90"
    write_text(gen_src, FT2_GEN_WRAPPER)
    write_text(decode_src, FT2_DECODE_WRAPPER)
    write_text(frame_src, FT2_FRAME_WRAPPER)
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
    return gen_bin, decode_bin, frame_bin


def build_ft4_refs(source_root: Path, build_root: Path) -> tuple[Path, Path]:
    ft4_root = source_root / "lib" / "ft4"
    ft8_root = source_root / "lib" / "ft8"
    lib_root = source_root / "lib"
    build_root.mkdir(parents=True, exist_ok=True)

    gen_src = build_root / "ft4_ref_gen.f90"
    frame_src = build_root / "ft4_ref_frame.f90"
    write_text(gen_src, FT4_GEN_WRAPPER)
    write_text(frame_src, FT4_FRAME_WRAPPER)

    common_cmd = [
        "gfortran",
        "-fno-second-underscore",
        "-fallow-argument-mismatch",
        "-std=legacy",
    ]
    boost_include = Path("/opt/homebrew/opt/boost/include")
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
    ]

    ordered_sources = [
        lib_root / "packjt.f90",
        lib_root / "77bit" / "packjt77.f90",
        lib_root / "crc.f90",
        lib_root / "wavhdr.f90",
        lib_root / "hashing.f90",
        lib_root / "hash.f90",
        lib_root / "chkcall.f90",
        lib_root / "deg2grid.f90",
        lib_root / "grid2deg.f90",
        lib_root / "fmtmsg.f90",
        ft8_root / "encode174_91.f90",
        ft4_root / "genft4.f90",
        ft4_root / "gen_ft4wave.f90",
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
    link_flags = ["-lc++"]
    run(common_cmd + include_flags + ["-o", str(gen_bin), str(gen_src)] + objects + link_flags)
    run(common_cmd + include_flags + ["-o", str(frame_bin), str(frame_src)] + objects + link_flags)
    return gen_bin, frame_bin


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

    ft2_gen, ft2_decode, ft2_frame = build_ft2_refs(source_root, output_dir / "ft2")
    ft4_gen, ft4_frame = build_ft4_refs(source_root, output_dir / "ft4")
    print(f"output_dir={output_dir}")
    print(f"ft4_ref_gen={ft4_gen}")
    print(f"ft4_ref_frame={ft4_frame}")
    print(f"ft2_ref_gen={ft2_gen}")
    print(f"ft2_ref_decode={ft2_decode}")
    print(f"ft2_ref_frame={ft2_frame}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

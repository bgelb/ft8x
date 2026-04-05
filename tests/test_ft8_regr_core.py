from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ft8_regr.core import describe_release_install, ensure_linux_compat_lib_dirs, select_host_artifact


class RegressionCoreTests(unittest.TestCase):
    def test_selects_linux_amd64_deb_for_linux_host(self) -> None:
        release_files = {
            "deb": {
                "name": "wsjtx_2.7.0_amd64.deb",
                "downloadable": True,
                "download_url": "https://example.invalid/wsjtx_2.7.0_amd64.deb",
            },
            "rpm": {
                "name": "wsjtx-2.7.0.x86_64.rpm",
                "downloadable": True,
                "download_url": "https://example.invalid/wsjtx-2.7.0.x86_64.rpm",
            },
            "dmg": {
                "name": "wsjtx-2.7.0-Darwin.dmg",
                "downloadable": True,
                "download_url": "https://example.invalid/wsjtx-2.7.0-Darwin.dmg",
            },
        }
        with (
            patch("platform.system", return_value="Linux"),
            patch("platform.machine", return_value="x86_64"),
        ):
            artifact = select_host_artifact(release_files)
        self.assertIsNotNone(artifact)
        self.assertEqual(artifact["name"], "wsjtx_2.7.0_amd64.deb")
        self.assertEqual(artifact["kind"], "deb")

    def test_describe_linux_install_uses_usr_bin_jt9(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            jt9_path = root / "usr" / "bin" / "jt9"
            jt9_path.parent.mkdir(parents=True, exist_ok=True)
            jt9_path.write_bytes(b"\x7fELFfake")
            paths = patch_default_paths(Path(tmpdir))
            with patch("ft8_regr.core.missing_shared_libraries", return_value=[]):
                metadata = describe_release_install(paths, root, "linux")
        self.assertEqual(metadata["platform"], "linux")
        self.assertTrue(metadata["jt9_path"].endswith("/usr/bin/jt9"))
        self.assertTrue(metadata["executable_path"].endswith("/usr/bin"))

    def test_describe_macos_install_uses_app_bundle_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = Path(tmpdir) / "wsjtx.app"
            jt9_path = app / "Contents" / "MacOS" / "jt9"
            jt9_path.parent.mkdir(parents=True, exist_ok=True)
            (app / "Contents" / "Resources").mkdir(parents=True, exist_ok=True)
            jt9_path.write_bytes(b"\x7fELFfake")
            paths = patch_default_paths(Path(tmpdir))
            metadata = describe_release_install(paths, app, "macos")
        self.assertEqual(metadata["platform"], "macos")
        self.assertTrue(metadata["jt9_path"].endswith("/Contents/MacOS/jt9"))
        self.assertTrue(metadata["executable_path"].endswith("/Contents/MacOS"))
        self.assertTrue(metadata["data_path"].endswith("/Contents/Resources"))
        self.assertEqual(metadata["compat_library_dirs"], [])
        self.assertEqual(metadata["missing_shared_libraries"], [])
        self.assertTrue(metadata["runnable"])

    def test_linux_compat_lib_dirs_include_vendored_libgfortran4(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            compat_root = Path(tmpdir) / "artifacts" / "cache" / "linux-runtime" / "libgfortran4-ubuntu18.04-amd64" / "usr" / "lib" / "x86_64-linux-gnu"
            compat_root.mkdir(parents=True, exist_ok=True)
            (compat_root / "libgfortran.so.4").write_text("stub")
            paths = patch_default_paths(Path(tmpdir))
            compat_dirs = ensure_linux_compat_lib_dirs(paths, ["libgfortran.so.4"])
        self.assertEqual([path.resolve() for path in compat_dirs], [compat_root.resolve()])

    def test_selects_macos_dmg_for_macos_host(self) -> None:
        release_files = {
            "deb": {
                "name": "wsjtx_2.7.0_amd64.deb",
                "downloadable": True,
                "download_url": "https://example.invalid/wsjtx_2.7.0_amd64.deb",
            },
            "dmg": {
                "name": "wsjtx-2.7.0-Darwin.dmg",
                "downloadable": True,
                "download_url": "https://example.invalid/wsjtx-2.7.0-Darwin.dmg",
            },
        }
        with (
            patch("platform.system", return_value="Darwin"),
            patch("platform.machine", return_value="arm64"),
        ):
            artifact = select_host_artifact(release_files)
        self.assertIsNotNone(artifact)
        self.assertEqual(artifact["name"], "wsjtx-2.7.0-Darwin.dmg")
        self.assertEqual(artifact["kind"], "dmg")


def patch_default_paths(root: Path):
    from ft8_regr.core import default_paths

    return default_paths(root)


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from resolver_app_ui import HTML, STATIC_DIR  # noqa: E402


class StaticUiAssetTests(unittest.TestCase):
    def test_index_references_external_static_assets(self) -> None:
        self.assertIn('/static/app.css', HTML)
        self.assertIn('/static/app.js', HTML)
        self.assertNotIn('<style>', HTML)
        self.assertNotIn('<script>', HTML)

    def test_static_asset_files_exist(self) -> None:
        self.assertTrue((STATIC_DIR / "index.html").is_file())
        self.assertTrue((STATIC_DIR / "app.css").is_file())
        self.assertTrue((STATIC_DIR / "app.js").is_file())


if __name__ == "__main__":
    unittest.main()

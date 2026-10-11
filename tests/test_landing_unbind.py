import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location("landing_unbind", Path(__file__).resolve().parents[1] / "scripts/unbind_unused_dev_landing.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MigrationTests(unittest.TestCase):
    def run_case(self, fail):
        test_root = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory(dir=test_root) as directory:
            bundle = Path(directory)
            self.assertTrue(bundle.resolve().is_relative_to(test_root))
            (bundle / 'resources').mkdir()
            original = b'bundle:\r\n  name: supplier_performance\r\ninclude:\r\n  - resources/schema.yml\r\n'
            (bundle / 'databricks.yml').write_bytes(original)
            calls = []

            def runner(command, cwd, check):
                calls.append(command)
                self.assertEqual(cwd, bundle)
                self.assertTrue(check)
                self.assertIn('_unbind_unused_landing.yml', (bundle / 'databricks.yml').read_text())
                self.assertTrue((bundle / 'resources/_unbind_unused_landing.yml').exists())
                if fail:
                    raise subprocess.CalledProcessError(1, command)

            if fail:
                with self.assertRaises(subprocess.CalledProcessError):
                    module.unbind(bundle, runner)
            else:
                module.unbind(bundle, runner)
            self.assertEqual((bundle / 'databricks.yml').read_bytes(), original)
            self.assertFalse((bundle / 'resources/_unbind_unused_landing.yml').exists())
            self.assertEqual([c[4] for c in calls], ['landing_files'] if fail else ['landing_files', 'landing_schema'])
            self.assertTrue(all(c[-2:] == ['-t', 'dev'] and c[3] == 'unbind' for c in calls))

    def test_restores_config_after_success(self):
        self.run_case(False)

    def test_restores_config_and_stops_on_error(self):
        self.run_case(True)


if __name__ == '__main__':
    unittest.main()

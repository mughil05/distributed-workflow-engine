import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class DemoProjectOwnershipTests(unittest.TestCase):
    def run_helper(self, *args):
        return subprocess.run([sys.executable, "scripts/demo_project.py", *args],
                              capture_output=True, text=True)

    def test_each_demo_gets_a_unique_owned_compose_project(self):
        first = self.run_helper("new")
        second = self.run_helper("new")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertNotEqual(first.stdout.strip(), second.stdout.strip())
        for project in (first.stdout.strip(), second.stdout.strip()):
            validated = self.run_helper("validate", project)
            self.assertEqual(validated.returncode, 0, validated.stderr)
            self.assertEqual(validated.stdout.strip(), project)

    def test_teardown_rejects_unowned_or_legacy_project_names(self):
        for project in ("wf005-demo-123", "other-project", "wf005-demo-000000000000;rm"):
            with self.subTest(project=project):
                self.assertNotEqual(self.run_helper("validate", project).returncode, 0)

    def test_teardown_targets_only_the_project_recorded_in_demo_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            shutil.copy("scripts/demo-down.sh", scripts / "demo-down.sh")
            shutil.copy("scripts/demo_project.py", scripts / "demo_project.py")
            bin_dir = root / "bin"
            bin_dir.mkdir()
            log_path = root / "docker-args"
            docker = bin_dir / "docker"
            docker.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DOCKER_ARGS_LOG\"\n")
            docker.chmod(0o755)
            project = "wf005-demo-012345abcdef"
            (root / ".env.demo").write_text(f"WORKFLOW_DEMO_PROJECT={project}\n")
            env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
                   "DOCKER_ARGS_LOG": str(log_path)}
            result = subprocess.run(["bash", str(scripts / "demo-down.sh")],
                                    cwd=root, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            args = log_path.read_text().splitlines()
            self.assertEqual(args[args.index("--project-name") + 1], project)
            self.assertIn("down", args)
            self.assertIn("--volumes", args)
            self.assertFalse((root / ".env.demo").exists())

            (root / ".env.demo").write_text("WORKFLOW_DEMO_PROJECT=unrelated-project\n")
            log_path.unlink()
            result = subprocess.run(["bash", str(scripts / "demo-down.sh")],
                                    cwd=root, env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(log_path.exists(), "invalid ownership must fail before Docker is called")
            self.assertTrue((root / ".env.demo").exists(), "failed validation must preserve ownership record")


if __name__ == "__main__":
    unittest.main()

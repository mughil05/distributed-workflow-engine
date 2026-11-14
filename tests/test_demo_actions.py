import unittest
from datetime import datetime, timezone

from workflow_engine.actions import default_action_registry
from workflow_engine.demo_actions import register_actions


class DemoActionTests(unittest.TestCase):
    def test_bounded_demo_actions_are_typed_and_registered(self):
        registry = default_action_registry()
        register_actions(registry)
        retry_at = datetime.now(timezone.utc).isoformat()
        self.assertIn("demo.retry_until.v1", registry.names)
        self.assertIsNotNone(registry.validate("demo.retry_until.v1", {"until": retry_at}))
        with self.assertRaises(Exception):
            registry.validate("demo.delay.v1", {"seconds": 1000})
        with self.assertRaises(Exception):
            registry.validate("demo.delay.v1", {"seconds": 0, "command": "bad"})


if __name__ == "__main__":
    unittest.main()

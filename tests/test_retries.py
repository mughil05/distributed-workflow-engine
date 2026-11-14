import math
import unittest

from workflow_engine import PermanentTaskError, RetryPolicy, Task


class RetryPolicyTests(unittest.TestCase):
    def test_policy_validation(self):
        for kwargs in ({"max_attempts": 0}, {"max_attempts": True},
                       {"initial_delay": -1}, {"initial_delay": math.inf},
                       {"multiplier": 0.5}, {"max_delay": -1}, {"jitter": 1.1},
                       {"jitter": math.nan}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                RetryPolicy(**kwargs)

    def test_exponential_backoff_is_capped(self):
        policy = RetryPolicy(max_attempts=6, initial_delay=2, multiplier=3, max_delay=10)
        self.assertEqual([policy.delay_for(n) for n in range(1, 6)], [2, 6, 10, 10, 10])

    def test_jitter_is_deterministic_with_injected_random_source(self):
        policy = RetryPolicy(max_attempts=4, initial_delay=8, max_delay=10, jitter=1)
        self.assertEqual(policy.delay_for(2, lambda: 0.25), 2.5)
        self.assertEqual(policy.delay_for(3, lambda: 1.0), 10)
        self.assertEqual(policy.delay_for(3, lambda: 0.0), 0)

    def test_extreme_policy_does_not_overflow_before_cap(self):
        policy = RetryPolicy(max_attempts=1000000, initial_delay=1e-300,
                             multiplier=1e300, max_delay=1e300)
        self.assertEqual(policy.delay_for(1000000), 1e300)

    def test_task_rejects_invalid_policy_type(self):
        with self.assertRaises(TypeError):
            Task("invalid", retry_policy=None)

    def test_permanent_failure_marker_is_an_exception(self):
        self.assertTrue(issubclass(PermanentTaskError, Exception))


if __name__ == "__main__":
    unittest.main()

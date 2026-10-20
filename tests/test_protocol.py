import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from protocol import topic_match


class TopicMatch(unittest.TestCase):
    def test_exact(self):
        self.assertTrue(topic_match("a/b", "a/b"))
        self.assertFalse(topic_match("a/b", "a/c"))

    def test_plus(self):
        self.assertTrue(topic_match("sensor/+/temp", "sensor/room1/temp"))
        self.assertFalse(topic_match("sensor/+/temp", "sensor/room1/humidity"))
        self.assertFalse(topic_match("sensor/+/temp", "sensor/a/b/temp"))

    def test_hash(self):
        self.assertTrue(topic_match("sensor/#", "sensor/room1/temp"))
        self.assertTrue(topic_match("#", "anything/at/all"))
        self.assertFalse(topic_match("sensor/#", "other/x"))

    def test_length_mismatch(self):
        self.assertFalse(topic_match("a/b/c", "a/b"))
        self.assertFalse(topic_match("a/b", "a/b/c"))


if __name__ == "__main__":
    unittest.main()

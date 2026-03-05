import copy

class VectorClock:
    def __init__(self, clock=None):
        self.clock = clock if clock else {}

    def increment(self, node_id):
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def compare(self, other):
        """
        Compares this vector clock with another.
        Returns:
            -1 if self < other (self is stale)
             1 if self > other (self is newer)
             0 if self == other
            None if concurrent (conflict)
        """
        if not isinstance(other, VectorClock):
            other = VectorClock(other)
            
        self_greater = False
        other_greater = False

        all_keys = set(self.clock.keys()).union(set(other.clock.keys()))

        for key in all_keys:
            v1 = self.clock.get(key, 0)
            v2 = other.clock.get(key, 0)
            if v1 > v2:
                self_greater = True
            elif v1 < v2:
                other_greater = True

        if self_greater and not other_greater:
            return 1
        if other_greater and not self_greater:
            return -1
        if not self_greater and not other_greater:
            return 0
        return None  # Concurrent

    def merge(self, other):
        if not isinstance(other, VectorClock):
            other = VectorClock(other)
            
        all_keys = set(self.clock.keys()).union(set(other.clock.keys()))
        new_clock = {}
        for key in all_keys:
            new_clock[key] = max(self.clock.get(key, 0), other.clock.get(key, 0))
        return VectorClock(new_clock)

    def to_dict(self):
        return copy.deepcopy(self.clock)

    def __str__(self):
        return str(self.clock)

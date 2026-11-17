"""Streaming algorithms (CS246 stream-mining): Count-Min Sketch, Bloom filter, DGIM,
Misra-Gries and reservoir sampling.  Pure NumPy/Python, mergeable so they can be built per
Spark partition (``mapPartitions``) and combined with ``reduce(merge)``.
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterable

import numpy as np

P = (1 << 31) - 1  # Mersenne prime; a*x stays inside int64 for a, x < P


def _hash_params(n: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    return rng.integers(1, P, n, dtype=np.int64), rng.integers(0, P, n, dtype=np.int64)


class CountMinSketch:
    """Count-Min Sketch: estimate(x) >= true(x) and <= true(x) + eps*N w.p. >= 1 - delta."""

    def __init__(self, width: int, depth: int, seed: int = 0) -> None:
        self.width, self.depth, self.seed = width, depth, seed
        self.a, self.b = _hash_params(depth, seed)
        self.table = np.zeros((depth, width), dtype=np.int64)
        self.n = 0

    @classmethod
    def from_error(cls, eps: float, delta: float, seed: int = 0) -> CountMinSketch:
        return cls(math.ceil(math.e / eps), math.ceil(math.log(1.0 / delta)), seed)

    def _idx(self, keys: np.ndarray) -> np.ndarray:
        k = np.asarray(keys, dtype=np.int64) % P
        return ((self.a[:, None] * k[None, :] + self.b[:, None]) % P) % self.width

    def update_many(self, keys, counts=None) -> None:
        keys = np.asarray(keys, dtype=np.int64)
        if keys.size == 0:
            return
        w = None if counts is None else np.asarray(counts, dtype=np.float64)
        idx = self._idx(keys)
        for i in range(self.depth):
            self.table[i] += np.bincount(idx[i], weights=w, minlength=self.width).astype(np.int64)
        self.n += int(keys.size if w is None else w.sum())

    def estimate_many(self, keys) -> np.ndarray:
        idx = self._idx(np.asarray(keys, dtype=np.int64))
        return self.table[np.arange(self.depth)[:, None], idx].min(axis=0)

    def estimate(self, key: int) -> int:
        return int(self.estimate_many([key])[0])

    def merge(self, other: CountMinSketch) -> CountMinSketch:
        if (self.width, self.depth, self.seed) != (other.width, other.depth, other.seed):
            raise ValueError("cannot merge sketches with different shape/seed")
        self.table += other.table
        self.n += other.n
        return self


class HeavyHitters:
    """Streaming top-k heavy hitters on top of a Count-Min Sketch (candidate set = recent keys)."""

    def __init__(self, k: int, cms: CountMinSketch) -> None:
        self.k, self.cms, self.cands = k, cms, np.empty(0, dtype=np.int64)

    def update(self, chunk) -> None:
        chunk = np.asarray(chunk, dtype=np.int64)
        self.cms.update_many(chunk)
        pool = np.union1d(self.cands, np.unique(chunk))
        est = self.cms.estimate_many(pool)
        self.cands = pool[np.argsort(-est, kind="stable")[: self.k]]

    def top(self) -> list[tuple[int, int]]:
        est = self.cms.estimate_many(self.cands)
        order = np.argsort(-est, kind="stable")
        return [(int(self.cands[i]), int(est[i])) for i in order]


class BloomFilter:
    """Bit-packed Bloom filter with double hashing; no false negatives."""

    def __init__(self, n_bits: int, n_hashes: int, seed: int = 0) -> None:
        if n_bits >= P:
            raise ValueError("n_bits must be < 2^31-1")
        self.m, self.k, self.seed = n_bits, n_hashes, seed
        self.a, self.b = _hash_params(2, seed)
        self.bits = np.zeros((n_bits + 7) // 8, dtype=np.uint8)

    @staticmethod
    def optimal_params(n_items: int, fpr: float) -> tuple[int, int]:
        m = math.ceil(-n_items * math.log(fpr) / (math.log(2) ** 2))
        k = max(1, round(m / n_items * math.log(2)))
        return m, k

    def _positions(self, keys) -> np.ndarray:
        x = np.asarray(keys, dtype=np.int64) % P
        h1 = (self.a[0] * x + self.b[0]) % P
        h2 = ((self.a[1] * x + self.b[1]) % P) | 1
        i = np.arange(self.k, dtype=np.int64)[:, None]
        return (h1[None, :] + i * h2[None, :]) % self.m  # shape (k, n)

    def add_many(self, keys) -> None:
        pos = self._positions(keys).ravel()
        np.bitwise_or.at(self.bits, pos >> 3, (1 << (pos & 7)).astype(np.uint8))

    def contains_many(self, keys) -> np.ndarray:
        pos = self._positions(keys)
        hit = (self.bits[pos >> 3] >> (pos & 7).astype(np.uint8)) & 1
        return hit.all(axis=0)

    def merge(self, other: BloomFilter) -> BloomFilter:
        if (self.m, self.k, self.seed) != (other.m, other.k, other.seed):
            raise ValueError("cannot merge filters with different parameters")
        self.bits |= other.bits
        return self

    def expected_fpr(self, n_items: int) -> float:
        return (1 - math.exp(-self.k * n_items / self.m)) ** self.k


class DGIM:
    """DGIM: approximate number of 1s among the last ``window`` bits using O(log^2 N) space.

    Relative error is at most 50% (with <=2 buckets per size).
    """

    def __init__(self, window: int) -> None:
        self.window, self.t = window, 0
        self.buckets: list[list[int]] = []  # [end_timestamp, size], newest first

    def update(self, bit: int) -> None:
        self.t += 1
        while self.buckets and self.buckets[-1][0] <= self.t - self.window:
            self.buckets.pop()
        if not bit:
            return
        self.buckets.insert(0, [self.t, 1])
        size = 1
        while True:
            same = [j for j, bk in enumerate(self.buckets) if bk[1] == size]
            if len(same) <= 2:
                break
            newer, older = same[-2], same[-1]
            self.buckets[newer][1] = size * 2  # keeps the newer end timestamp
            del self.buckets[older]
            size *= 2

    def count(self) -> float:
        if not self.buckets:
            return 0.0
        return float(sum(b[1] for b in self.buckets[:-1]) + self.buckets[-1][1] / 2)


class MisraGries:
    """Deterministic heavy-hitter summary with k-1 counters (underestimates by <= N/k)."""

    def __init__(self, k: int) -> None:
        self.k, self.counters = k, {}

    def update(self, x) -> None:
        c = self.counters
        if x in c:
            c[x] += 1
        elif len(c) < self.k - 1:
            c[x] = 1
        else:
            for key in list(c):
                c[key] -= 1
                if c[key] == 0:
                    del c[key]


def reservoir_sample(stream: Iterable, k: int, seed: int = 0) -> list:
    """Uniform sample of size k from a stream of unknown length (Algorithm R)."""
    rnd, res = random.Random(seed), []
    for i, x in enumerate(stream):
        if i < k:
            res.append(x)
        else:
            j = rnd.randint(0, i)
            if j < k:
                res[j] = x
    return res

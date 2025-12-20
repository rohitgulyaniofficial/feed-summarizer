#!/usr/bin/env python3
"""Shared clustering helpers.

This module centralizes the clustering logic used by both runtime merging
(publisher) and diagnostic reporting (tools/merge_report.py).

It supports:
- single-linkage clustering via union-find
- complete-linkage clustering via greedy agglomeration

All clustering is based on index pairs (0..n-1) and callables for distance and
per-pair threshold.
"""

from __future__ import annotations

from typing import Callable, List, Optional


GetInt = Callable[[int, int], Optional[int]]
KeyFn = Callable[[int], int]


def cluster_indices(
    n: int,
    linkage: str,
    get_dist: GetInt,
    get_thr: GetInt,
    *,
    leader_key: Optional[KeyFn] = None,
) -> List[List[int]]:
    """Cluster indices under single- or complete-linkage.

    Args:
        n: Number of items.
        linkage: "single" or "complete".
        get_dist: Callable returning distance for (i,j) or None if unknown/ineligible.
        get_thr: Callable returning threshold for (i,j) or None if unknown.
        leader_key: Optional ordering key used for stable leaders in union-find.

    Returns:
        List of clusters as lists of indices, filtered to clusters with size >= 2.
    """
    if n < 2:
        return []

    linkage_norm = (linkage or "complete").strip().lower()
    if linkage_norm not in {"single", "complete"}:
        linkage_norm = "complete"

    key = leader_key if leader_key is not None else (lambda idx: idx)

    def can_link(i: int, j: int) -> bool:
        d = get_dist(i, j)
        if d is None:
            return False
        t = get_thr(i, j)
        if t is None:
            return False
        return int(d) <= int(t)

    if linkage_norm == "single":
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            if key(ra) <= key(rb):
                parent[rb] = ra
            else:
                parent[ra] = rb

        for i in range(n):
            for j in range(i + 1, n):
                if can_link(i, j):
                    union(i, j)

        groups: dict[int, List[int]] = {}
        for i in range(n):
            root = find(i)
            groups.setdefault(root, []).append(i)

        out = [sorted(g) for g in groups.values() if len(g) > 1]
        out.sort(key=lambda g: (key(g[0]), g[0]))
        return out

    clusters: List[List[int]] = [[i] for i in range(n)]

    def can_merge(ci: List[int], cj: List[int]) -> Optional[int]:
        max_d = 0
        for i in ci:
            for j in cj:
                d = get_dist(i, j)
                if d is None:
                    return None
                t = get_thr(i, j)
                if t is None or int(d) > int(t):
                    return None
                if int(d) > max_d:
                    max_d = int(d)
        return max_d

    while True:
        best: Optional[tuple[int, int, int, int]] = None  # (max_d, min_key, a_idx, b_idx)
        for a_idx in range(len(clusters)):
            for b_idx in range(a_idx + 1, len(clusters)):
                merge_d = can_merge(clusters[a_idx], clusters[b_idx])
                if merge_d is None:
                    continue
                min_key = min(key(clusters[a_idx][0]), key(clusters[b_idx][0]))
                cand = (int(merge_d), int(min_key), int(a_idx), int(b_idx))
                if best is None or cand < best:
                    best = cand
        if best is None:
            break
        _, _, a_idx, b_idx = best
        merged = sorted(clusters[a_idx] + clusters[b_idx])
        clusters[a_idx] = merged
        del clusters[b_idx]

    out = [c for c in clusters if len(c) > 1]
    out.sort(key=lambda g: (key(g[0]), g[0]))
    return out

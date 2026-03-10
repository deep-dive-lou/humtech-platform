"""
Optimisation Engine — Taguchi Orthogonal Array Library

Pure math functions for fractional factorial experiment design.
No web dependencies. Generates orthogonal arrays and selects the smallest
array that covers a given factor/level configuration.

References:
- Taguchi, G. (1986). Introduction to Quality Engineering. Asian Productivity Organization.
- Jiang et al. (2018). A Comparison of the Taguchi Method and Evolutionary Optimization
  in Multivariate Testing. arXiv:1808.08347.
"""

from __future__ import annotations

import numpy as np


# ---------------------------------------------------------------------------
# Standard Orthogonal Arrays
# ---------------------------------------------------------------------------
# Each entry: (name, n_runs, max_factors, n_levels, array)
# Arrays are stored as lists-of-lists: rows = runs, columns = factor assignments.
# Values are 0-indexed level indices.
#
# These are the most commonly used Taguchi arrays for 2- and 3-level factors.
# ---------------------------------------------------------------------------

# L4: up to 3 factors, 2 levels each, 4 runs
_L4 = np.array([
    [0, 0, 0],
    [0, 1, 1],
    [1, 0, 1],
    [1, 1, 0],
], dtype=np.int8)

# L8: up to 7 factors, 2 levels each, 8 runs
_L8 = np.array([
    [0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 1, 1, 1, 1],
    [0, 1, 1, 0, 0, 1, 1],
    [0, 1, 1, 1, 1, 0, 0],
    [1, 0, 1, 0, 1, 0, 1],
    [1, 0, 1, 1, 0, 1, 0],
    [1, 1, 0, 0, 1, 1, 0],
    [1, 1, 0, 1, 0, 0, 1],
], dtype=np.int8)

# L9: up to 4 factors, 3 levels each, 9 runs
_L9 = np.array([
    [0, 0, 0, 0],
    [0, 1, 1, 1],
    [0, 2, 2, 2],
    [1, 0, 1, 2],
    [1, 1, 2, 0],
    [1, 2, 0, 1],
    [2, 0, 2, 1],
    [2, 1, 0, 2],
    [2, 2, 1, 0],
], dtype=np.int8)

# L12: up to 11 factors, 2 levels each, 12 runs (Plackett-Burman)
# Generated from the first row [1,1,0,1,1,1,0,0,0,1,0] by cyclic permutation
_L12 = np.array([
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [1, 1, 0, 1, 1, 1, 0, 0, 0, 1, 0],
    [0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 1],
    [1, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0],
    [0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0],
    [0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0],
    [0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1],
    [1, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1],
    [1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 1],
    [1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0],
    [0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1],
    [1, 0, 1, 1, 1, 0, 0, 0, 1, 0, 1],
], dtype=np.int8)

# L16: up to 15 factors, 2 levels each, 16 runs
_L16 = np.array([
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1],
    [0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1],
    [0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0],
    [0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1],
    [0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0],
    [0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0],
    [0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1],
    [1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
    [1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0],
    [1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0],
    [1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1],
    [1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0],
    [1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1],
    [1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1],
    [1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0],
], dtype=np.int8)

# L18: up to 7 factors — 1 factor at 2 levels + 7 factors at 3 levels, 18 runs
# Mixed-level array. Column 0 is 2-level, columns 1-7 are 3-level.
_L18 = np.array([
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 1, 1, 1, 1, 1, 1],
    [0, 0, 2, 2, 2, 2, 2, 2],
    [0, 1, 0, 0, 1, 1, 2, 2],
    [0, 1, 1, 1, 2, 2, 0, 0],
    [0, 1, 2, 2, 0, 0, 1, 1],
    [0, 2, 0, 1, 0, 2, 1, 2],
    [0, 2, 1, 2, 1, 0, 2, 0],
    [0, 2, 2, 0, 2, 1, 0, 1],
    [1, 0, 0, 2, 1, 2, 0, 1],
    [1, 0, 1, 0, 2, 0, 1, 2],
    [1, 0, 2, 1, 0, 1, 2, 0],
    [1, 1, 0, 1, 2, 0, 2, 1],
    [1, 1, 1, 2, 0, 1, 0, 2],
    [1, 1, 2, 0, 1, 2, 1, 0],
    [1, 2, 0, 2, 2, 1, 1, 0],
    [1, 2, 1, 0, 0, 2, 2, 1],
    [1, 2, 2, 1, 1, 0, 0, 2],
], dtype=np.int8)

# L27: up to 13 factors, 3 levels each, 27 runs
_L27 = np.array([
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 0, 0, 0],
    [0, 1, 1, 1, 2, 2, 2, 0, 0, 0, 1, 1, 1],
    [0, 2, 2, 2, 0, 0, 0, 2, 2, 2, 1, 1, 1],
    [0, 2, 2, 2, 1, 1, 1, 0, 0, 0, 2, 2, 2],
    [0, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0],
    [1, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2],
    [1, 0, 1, 2, 1, 2, 0, 1, 2, 0, 1, 2, 0],
    [1, 0, 1, 2, 2, 0, 1, 2, 0, 1, 2, 0, 1],
    [1, 1, 2, 0, 0, 1, 2, 1, 2, 0, 2, 0, 1],
    [1, 1, 2, 0, 1, 2, 0, 2, 0, 1, 0, 1, 2],
    [1, 1, 2, 0, 2, 0, 1, 0, 1, 2, 1, 2, 0],
    [1, 2, 0, 1, 0, 1, 2, 2, 0, 1, 1, 2, 0],
    [1, 2, 0, 1, 1, 2, 0, 0, 1, 2, 2, 0, 1],
    [1, 2, 0, 1, 2, 0, 1, 1, 2, 0, 0, 1, 2],
    [2, 0, 2, 1, 0, 2, 1, 0, 2, 1, 0, 2, 1],
    [2, 0, 2, 1, 1, 0, 2, 1, 0, 2, 1, 0, 2],
    [2, 0, 2, 1, 2, 1, 0, 2, 1, 0, 2, 1, 0],
    [2, 1, 0, 2, 0, 2, 1, 1, 0, 2, 2, 1, 0],
    [2, 1, 0, 2, 1, 0, 2, 2, 1, 0, 0, 2, 1],
    [2, 1, 0, 2, 2, 1, 0, 0, 2, 1, 1, 0, 2],
    [2, 2, 1, 0, 0, 2, 1, 2, 1, 0, 1, 0, 2],
    [2, 2, 1, 0, 1, 0, 2, 0, 2, 1, 2, 1, 0],
    [2, 2, 1, 0, 2, 1, 0, 1, 0, 2, 0, 2, 1],
], dtype=np.int8)


# Registry: sorted by n_runs (ascending) for greedy selection
ORTHOGONAL_ARRAYS = [
    {"name": "L4",  "n_runs": 4,  "max_factors": 3,  "n_levels": 2,  "array": _L4},
    {"name": "L8",  "n_runs": 8,  "max_factors": 7,  "n_levels": 2,  "array": _L8},
    {"name": "L9",  "n_runs": 9,  "max_factors": 4,  "n_levels": 3,  "array": _L9},
    {"name": "L12", "n_runs": 12, "max_factors": 11, "n_levels": 2,  "array": _L12},
    {"name": "L16", "n_runs": 16, "max_factors": 15, "n_levels": 2,  "array": _L16},
    {"name": "L18", "n_runs": 18, "max_factors": 8,  "n_levels": 3,  "array": _L18,
     "mixed": True, "level_counts": [2, 3, 3, 3, 3, 3, 3, 3]},
    {"name": "L27", "n_runs": 27, "max_factors": 13, "n_levels": 3,  "array": _L27},
]


# ---------------------------------------------------------------------------
# Array selection
# ---------------------------------------------------------------------------

def select_array(
    n_factors: int,
    n_levels: int,
) -> dict | None:
    """Select the smallest orthogonal array that fits the given configuration.

    Args:
        n_factors: Number of factors (variables) to test.
        n_levels: Number of levels per factor (2 or 3). All factors must have
                  the same number of levels for standard arrays.

    Returns:
        Dict with keys: name, n_runs, max_factors, n_levels, array.
        None if no suitable array exists in the registry.

    Example:
        >>> oa = select_array(n_factors=5, n_levels=2)
        >>> oa["name"]
        'L8'
        >>> oa["n_runs"]
        8
    """
    if n_factors < 1:
        raise ValueError("n_factors must be >= 1")
    if n_levels not in (2, 3):
        raise ValueError("n_levels must be 2 or 3 (standard Taguchi arrays)")

    for oa in ORTHOGONAL_ARRAYS:
        # Skip mixed-level arrays for uniform-level selection
        if oa.get("mixed"):
            continue
        if oa["n_levels"] == n_levels and oa["max_factors"] >= n_factors:
            return oa

    return None


def select_mixed_array(
    level_counts: list[int],
) -> dict | None:
    """Select an orthogonal array for mixed-level factors.

    Args:
        level_counts: List of level counts per factor, e.g. [2, 3, 3, 3].

    Returns:
        Matching OA dict or None.
    """
    n_factors = len(level_counts)
    if n_factors < 1:
        raise ValueError("Need at least one factor")

    for oa in ORTHOGONAL_ARRAYS:
        if not oa.get("mixed"):
            continue
        if oa["max_factors"] >= n_factors:
            oa_levels = oa["level_counts"][:n_factors]
            # Check that each factor's level count is supported
            if all(req <= avail for req, avail in zip(level_counts, oa_levels)):
                return oa

    return None


# ---------------------------------------------------------------------------
# Generate experiment variants from an orthogonal array
# ---------------------------------------------------------------------------

def generate_variants(
    factors: list[dict],
    oa: dict | None = None,
) -> list[dict]:
    """Generate variant combinations from an orthogonal array.

    Args:
        factors: List of factor definitions, each with:
            - "factor_id": str — unique identifier
            - "name": str — human-readable name
            - "levels": list[dict] — each with "level_id" and "value"
        oa: Orthogonal array dict (from select_array). If None, auto-selects.

    Returns:
        List of variant dicts, each with:
            - "run_index": int — row index in the OA (0-based)
            - "factor_values": dict mapping factor_id -> level dict

    Raises:
        ValueError: If no suitable OA exists for the configuration.

    Example:
        >>> factors = [
        ...     {"factor_id": "h", "name": "Headline", "levels": [
        ...         {"level_id": "h0", "value": "Original"},
        ...         {"level_id": "h1", "value": "Urgency"},
        ...     ]},
        ...     {"factor_id": "c", "name": "CTA", "levels": [
        ...         {"level_id": "c0", "value": "Learn More"},
        ...         {"level_id": "c1", "value": "Get Started"},
        ...     ]},
        ... ]
        >>> variants = generate_variants(factors)
        >>> len(variants)
        4
    """
    n_factors = len(factors)
    level_counts = [len(f["levels"]) for f in factors]

    # Validate all factors have the same number of levels for standard arrays
    unique_levels = set(level_counts)

    if oa is None:
        if len(unique_levels) == 1:
            n_levels = level_counts[0]
            oa = select_array(n_factors, n_levels)
        else:
            oa = select_mixed_array(level_counts)

        if oa is None:
            raise ValueError(
                f"No orthogonal array available for {n_factors} factors "
                f"with level counts {level_counts}. "
                f"Supported: 2-level (up to 15 factors), 3-level (up to 13 factors)."
            )

    array = oa["array"]
    # Use only the first n_factors columns
    sub_array = array[:, :n_factors]

    variants = []
    for run_idx in range(sub_array.shape[0]):
        factor_values = {}
        for col_idx, factor in enumerate(factors):
            level_idx = int(sub_array[run_idx, col_idx])
            level = factor["levels"][level_idx]
            factor_values[factor["factor_id"]] = {
                "level_id": level["level_id"],
                "value": level["value"],
                "level_index": level_idx,
            }
        variants.append({
            "run_index": run_idx,
            "factor_values": factor_values,
        })

    return variants


# ---------------------------------------------------------------------------
# Orthogonality verification
# ---------------------------------------------------------------------------

def verify_orthogonality(array: np.ndarray) -> bool:
    """Check that an array satisfies the Taguchi orthogonality property.

    For any pair of columns, every combination of level values must appear
    an equal number of times.

    Args:
        array: 2D numpy array (rows = runs, columns = factors).

    Returns:
        True if the array is orthogonal, False otherwise.
    """
    n_rows, n_cols = array.shape
    for i in range(n_cols):
        for j in range(i + 1, n_cols):
            levels_i = set(int(x) for x in array[:, i])
            levels_j = set(int(x) for x in array[:, j])

            # Count occurrences of each (level_i, level_j) pair
            pairs: dict[tuple[int, int], int] = {}
            for row in range(n_rows):
                pair = (int(array[row, i]), int(array[row, j]))
                pairs[pair] = pairs.get(pair, 0) + 1

            # Every possible combination must appear
            expected_pairs = len(levels_i) * len(levels_j)
            if len(pairs) != expected_pairs:
                return False

            # All pair counts must be equal
            counts = list(pairs.values())
            if len(set(counts)) != 1:
                return False
    return True
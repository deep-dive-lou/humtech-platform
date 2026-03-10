"""
Genome encoding, crossover, mutation, and neighbourhood fitness for evolutionary optimisation.

A genome is a list of integers — each gene is an index into that factor's levels list.
Example: [0, 2, 1] means factor 0 at level 0, factor 1 at level 2, factor 2 at level 1.

Pure math module — no DB, no IO.
"""

from __future__ import annotations

import random
from typing import Sequence


# ---------------------------------------------------------------------------
# Encoding / decoding
# ---------------------------------------------------------------------------

def encode_variant(
    factor_values: dict[str, str],
    factor_order: list[str],
    level_map: dict[str, list[str]],
) -> list[int]:
    """Convert a factor_values dict to a genome (list of level indices).

    factor_values: {factor_id: level_id, ...}
    factor_order:  ordered list of factor_ids (defines gene positions)
    level_map:     {factor_id: [level_id_0, level_id_1, ...]}
    """
    genome = []
    for fid in factor_order:
        lid = factor_values.get(fid)
        levels = level_map[fid]
        genome.append(levels.index(lid) if lid in levels else 0)
    return genome


def decode_genome(
    genome: list[int],
    factor_order: list[str],
    level_map: dict[str, list[str]],
) -> dict[str, str]:
    """Convert a genome back to a factor_values dict."""
    return {
        fid: level_map[fid][gene]
        for fid, gene in zip(factor_order, genome)
    }


# ---------------------------------------------------------------------------
# Random genome
# ---------------------------------------------------------------------------

def random_genome(levels_per_factor: list[int]) -> list[int]:
    """Generate a random genome respecting per-factor level counts."""
    return [random.randint(0, n - 1) for n in levels_per_factor]


# ---------------------------------------------------------------------------
# Crossover
# ---------------------------------------------------------------------------

def uniform_crossover(parent_a: list[int], parent_b: list[int]) -> list[int]:
    """Uniform crossover — each gene randomly chosen from either parent."""
    return [
        random.choice((a, b))
        for a, b in zip(parent_a, parent_b)
    ]


def one_point_crossover(parent_a: list[int], parent_b: list[int]) -> list[int]:
    """One-point crossover — genes before point from A, after from B."""
    if len(parent_a) <= 1:
        return list(parent_a)
    point = random.randint(1, len(parent_a) - 1)
    return parent_a[:point] + parent_b[point:]


def two_point_crossover(parent_a: list[int], parent_b: list[int]) -> list[int]:
    """Two-point crossover — segment between points swapped from B."""
    n = len(parent_a)
    if n <= 2:
        return uniform_crossover(parent_a, parent_b)
    p1, p2 = sorted(random.sample(range(1, n), 2))
    return parent_a[:p1] + parent_b[p1:p2] + parent_a[p2:]


# ---------------------------------------------------------------------------
# Mutation
# ---------------------------------------------------------------------------

def mutate(
    genome: list[int],
    gene_mutation_prob: float,
    levels_per_factor: list[int],
) -> list[int]:
    """Mutate a genome — each gene independently mutated with given probability.

    When a gene mutates, it picks a *different* level uniformly at random.
    If a factor has only 1 level, mutation is a no-op for that gene.
    """
    result = list(genome)
    for i, (gene, n_levels) in enumerate(zip(genome, levels_per_factor)):
        if n_levels <= 1:
            continue
        if random.random() < gene_mutation_prob:
            # Pick a different level
            options = [v for v in range(n_levels) if v != gene]
            result[i] = random.choice(options)
    return result


# ---------------------------------------------------------------------------
# Distance & neighbourhood
# ---------------------------------------------------------------------------

def hamming_distance(genome_a: list[int], genome_b: list[int]) -> int:
    """Count positions where two genomes differ."""
    return sum(a != b for a, b in zip(genome_a, genome_b))


def neighbourhood_fitness(
    target_idx: int,
    population: Sequence[dict],
    max_distance: int = 2,
) -> float:
    """Smoothed fitness estimate using nearby genomes (Hamming distance ≤ max_distance).

    population: list of dicts with keys 'genome' (list[int]) and 'fitness' (float).
    target_idx: index into population of the variant to estimate.

    Returns weighted average fitness where weight = 1 / (1 + distance).
    Falls back to the target's own fitness if no neighbours found.
    """
    target = population[target_idx]
    target_genome = target["genome"]

    weighted_sum = 0.0
    weight_total = 0.0

    for i, member in enumerate(population):
        dist = hamming_distance(target_genome, member["genome"])
        if dist <= max_distance and member.get("fitness") is not None:
            w = 1.0 / (1.0 + dist)
            weighted_sum += w * member["fitness"]
            weight_total += w

    if weight_total == 0.0:
        return target.get("fitness", 0.0) or 0.0

    return weighted_sum / weight_total


# ---------------------------------------------------------------------------
# Genome label
# ---------------------------------------------------------------------------

def genome_label(
    genome: list[int],
    factor_names: list[str],
    level_labels: dict[str, list[str]],
    factor_order: list[str],
) -> str:
    """Human-readable label for a genome, e.g. 'Headline-B / CTA-A / Image-C'."""
    parts = []
    for fid, gene in zip(factor_order, genome):
        fname = factor_names[factor_order.index(fid)] if fid in factor_order else fid
        labels = level_labels.get(fid, [])
        lbl = labels[gene] if gene < len(labels) else f"L{gene}"
        parts.append(f"{fname}-{lbl}")
    return " / ".join(parts)
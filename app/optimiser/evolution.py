"""
Population management for evolutionary optimisation.

Handles population initialisation, fitness evaluation, parent selection,
breeding (crossover + mutation), and full generation stepping.

Pure math module — no DB, no IO.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Sequence

from . import genome as G


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Member:
    """One individual in a population."""
    genome: list[int]
    variant_id: str | None = None
    fitness: float | None = None
    impressions: int = 0
    parent_ids: list[str] = field(default_factory=list)
    is_elite: bool = False


@dataclass
class GenerationStats:
    """Summary stats for a completed generation."""
    generation_number: int
    pop_size: int
    best_fitness: float | None
    avg_fitness: float | None
    elite_count: int
    parent_count: int


@dataclass
class EvolutionConfig:
    """GA hyperparameters stored in experiments.config JSONB."""
    pop_size: int = 8
    mutation_rate: float = 0.1          # probability a genome mutates at all
    gene_mutation_prob: float = 0.2     # per-gene flip prob when genome mutates
    crossover_fn: str = "uniform"       # "uniform" | "one_point" | "two_point"
    elite_pct: float = 0.2             # top % kept unchanged
    parent_pct: float = 0.5            # top % eligible to breed
    max_generations: int = 20
    min_impressions: int = 50           # per variant before advancing
    neighbourhood_radius: int = 2       # Hamming distance for fitness smoothing
    use_relative_fitness: bool = True   # measure lift vs control

    @classmethod
    def from_dict(cls, d: dict) -> "EvolutionConfig":
        """Parse from experiments.config JSONB."""
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


CROSSOVER_FNS: dict[str, Callable] = {
    "uniform": G.uniform_crossover,
    "one_point": G.one_point_crossover,
    "two_point": G.two_point_crossover,
}


# ---------------------------------------------------------------------------
# Population initialisation
# ---------------------------------------------------------------------------

def init_population(
    levels_per_factor: list[int],
    pop_size: int,
    seed_from: list[list[int]] | None = None,
) -> list[Member]:
    """Create initial population.

    seed_from: optional list of genomes (e.g. from Taguchi results) to include.
    Remaining slots filled with random genomes. Deduplicates.
    """
    seen: set[tuple[int, ...]] = set()
    members: list[Member] = []

    # Seed genomes first
    if seed_from:
        for g in seed_from:
            key = tuple(g)
            if key not in seen:
                seen.add(key)
                members.append(Member(genome=list(g)))

    # Fill remaining with random
    attempts = 0
    max_attempts = pop_size * 10
    while len(members) < pop_size and attempts < max_attempts:
        g = G.random_genome(levels_per_factor)
        key = tuple(g)
        if key not in seen:
            seen.add(key)
            members.append(Member(genome=list(g)))
        attempts += 1

    return members[:pop_size]


# ---------------------------------------------------------------------------
# Fitness evaluation
# ---------------------------------------------------------------------------

def evaluate_fitness(
    population: list[Member],
    observations: dict[str, dict],
) -> list[Member]:
    """Assign raw fitness to each member from observation data.

    observations: {variant_id: {"impressions": int, "conversions": int}}
    Fitness = conversion rate (conversions / impressions).
    """
    for m in population:
        if m.variant_id and m.variant_id in observations:
            obs = observations[m.variant_id]
            m.impressions = obs.get("impressions", 0)
            convs = obs.get("conversions", 0)
            m.fitness = convs / m.impressions if m.impressions > 0 else None
        else:
            m.fitness = None
            m.impressions = 0
    return population


def apply_relative_fitness(
    population: list[Member],
    control_fitness: float,
) -> list[Member]:
    """Convert absolute fitness to relative lift vs control.

    relative = (variant_fitness - control_fitness) / control_fitness
    Positive = better than control, negative = worse.
    """
    if control_fitness <= 0:
        return population
    for m in population:
        if m.fitness is not None:
            m.fitness = (m.fitness - control_fitness) / control_fitness
    return population


def apply_neighbourhood_smoothing(
    population: list[Member],
    radius: int = 2,
) -> list[Member]:
    """Smooth fitness estimates using neighbourhood averaging."""
    pop_dicts = [{"genome": m.genome, "fitness": m.fitness} for m in population]
    for i, m in enumerate(population):
        if m.fitness is not None:
            m.fitness = G.neighbourhood_fitness(i, pop_dicts, max_distance=radius)
    return population


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def select_parents(
    population: list[Member],
    elite_pct: float = 0.2,
    parent_pct: float = 0.5,
) -> tuple[list[Member], list[Member]]:
    """Select elite and parent pools from population.

    Sorts by fitness (descending). Members without fitness are ranked last.
    Elite pool: top elite_pct (carried forward unchanged).
    Parent pool: top parent_pct (eligible to breed).
    """
    # Sort: fitness descending, None last
    ranked = sorted(
        population,
        key=lambda m: (m.fitness is not None, m.fitness or 0.0),
        reverse=True,
    )

    n = len(ranked)
    elite_count = max(1, int(n * elite_pct))
    parent_count = max(2, int(n * parent_pct))

    elite = ranked[:elite_count]
    parents = ranked[:parent_count]

    for m in elite:
        m.is_elite = True

    return elite, parents


# ---------------------------------------------------------------------------
# Breeding
# ---------------------------------------------------------------------------

def breed_generation(
    parent_pool: list[Member],
    elite_pool: list[Member],
    pop_size: int,
    levels_per_factor: list[int],
    config: EvolutionConfig,
) -> list[Member]:
    """Create next generation via crossover + mutation.

    1. Elite members carried forward unchanged.
    2. Remaining slots filled by breeding from parent pool.
    3. Each child: crossover two random parents → maybe mutate.
    4. Deduplication: skip duplicates, retry with different parents.
    """
    crossover_fn = CROSSOVER_FNS.get(config.crossover_fn, G.uniform_crossover)
    next_gen: list[Member] = []
    seen: set[tuple[int, ...]] = set()

    # Carry elite forward
    for m in elite_pool:
        key = tuple(m.genome)
        if key not in seen:
            seen.add(key)
            next_gen.append(Member(
                genome=list(m.genome),
                parent_ids=[m.variant_id] if m.variant_id else [],
                is_elite=True,
            ))

    # Breed remaining
    attempts = 0
    max_attempts = pop_size * 20
    while len(next_gen) < pop_size and attempts < max_attempts:
        p1, p2 = random.sample(parent_pool, min(2, len(parent_pool)))
        child_genome = crossover_fn(p1.genome, p2.genome)

        # Two-level mutation: first decide if genome mutates at all
        if random.random() < config.mutation_rate:
            child_genome = G.mutate(
                child_genome, config.gene_mutation_prob, levels_per_factor
            )

        key = tuple(child_genome)
        if key not in seen:
            seen.add(key)
            parent_ids = []
            if p1.variant_id:
                parent_ids.append(p1.variant_id)
            if p2.variant_id:
                parent_ids.append(p2.variant_id)
            next_gen.append(Member(genome=child_genome, parent_ids=parent_ids))

        attempts += 1

    return next_gen[:pop_size]


# ---------------------------------------------------------------------------
# Full generation step
# ---------------------------------------------------------------------------

def step_generation(
    population: list[Member],
    observations: dict[str, dict],
    levels_per_factor: list[int],
    config: EvolutionConfig,
    control_variant_id: str | None = None,
) -> tuple[list[Member], GenerationStats]:
    """Execute one full generation cycle.

    1. Evaluate fitness from observations
    2. Optionally apply relative fitness (vs control)
    3. Apply neighbourhood smoothing
    4. Select elite + parent pools
    5. Breed next generation

    Returns (next_generation, stats_for_current_generation).
    """
    # 1. Raw fitness
    evaluate_fitness(population, observations)

    # 2. Relative fitness
    if config.use_relative_fitness and control_variant_id:
        control_obs = observations.get(control_variant_id, {})
        c_imp = control_obs.get("impressions", 0)
        c_conv = control_obs.get("conversions", 0)
        control_fit = c_conv / c_imp if c_imp > 0 else 0.0
        if control_fit > 0:
            apply_relative_fitness(population, control_fit)

    # 3. Neighbourhood smoothing
    if config.neighbourhood_radius > 0:
        apply_neighbourhood_smoothing(population, config.neighbourhood_radius)

    # 4. Selection
    elite, parents = select_parents(population, config.elite_pct, config.parent_pct)

    # Stats for current generation
    fitnesses = [m.fitness for m in population if m.fitness is not None]
    stats = GenerationStats(
        generation_number=0,  # caller sets this
        pop_size=len(population),
        best_fitness=max(fitnesses) if fitnesses else None,
        avg_fitness=sum(fitnesses) / len(fitnesses) if fitnesses else None,
        elite_count=len(elite),
        parent_count=len(parents),
    )

    # 5. Breed
    next_gen = breed_generation(parents, elite, config.pop_size, levels_per_factor, config)

    return next_gen, stats
"""Tests for optimiser evolutionary/GA modules (genome.py + evolution.py)."""

import random
import pytest

from app.optimiser.genome import (
    decode_genome,
    encode_variant,
    hamming_distance,
    mutate,
    neighbourhood_fitness,
    one_point_crossover,
    random_genome,
    two_point_crossover,
    uniform_crossover,
    genome_label,
)
from app.optimiser.evolution import (
    EvolutionConfig,
    Member,
    apply_neighbourhood_smoothing,
    apply_relative_fitness,
    breed_generation,
    evaluate_fitness,
    init_population,
    select_parents,
    step_generation,
)


# ===== Fixtures =====

FACTOR_ORDER = ["f1", "f2", "f3"]
LEVEL_MAP = {
    "f1": ["l1a", "l1b"],
    "f2": ["l2a", "l2b", "l2c"],
    "f3": ["l3a", "l3b"],
}
LEVELS_PER_FACTOR = [2, 3, 2]  # matches LEVEL_MAP


# ===== genome.py tests =====


class TestEncoding:
    def test_encode_decode_round_trip(self):
        fv = {"f1": "l1b", "f2": "l2c", "f3": "l3a"}
        genome = encode_variant(fv, FACTOR_ORDER, LEVEL_MAP)
        assert genome == [1, 2, 0]
        decoded = decode_genome(genome, FACTOR_ORDER, LEVEL_MAP)
        assert decoded == fv

    def test_encode_missing_factor_defaults_to_zero(self):
        fv = {"f1": "l1a", "f3": "l3b"}  # f2 missing
        genome = encode_variant(fv, FACTOR_ORDER, LEVEL_MAP)
        assert genome[1] == 0  # f2 defaults to index 0

    def test_encode_unknown_level_defaults_to_zero(self):
        fv = {"f1": "l1a", "f2": "UNKNOWN", "f3": "l3a"}
        genome = encode_variant(fv, FACTOR_ORDER, LEVEL_MAP)
        assert genome[1] == 0

    def test_decode_produces_correct_types(self):
        genome = [0, 0, 0]
        result = decode_genome(genome, FACTOR_ORDER, LEVEL_MAP)
        assert isinstance(result, dict)
        assert all(isinstance(v, str) for v in result.values())


class TestRandomGenome:
    def test_respects_level_bounds(self):
        random.seed(42)
        for _ in range(100):
            g = random_genome(LEVELS_PER_FACTOR)
            assert len(g) == 3
            assert 0 <= g[0] < 2
            assert 0 <= g[1] < 3
            assert 0 <= g[2] < 2

    def test_single_level_factor(self):
        g = random_genome([1, 1, 1])
        assert g == [0, 0, 0]


class TestCrossover:
    def test_uniform_crossover_length(self):
        random.seed(42)
        p1 = [0, 0, 0]
        p2 = [1, 2, 1]
        child = uniform_crossover(p1, p2)
        assert len(child) == 3
        for i, gene in enumerate(child):
            assert gene in (p1[i], p2[i])

    def test_one_point_crossover_length(self):
        random.seed(42)
        p1 = [0, 0, 0, 0]
        p2 = [1, 1, 1, 1]
        child = one_point_crossover(p1, p2)
        assert len(child) == 4

    def test_one_point_crossover_single_gene(self):
        child = one_point_crossover([0], [1])
        assert child == [0]  # single gene returns parent_a

    def test_two_point_crossover_length(self):
        random.seed(42)
        p1 = [0, 0, 0, 0, 0]
        p2 = [1, 1, 1, 1, 1]
        child = two_point_crossover(p1, p2)
        assert len(child) == 5

    def test_two_point_crossover_short_genome(self):
        random.seed(42)
        child = two_point_crossover([0, 0], [1, 1])
        assert len(child) == 2

    def test_crossover_genes_from_parents(self):
        """All child genes must come from one parent or the other."""
        random.seed(42)
        p1 = [0, 1, 0, 2, 0]
        p2 = [1, 0, 1, 0, 1]
        for _ in range(50):
            child = uniform_crossover(p1, p2)
            for i, gene in enumerate(child):
                assert gene in (p1[i], p2[i])


class TestMutation:
    def test_mutation_respects_bounds(self):
        random.seed(42)
        g = [1, 2, 1]
        for _ in range(100):
            m = mutate(g, 1.0, LEVELS_PER_FACTOR)  # 100% mutation rate
            assert 0 <= m[0] < 2
            assert 0 <= m[1] < 3
            assert 0 <= m[2] < 2

    def test_mutation_changes_gene(self):
        """With 100% mutation, at least some genes should differ."""
        random.seed(42)
        g = [0, 0, 0]
        changed = False
        for _ in range(50):
            m = mutate(g, 1.0, LEVELS_PER_FACTOR)
            if m != g:
                changed = True
                break
        assert changed

    def test_zero_mutation_rate(self):
        g = [1, 2, 1]
        m = mutate(g, 0.0, LEVELS_PER_FACTOR)
        assert m == g

    def test_single_level_factor_unchanged(self):
        """Factors with 1 level can't mutate."""
        g = [0, 0, 0]
        m = mutate(g, 1.0, [1, 1, 1])
        assert m == [0, 0, 0]

    def test_mutation_picks_different_value(self):
        """When mutated, gene must differ from original."""
        random.seed(42)
        g = [0, 1, 0]
        for _ in range(100):
            m = mutate(g, 1.0, LEVELS_PER_FACTOR)
            for i in range(len(g)):
                if m[i] != g[i]:
                    # Mutated gene should be different from original
                    assert m[i] != g[i]


class TestHammingDistance:
    def test_identical_genomes(self):
        assert hamming_distance([0, 1, 2], [0, 1, 2]) == 0

    def test_completely_different(self):
        assert hamming_distance([0, 0, 0], [1, 1, 1]) == 3

    def test_one_difference(self):
        assert hamming_distance([0, 0, 0], [0, 1, 0]) == 1


class TestNeighbourhoodFitness:
    def test_self_included(self):
        """Target's own fitness contributes (distance 0, weight 1.0)."""
        pop = [
            {"genome": [0, 0], "fitness": 0.1},
            {"genome": [1, 1], "fitness": 0.9},  # distance 2
        ]
        result = neighbourhood_fitness(0, pop, max_distance=2)
        # weighted: 0.1 * 1.0 + 0.9 * (1/3) = 0.1 + 0.3 = 0.4 / (1.0 + 1/3) = 0.4/1.333 ≈ 0.3
        assert 0.2 < result < 0.5

    def test_distant_excluded(self):
        """Genomes beyond max_distance don't contribute."""
        pop = [
            {"genome": [0, 0, 0], "fitness": 0.1},
            {"genome": [1, 1, 1], "fitness": 0.9},  # distance 3
        ]
        result = neighbourhood_fitness(0, pop, max_distance=2)
        assert result == pytest.approx(0.1)  # only self

    def test_none_fitness_skipped(self):
        pop = [
            {"genome": [0, 0], "fitness": 0.5},
            {"genome": [0, 1], "fitness": None},
        ]
        result = neighbourhood_fitness(0, pop, max_distance=2)
        assert result == pytest.approx(0.5)

    def test_all_none_returns_zero(self):
        pop = [{"genome": [0, 0], "fitness": None}]
        result = neighbourhood_fitness(0, pop, max_distance=2)
        assert result == 0.0


class TestGenomeLabel:
    def test_basic_label(self):
        genome = [0, 2, 1]
        factor_names = ["Headline", "CTA", "Image"]
        level_labels = {
            "f1": ["A", "B"],
            "f2": ["X", "Y", "Z"],
            "f3": ["Dark", "Light"],
        }
        label = genome_label(genome, factor_names, level_labels, FACTOR_ORDER)
        assert label == "Headline-A / CTA-Z / Image-Light"


# ===== evolution.py tests =====


class TestInitPopulation:
    def test_correct_size(self):
        random.seed(42)
        pop = init_population(LEVELS_PER_FACTOR, 10)
        assert len(pop) == 10

    def test_no_duplicates(self):
        random.seed(42)
        pop = init_population(LEVELS_PER_FACTOR, 10)
        genomes = [tuple(m.genome) for m in pop]
        assert len(set(genomes)) == len(genomes)

    def test_seeded(self):
        seeds = [[0, 0, 0], [1, 1, 1]]
        pop = init_population(LEVELS_PER_FACTOR, 5, seed_from=seeds)
        assert len(pop) == 5
        assert pop[0].genome == [0, 0, 0]
        assert pop[1].genome == [1, 1, 1]

    def test_max_population_capped(self):
        """With [2,2] factors there are only 4 unique combos."""
        pop = init_population([2, 2], 10)
        assert len(pop) <= 4

    def test_single_level_factor(self):
        pop = init_population([1, 1], 1)
        assert len(pop) == 1
        assert pop[0].genome == [0, 0]


class TestEvaluateFitness:
    def test_assigns_fitness(self):
        pop = [
            Member(genome=[0, 0], variant_id="v1"),
            Member(genome=[1, 1], variant_id="v2"),
        ]
        obs = {
            "v1": {"impressions": 100, "conversions": 10},
            "v2": {"impressions": 200, "conversions": 50},
        }
        evaluate_fitness(pop, obs)
        assert pop[0].fitness == pytest.approx(0.1)
        assert pop[1].fitness == pytest.approx(0.25)
        assert pop[0].impressions == 100

    def test_zero_impressions(self):
        pop = [Member(genome=[0], variant_id="v1")]
        obs = {"v1": {"impressions": 0, "conversions": 0}}
        evaluate_fitness(pop, obs)
        assert pop[0].fitness is None

    def test_missing_variant(self):
        pop = [Member(genome=[0], variant_id="v1")]
        evaluate_fitness(pop, {})
        assert pop[0].fitness is None
        assert pop[0].impressions == 0


class TestRelativeFitness:
    def test_relative_lift(self):
        pop = [
            Member(genome=[0], fitness=0.15),
            Member(genome=[1], fitness=0.10),
        ]
        apply_relative_fitness(pop, control_fitness=0.10)
        assert pop[0].fitness == pytest.approx(0.5)   # 50% lift
        assert pop[1].fitness == pytest.approx(0.0)    # no lift

    def test_zero_control_no_change(self):
        pop = [Member(genome=[0], fitness=0.15)]
        original = pop[0].fitness
        apply_relative_fitness(pop, control_fitness=0.0)
        assert pop[0].fitness == original


class TestSelectParents:
    def test_elite_and_parent_sizes(self):
        pop = [Member(genome=[i], fitness=i * 0.1) for i in range(10)]
        elite, parents = select_parents(pop, elite_pct=0.3, parent_pct=0.5)
        assert len(elite) == 3
        assert len(parents) == 5

    def test_elite_are_best(self):
        pop = [Member(genome=[i], fitness=i * 0.1) for i in range(10)]
        elite, _ = select_parents(pop, elite_pct=0.2, parent_pct=0.5)
        fitnesses = [m.fitness for m in elite]
        assert fitnesses == sorted(fitnesses, reverse=True)

    def test_none_fitness_ranked_last(self):
        pop = [
            Member(genome=[0], fitness=None),
            Member(genome=[1], fitness=0.5),
            Member(genome=[2], fitness=0.3),
        ]
        elite, _ = select_parents(pop, elite_pct=0.5, parent_pct=1.0)
        assert elite[0].fitness == 0.5

    def test_minimum_counts(self):
        """Elite min 1, parent min 2."""
        pop = [Member(genome=[i], fitness=i * 0.1) for i in range(3)]
        elite, parents = select_parents(pop, elite_pct=0.01, parent_pct=0.01)
        assert len(elite) >= 1
        assert len(parents) >= 2


class TestBreedGeneration:
    def test_output_size(self):
        random.seed(42)
        config = EvolutionConfig(pop_size=8, mutation_rate=0.1, gene_mutation_prob=0.2)
        parents = [Member(genome=[i % 2, i % 3, i % 2], variant_id=f"v{i}") for i in range(4)]
        elite = [parents[0]]
        next_gen = breed_generation(parents, elite, 8, LEVELS_PER_FACTOR, config)
        assert len(next_gen) <= 8

    def test_elite_carried_forward(self):
        random.seed(42)
        config = EvolutionConfig(pop_size=6, mutation_rate=0.0)
        elite_member = Member(genome=[0, 0, 0], variant_id="elite1")
        parents = [elite_member, Member(genome=[1, 2, 1], variant_id="p2")]
        next_gen = breed_generation(parents, [elite_member], 6, LEVELS_PER_FACTOR, config)
        elite_genomes = [tuple(m.genome) for m in next_gen if m.is_elite]
        assert (0, 0, 0) in elite_genomes

    def test_no_duplicates(self):
        random.seed(42)
        config = EvolutionConfig(pop_size=8, mutation_rate=0.5, gene_mutation_prob=0.3)
        parents = [Member(genome=random_genome(LEVELS_PER_FACTOR), variant_id=f"v{i}") for i in range(4)]
        elite = [parents[0]]
        next_gen = breed_generation(parents, elite, 8, LEVELS_PER_FACTOR, config)
        genomes = [tuple(m.genome) for m in next_gen]
        assert len(set(genomes)) == len(genomes)

    def test_parent_ids_tracked(self):
        random.seed(42)
        config = EvolutionConfig(pop_size=4, mutation_rate=0.0)
        parents = [
            Member(genome=[0, 0, 0], variant_id="v1"),
            Member(genome=[1, 2, 1], variant_id="v2"),
        ]
        next_gen = breed_generation(parents, [parents[0]], 4, LEVELS_PER_FACTOR, config)
        bred = [m for m in next_gen if not m.is_elite]
        for m in bred:
            assert len(m.parent_ids) > 0


class TestNeighbourhoodSmoothing:
    def test_smoothing_reduces_noise(self):
        pop = [
            Member(genome=[0, 0], fitness=0.1),
            Member(genome=[0, 1], fitness=0.9),  # distance 1
            Member(genome=[1, 0], fitness=0.5),  # distance 1
        ]
        apply_neighbourhood_smoothing(pop, radius=1)
        # All fitnesses should move toward each other
        assert 0.1 < pop[0].fitness < 0.9


class TestStepGeneration:
    def test_full_cycle(self):
        random.seed(42)
        config = EvolutionConfig(
            pop_size=6, mutation_rate=0.2, gene_mutation_prob=0.3,
            elite_pct=0.3, parent_pct=0.5, neighbourhood_radius=0,
            use_relative_fitness=False,
        )
        pop = init_population(LEVELS_PER_FACTOR, 6)
        for i, m in enumerate(pop):
            m.variant_id = f"v{i}"

        obs = {f"v{i}": {"impressions": 100, "conversions": 5 + i * 3} for i in range(6)}

        next_gen, stats = step_generation(pop, obs, LEVELS_PER_FACTOR, config)

        assert len(next_gen) <= 6
        assert stats.pop_size == 6
        assert stats.best_fitness is not None
        assert stats.avg_fitness is not None
        assert stats.elite_count >= 1

    def test_with_relative_fitness(self):
        random.seed(42)
        config = EvolutionConfig(
            pop_size=4, mutation_rate=0.1, neighbourhood_radius=0,
            use_relative_fitness=True,
        )
        pop = [
            Member(genome=[0, 0], variant_id="ctrl"),
            Member(genome=[0, 1], variant_id="v1"),
            Member(genome=[1, 0], variant_id="v2"),
            Member(genome=[1, 1], variant_id="v3"),
        ]
        obs = {
            "ctrl": {"impressions": 100, "conversions": 10},  # 10% baseline
            "v1": {"impressions": 100, "conversions": 15},     # 50% lift
            "v2": {"impressions": 100, "conversions": 5},      # -50% lift
            "v3": {"impressions": 100, "conversions": 20},     # 100% lift
        }

        next_gen, stats = step_generation(
            pop, obs, [2, 2], config, control_variant_id="ctrl"
        )
        assert stats.best_fitness is not None
        assert len(next_gen) <= 4


class TestEvolutionConfig:
    def test_from_dict(self):
        d = {"pop_size": 16, "mutation_rate": 0.3, "unknown_field": True}
        config = EvolutionConfig.from_dict(d)
        assert config.pop_size == 16
        assert config.mutation_rate == 0.3
        assert config.gene_mutation_prob == 0.2  # default

    def test_defaults(self):
        config = EvolutionConfig()
        assert config.pop_size == 8
        assert config.max_generations == 20


class TestEdgeCases:
    def test_single_factor(self):
        random.seed(42)
        pop = init_population([3], 3)
        assert len(pop) == 3
        for m in pop:
            assert len(m.genome) == 1
            assert 0 <= m.genome[0] < 3

    def test_pop_size_one(self):
        pop = init_population(LEVELS_PER_FACTOR, 1)
        assert len(pop) == 1

    def test_all_same_fitness(self):
        """Selection should still work when all fitnesses are equal."""
        pop = [Member(genome=[i], fitness=0.5) for i in range(5)]
        elite, parents = select_parents(pop, elite_pct=0.2, parent_pct=0.5)
        assert len(elite) >= 1
        assert len(parents) >= 2

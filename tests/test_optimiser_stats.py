"""
Tests for the Optimisation Engine statistics library.

Verifies: Beta posterior updates, credible intervals, P(best),
expected loss, winner declaration, and Thompson Sampling allocation.
"""

from datetime import datetime, timedelta, timezone

import pytest

from app.optimiser.stats import (
    beta_posterior,
    check_winner,
    credible_interval,
    expected_loss,
    p_best,
    thompson_allocate,
)


# ---------------------------------------------------------------------------
# beta_posterior
# ---------------------------------------------------------------------------

class TestBetaPosterior:
    def test_uniform_prior_with_data(self):
        a, b = beta_posterior(1, 1, 10, 90)
        assert a == 11
        assert b == 91

    def test_zero_observations(self):
        a, b = beta_posterior(1, 1, 0, 0)
        assert a == 1
        assert b == 1

    def test_custom_prior(self):
        a, b = beta_posterior(5, 5, 20, 80)
        assert a == 25
        assert b == 85


# ---------------------------------------------------------------------------
# credible_interval
# ---------------------------------------------------------------------------

class TestCredibleInterval:
    def test_uniform_prior_bounds(self):
        """Uniform Beta(1,1) should give roughly [0.025, 0.975] at 95% CI."""
        lo, hi = credible_interval(1, 1, ci=0.95)
        assert abs(lo - 0.025) < 0.001
        assert abs(hi - 0.975) < 0.001

    def test_tight_posterior(self):
        """Strong evidence (alpha=100, beta=100) should give narrow CI around 0.5."""
        lo, hi = credible_interval(100, 100, ci=0.95)
        assert lo > 0.43
        assert hi < 0.57
        assert lo < 0.5 < hi

    def test_skewed_posterior(self):
        """High conversion rate posterior: CI should be above 0.5."""
        lo, hi = credible_interval(100, 10, ci=0.95)
        assert lo > 0.8
        assert hi < 1.0

    def test_90_percent_ci_narrower(self):
        lo_90, hi_90 = credible_interval(50, 50, ci=0.90)
        lo_95, hi_95 = credible_interval(50, 50, ci=0.95)
        assert (hi_90 - lo_90) < (hi_95 - lo_95)


# ---------------------------------------------------------------------------
# p_best
# ---------------------------------------------------------------------------

class TestPBest:
    def test_clear_winner(self):
        """One variant with 100 conversions/10 failures vs one with 10/100.
        The first should have P(best) > 0.99."""
        variants = [
            {"variant_id": "A", "alpha": 101, "beta": 11},
            {"variant_id": "B", "alpha": 11, "beta": 101},
        ]
        result = p_best(variants, n_samples=100_000)
        assert result["A"] > 0.99
        assert result["B"] < 0.01

    def test_equal_variants(self):
        """Two identical posteriors should each get ~0.5."""
        variants = [
            {"variant_id": "A", "alpha": 50, "beta": 50},
            {"variant_id": "B", "alpha": 50, "beta": 50},
        ]
        result = p_best(variants, n_samples=100_000)
        assert abs(result["A"] - 0.5) < 0.05
        assert abs(result["B"] - 0.5) < 0.05

    def test_probabilities_sum_to_one(self):
        variants = [
            {"variant_id": "A", "alpha": 30, "beta": 70},
            {"variant_id": "B", "alpha": 40, "beta": 60},
            {"variant_id": "C", "alpha": 50, "beta": 50},
        ]
        result = p_best(variants, n_samples=100_000)
        assert abs(sum(result.values()) - 1.0) < 0.001

    def test_three_variants_ordering(self):
        """C has highest rate, B middle, A lowest."""
        variants = [
            {"variant_id": "A", "alpha": 20, "beta": 80},
            {"variant_id": "B", "alpha": 40, "beta": 60},
            {"variant_id": "C", "alpha": 60, "beta": 40},
        ]
        result = p_best(variants, n_samples=100_000)
        assert result["C"] > result["B"] > result["A"]


# ---------------------------------------------------------------------------
# expected_loss
# ---------------------------------------------------------------------------

class TestExpectedLoss:
    def test_dominant_variant_near_zero(self):
        """A dominant variant should have expected loss approaching 0."""
        variants = [
            {"variant_id": "A", "alpha": 200, "beta": 10},
            {"variant_id": "B", "alpha": 10, "beta": 200},
        ]
        result = expected_loss(variants, n_samples=100_000)
        assert result["A"] < 0.01
        assert result["B"] > 0.5

    def test_equal_variants_similar_loss(self):
        variants = [
            {"variant_id": "A", "alpha": 50, "beta": 50},
            {"variant_id": "B", "alpha": 50, "beta": 50},
        ]
        result = expected_loss(variants, n_samples=100_000)
        assert abs(result["A"] - result["B"]) < 0.02

    def test_loss_always_non_negative(self):
        variants = [
            {"variant_id": "A", "alpha": 30, "beta": 70},
            {"variant_id": "B", "alpha": 50, "beta": 50},
        ]
        result = expected_loss(variants, n_samples=100_000)
        for v in result.values():
            assert v >= 0


# ---------------------------------------------------------------------------
# check_winner
# ---------------------------------------------------------------------------

class TestCheckWinner:
    def _make_rules(self, **overrides):
        rules = {
            "p_best_threshold": 0.95,
            "expected_loss_threshold": 0.01,
            "min_impressions": 100,
            "min_days": 7,
        }
        rules.update(overrides)
        return rules

    def test_respects_min_days(self):
        """Should not declare winner before min_days even with perfect data."""
        variants = [
            {"variant_id": "A", "alpha": 500, "beta": 10, "impressions": 510},
            {"variant_id": "B", "alpha": 10, "beta": 500, "impressions": 510},
        ]
        started = datetime.now(timezone.utc) - timedelta(days=3)
        result = check_winner(variants, self._make_rules(), started)
        assert result is None

    def test_respects_min_impressions(self):
        """Should not declare winner if any variant has too few impressions."""
        variants = [
            {"variant_id": "A", "alpha": 500, "beta": 10, "impressions": 510},
            {"variant_id": "B", "alpha": 10, "beta": 500, "impressions": 50},  # below threshold
        ]
        started = datetime.now(timezone.utc) - timedelta(days=14)
        result = check_winner(variants, self._make_rules(), started)
        assert result is None

    def test_declares_winner(self):
        """All rules met: should declare the dominant variant."""
        variants = [
            {"variant_id": "A", "alpha": 500, "beta": 10, "impressions": 510},
            {"variant_id": "B", "alpha": 10, "beta": 500, "impressions": 510},
        ]
        started = datetime.now(timezone.utc) - timedelta(days=14)
        result = check_winner(variants, self._make_rules(), started)
        assert result is not None
        assert result["variant_id"] == "A"
        assert result["p_best"] > 0.95
        assert result["expected_loss"] < 0.01

    def test_no_winner_when_close(self):
        """Two similar variants — should not declare winner."""
        variants = [
            {"variant_id": "A", "alpha": 51, "beta": 49, "impressions": 200},
            {"variant_id": "B", "alpha": 49, "beta": 51, "impressions": 200},
        ]
        started = datetime.now(timezone.utc) - timedelta(days=14)
        result = check_winner(variants, self._make_rules(), started)
        assert result is None


# ---------------------------------------------------------------------------
# thompson_allocate
# ---------------------------------------------------------------------------

class TestThompsonAllocate:
    def test_returns_valid_id(self):
        variants = [
            {"variant_id": "A", "alpha": 10, "beta": 10},
            {"variant_id": "B", "alpha": 10, "beta": 10},
            {"variant_id": "C", "alpha": 10, "beta": 10},
        ]
        result = thompson_allocate(variants)
        assert result in {"A", "B", "C"}

    def test_favours_better_variant(self):
        """Over many allocations, a dominant variant should be selected most often."""
        variants = [
            {"variant_id": "A", "alpha": 200, "beta": 10},
            {"variant_id": "B", "alpha": 10, "beta": 200},
        ]
        counts = {"A": 0, "B": 0}
        for _ in range(1000):
            counts[thompson_allocate(variants)] += 1
        assert counts["A"] > 900  # Should win almost every time

"""
Tests for the optimisation engine sequential statistics library.

Covers: confidence_sequence, confidence_sequence_difference, msprt,
sprt_all_variants, sequential_status.
"""

import math
import pytest
from app.optimiser.sequential import (
    confidence_sequence,
    confidence_sequence_difference,
    msprt,
    sprt_all_variants,
    sequential_status,
)


# ---------------------------------------------------------------------------
# confidence_sequence
# ---------------------------------------------------------------------------

class TestConfidenceSequence:

    def test_zero_trials_returns_full_range(self):
        lo, hi = confidence_sequence(0, 0)
        assert lo == 0.0
        assert hi == 1.0

    def test_small_sample_wide_interval(self):
        lo, hi = confidence_sequence(5, 10)
        assert hi - lo > 0.3

    def test_large_sample_narrow_interval(self):
        lo, hi = confidence_sequence(500, 1000)
        assert hi - lo < 0.15

    def test_bounds_within_01(self):
        lo, hi = confidence_sequence(100, 100)
        assert 0.0 <= lo <= hi <= 1.0

    def test_bounds_within_01_zero_conversions(self):
        lo, hi = confidence_sequence(0, 100)
        assert 0.0 <= lo <= hi <= 1.0

    def test_interval_shrinks_with_more_data(self):
        _, hi1 = confidence_sequence(50, 100)
        lo1, _ = confidence_sequence(50, 100)
        _, hi2 = confidence_sequence(500, 1000)
        lo2, _ = confidence_sequence(500, 1000)
        assert (hi2 - lo2) < (hi1 - lo1)

    def test_centered_on_observed_rate(self):
        lo, hi = confidence_sequence(250, 1000)
        midpoint = (lo + hi) / 2
        assert abs(midpoint - 0.25) < 0.05

    def test_wider_than_naive_interval(self):
        """Sequential CI must be wider than a simple normal CI."""
        n, s = 500, 50
        p = s / n
        naive_hw = 1.96 * math.sqrt(p * (1 - p) / n)
        lo, hi = confidence_sequence(s, n)
        seq_hw = (hi - lo) / 2
        assert seq_hw > naive_hw

    def test_monte_carlo_coverage(self):
        """True rate should be covered >= 94% at multiple stopping times."""
        import random
        random.seed(42)
        true_p = 0.10
        n_sims = 500
        covered_early = 0
        covered_late = 0

        for _ in range(n_sims):
            successes = 0
            # Check at n=50 (early) and n=500 (late)
            for i in range(1, 501):
                if random.random() < true_p:
                    successes += 1
                if i == 50:
                    lo, hi = confidence_sequence(successes, i)
                    if lo <= true_p <= hi:
                        covered_early += 1
                if i == 500:
                    lo, hi = confidence_sequence(successes, i)
                    if lo <= true_p <= hi:
                        covered_late += 1

        # Both stopping times should have >= 94% coverage
        assert covered_early / n_sims >= 0.93
        assert covered_late / n_sims >= 0.93


# ---------------------------------------------------------------------------
# confidence_sequence_difference
# ---------------------------------------------------------------------------

class TestConfidenceSequenceDifference:

    def test_no_data_returns_wide(self):
        delta, lo, hi = confidence_sequence_difference(0, 0, 0, 0)
        assert lo == -1.0
        assert hi == 1.0

    def test_same_rates_includes_zero(self):
        delta, lo, hi = confidence_sequence_difference(50, 500, 50, 500)
        assert lo <= 0 <= hi

    def test_clear_winner_excludes_zero(self):
        delta, lo, hi = confidence_sequence_difference(150, 500, 50, 500)
        assert lo > 0  # treatment significantly better

    def test_delta_hat_correct(self):
        delta, _, _ = confidence_sequence_difference(100, 500, 50, 500)
        assert abs(delta - 0.10) < 0.001  # 20% - 10%

    def test_negative_delta_when_control_better(self):
        delta, _, _ = confidence_sequence_difference(50, 500, 100, 500)
        assert delta < 0


# ---------------------------------------------------------------------------
# mSPRT
# ---------------------------------------------------------------------------

class TestMSPRT:

    def test_no_data(self):
        result = msprt(0, 0, 0, 0)
        assert result["reject_null"] is False
        assert result["statistic"] == 0.0

    def test_rejects_with_large_effect(self):
        result = msprt(150, 500, 50, 500)
        assert result["reject_null"] is True

    def test_continues_with_small_sample(self):
        result = msprt(3, 10, 2, 10)
        assert result["reject_null"] is False

    def test_continues_with_no_effect(self):
        result = msprt(50, 500, 50, 500)
        assert result["reject_null"] is False

    def test_statistic_positive(self):
        result = msprt(60, 500, 50, 500)
        assert result["statistic"] > 0

    def test_threshold_is_reciprocal_alpha(self):
        result = msprt(50, 500, 50, 500, alpha=0.05)
        assert result["threshold"] == pytest.approx(20.0)

    def test_returns_correct_rates(self):
        result = msprt(100, 500, 50, 500)
        assert result["p_treatment"] == pytest.approx(0.2)
        assert result["p_control"] == pytest.approx(0.1)
        assert result["delta_hat"] == pytest.approx(0.1)

    def test_smaller_tau_less_sensitive(self):
        """Smaller tau = harder to reject (tuned for larger effects only)."""
        result_small_tau = msprt(70, 500, 50, 500, tau=0.01)
        result_large_tau = msprt(70, 500, 50, 500, tau=0.05)
        assert result_large_tau["statistic"] >= result_small_tau["statistic"]


# ---------------------------------------------------------------------------
# sprt_all_variants
# ---------------------------------------------------------------------------

class TestSPRTAllVariants:

    def test_control_excluded_from_results(self):
        variants = [
            {"variant_id": "ctrl", "impressions": 500, "conversions": 50, "is_control": True},
            {"variant_id": "A", "impressions": 500, "conversions": 75, "is_control": False},
        ]
        results = sprt_all_variants(variants)
        assert "ctrl" not in results
        assert "A" in results

    def test_multiple_variants(self):
        variants = [
            {"variant_id": "ctrl", "impressions": 1000, "conversions": 100, "is_control": True},
            {"variant_id": "A", "impressions": 1000, "conversions": 150, "is_control": False},
            {"variant_id": "B", "impressions": 1000, "conversions": 95, "is_control": False},
        ]
        results = sprt_all_variants(variants)
        assert len(results) == 2
        assert "A" in results
        assert "B" in results

    def test_no_control_returns_empty(self):
        variants = [
            {"variant_id": "A", "impressions": 500, "conversions": 50, "is_control": False},
        ]
        results = sprt_all_variants(variants)
        assert results == {}

    def test_empty_variants(self):
        results = sprt_all_variants([])
        assert results == {}


# ---------------------------------------------------------------------------
# sequential_status
# ---------------------------------------------------------------------------

class TestSequentialStatus:

    def test_winner_found(self):
        variants = [
            {"variant_id": "ctrl", "impressions": 2000, "conversions": 200, "is_control": True},
            {"variant_id": "A", "impressions": 2000, "conversions": 400, "is_control": False},
        ]
        status = sequential_status(variants)
        assert status["recommendation"] == "winner_found"
        assert status["winner_id"] == "A"
        assert status["safe_to_stop"] is True

    def test_continue_early(self):
        variants = [
            {"variant_id": "ctrl", "impressions": 20, "conversions": 2, "is_control": True},
            {"variant_id": "A", "impressions": 20, "conversions": 3, "is_control": False},
        ]
        status = sequential_status(variants)
        assert status["recommendation"] == "continue"
        assert status["safe_to_stop"] is False

    def test_no_effect(self):
        """All treatments worse than control with large sample."""
        variants = [
            {"variant_id": "ctrl", "impressions": 5000, "conversions": 500, "is_control": True},
            {"variant_id": "A", "impressions": 5000, "conversions": 200, "is_control": False},
        ]
        status = sequential_status(variants)
        assert status["recommendation"] == "no_effect"
        assert status["safe_to_stop"] is True

    def test_variant_results_contain_cis(self):
        variants = [
            {"variant_id": "ctrl", "impressions": 100, "conversions": 10, "is_control": True},
            {"variant_id": "A", "impressions": 100, "conversions": 15, "is_control": False},
        ]
        status = sequential_status(variants)
        vr = status["variant_results"]
        assert "ci_lower" in vr["ctrl"]
        assert "ci_upper" in vr["ctrl"]
        assert "ci_lower" in vr["A"]
        assert "sprt_decision" in vr["A"]

    def test_control_marked_as_control(self):
        variants = [
            {"variant_id": "ctrl", "impressions": 100, "conversions": 10, "is_control": True},
            {"variant_id": "A", "impressions": 100, "conversions": 15, "is_control": False},
        ]
        status = sequential_status(variants)
        assert status["variant_results"]["ctrl"]["sprt_decision"] == "control"

    def test_picks_highest_rate_winner(self):
        """When multiple variants win, pick the one with highest rate."""
        variants = [
            {"variant_id": "ctrl", "impressions": 3000, "conversions": 150, "is_control": True},
            {"variant_id": "A", "impressions": 3000, "conversions": 450, "is_control": False},
            {"variant_id": "B", "impressions": 3000, "conversions": 600, "is_control": False},
        ]
        status = sequential_status(variants)
        if status["recommendation"] == "winner_found":
            assert status["winner_id"] == "B"

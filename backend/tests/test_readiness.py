from app.analytics.readiness import strategy_readiness


def performance(days=2, cycles=4, pnl=2.0, drawdown=0.5, profit_factor=2.0,
                win_rate=60.0, excess=0.2, fees=0.5):
    return {
        "elapsed_hours": days * 24,
        "excess_return_pct": excess,
        "metrics": {
            "cycles": cycles, "realized_pnl": pnl, "fees": fees,
            "realized_max_drawdown_pct": drawdown,
            "profit_factor": profit_factor, "profitable_cycles": max(1, cycles - 1),
            "win_rate_pct": win_rate,
        },
    }


def test_readiness_collects_minimum_evidence_before_scoring():
    result = strategy_readiness(performance())
    assert result["status"] == "COLLECTING_DATA"
    assert result["quality_score_pct"] is None
    assert result["data_progress_pct"] < 100


def test_readiness_passes_only_when_all_criteria_pass():
    result = strategy_readiness(performance(days=8, cycles=25, fees=0.2))
    assert result["status"] == "PASSED"
    assert result["quality_score_pct"] == 100


def test_readiness_fails_after_enough_data_with_failed_criteria():
    result = strategy_readiness(performance(days=8, cycles=25, pnl=-2, excess=-1, fees=1))
    assert result["status"] == "FAILED"
    assert any(not criterion["passed"] for criterion in result["criteria"])

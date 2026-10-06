import json
from ecore_weather.runlog import compact
from ecore_weather.transfer_budget import Budget,BudgetExceeded
import pytest


def test_compact_preserves_metrics_without_repeating_provenance():
    actual=compact({'read_bytes':123,'records':[{'key':'scientific'}],
                    'monthly_archives':[{'assets':[1,2],'stored_bytes':42}]})
    assert actual=={'read_bytes':123,'records_count':1,'monthly_archives':[{'assets_count':2,'stored_bytes':42}]}


def test_transfer_budget_reserves_inflight_and_counts_failed_bytes(tmp_path):
    budget=Budget(tmp_path/'budget.json',100)
    a=budget.reserve(70)
    with pytest.raises(BudgetExceeded):budget.reserve(31)
    budget.settle(a,40)
    b=budget.reserve(60);budget.settle(b) # Opaque failure charged conservatively.
    assert budget.summary()=={'limit':100,'charged':100,'observed':40,'inflight':0,'requests':2}
    with pytest.raises(BudgetExceeded):budget.reserve(1)

import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark


def test_order_totals(test_spark):
    status = TestPipeline.active().run(test_spark)
    assert status.is_success, f"{status.error_class}: {status.error_message}"
    totals = next(info for name, info in status.flow_info.items() if name.endswith(".order_totals"))
    assert totals.records_written == 3


@pytest.mark.parametrize(("value", "expected"), [(2, 4), (3, 6)])
def test_regular_python(value, expected):
    assert value * 2 == expected


@pytest.mark.skip(reason="E2E exercises typed skipped results")
def test_skipped_case():
    raise AssertionError("pytest should not execute a skipped case")


def test_intentional_failure():
    assert 1 == 2, "E2E exercises typed failure and traceback rendering"

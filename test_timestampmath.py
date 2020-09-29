import contextlib
import tempfile
from datetime import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pyarrow as pa

import timestampmath

DefaultParams = {
    "operation": "difference",
    "colnames": [],
    "colname1": None,
    "colname2": None,
    "unit": "day",
    "outcolname": "",
}


def P(**params):
    for p in params.keys():
        assert p in DefaultParams
    return {**DefaultParams, **params}


def assert_arrow_table_equals(actual, expected):
    assert actual.column_names == expected.column_names
    assert [c.type for c in actual.columns] == [c.type for c in expected.columns]
    import pandas  # for to_pylist() to handle ns-precision timestamps

    assert actual.to_pydict() == expected.to_pydict()


def assert_result_equals(actual, expected):
    actual_table, actual_errors = actual
    expected_table, expected_errors = expected
    assert actual_errors == expected_errors
    if expected_table is None:
        assert actual_table is None
    else:
        assert actual_table is not None
        assert_arrow_table_equals(actual_table, expected_table)


@contextlib.contextmanager
def tempfile_context(**kwargs):
    with tempfile.NamedTemporaryFile(**kwargs) as tf:
        yield Path(tf.name)


def render(
    table: pa.Table, params: Dict[str, Any], **kwargs
) -> Tuple[pa.Table, List[Dict[str, Any]]]:
    with tempfile_context(suffix=".arrow") as output_path:
        errors = timestampmath.render(table, params, output_path, **kwargs)
        if output_path.stat().st_size == 0:
            return None, errors
        else:
            with pa.ipc.open_file(output_path) as reader:
                return reader.read_all(), errors


def test_maximum_no_colnames():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([1, 2, 3], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=[], outcolname="B"),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_maximum_no_outcolname():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([1, 2, 3], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=["A"], outcolname=""),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_maximum_one_colname():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([1, 2, 3], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=["A"], outcolname="B"),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                    "B": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_maximum_multiple_colnames():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, 1], pa.timestamp(unit="ns")),
                    "C": pa.array([3, 1, 2], pa.timestamp(unit="ns")),
                }
            ),
            P(operation="maximum", colnames=["A", "B", "C"], outcolname="D"),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, 1], pa.timestamp(unit="ns")),
                    "C": pa.array([3, 1, 2], pa.timestamp(unit="ns")),
                    "D": pa.array([3, 3, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_maximum_reuse_outcolname():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, 1], pa.timestamp(unit="ns")),
                }
            ),
            P(operation="maximum", colnames=["A", "B"], outcolname="A"),
        ),
        (
            pa.table(
                {
                    "B": pa.array([2, 3, 1], pa.timestamp(unit="ns")),
                    "A": pa.array([2, 3, 3], pa.timestamp(unit="ns")),  # added to end
                }
            ),
            [],
        ),
    )


def test_maximum_zero_chunks():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=["A"], outcolname="B"),
        ),
        (
            pa.table(
                {
                    "A": pa.array([], pa.timestamp(unit="ns")),
                    "B": pa.array([], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_maximum_nulls():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, None, 3, None], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, None, None], pa.timestamp(unit="ns")),
                }
            ),
            P(operation="maximum", colnames=["A", "B"], outcolname="C"),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, None, 3, None], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, None, None], pa.timestamp(unit="ns")),
                    "C": pa.array([2, 3, 3, None], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_minimum():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, None, 3, None], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, None, None], pa.timestamp(unit="ns")),
                }
            ),
            P(operation="minimum", colnames=["A", "B"], outcolname="C"),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, None, 3, None], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, None, None], pa.timestamp(unit="ns")),
                    "C": pa.array([1, 3, 3, None], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_difference_no_colnames():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            P(
                operation="difference",
                colname1="",
                colname2="",
                unit="day",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_difference_no_colname1():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            P(
                operation="difference",
                colname1="",
                colname2="B",
                unit="day",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_difference_no_colname2():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="",
                unit="day",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_difference_no_outcolname():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="day",
                outcolname="",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
            [],
        ),
    )


def test_difference_days():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1), dt(2020, 3, 2)], pa.timestamp(unit="ns")
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1), dt(2020, 1, 2)], pa.timestamp(unit="ns")
                    ),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="day",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1), dt(2020, 3, 2)], pa.timestamp(unit="ns")
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1), dt(2020, 1, 2)], pa.timestamp(unit="ns")
                    ),
                    "C": pa.array([365.0, -60.0]),
                }
            ),
            [],
        ),
    )


def test_difference_reuse_outcolname():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1), dt(2020, 3, 2)], pa.timestamp(unit="ns")
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1), dt(2020, 1, 2)], pa.timestamp(unit="ns")
                    ),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="day",
                outcolname="A",
            ),
        ),
        (
            pa.table(
                {
                    "B": pa.array(
                        [dt(2020, 1, 1), dt(2020, 1, 2)], pa.timestamp(unit="ns")
                    ),
                    "A": pa.array([365.0, -60.0]),  # appended to table
                }
            ),
            [],
        ),
    )


def test_difference_fractional():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1, 0), dt(2020, 3, 2, 0, 0)],
                        pa.timestamp(unit="ns"),
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1, 12), dt(2020, 1, 2, 0, 45)],
                        pa.timestamp(unit="ns"),
                    ),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="day",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1, 0), dt(2020, 3, 2, 0, 0)],
                        pa.timestamp(unit="ns"),
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1, 12), dt(2020, 1, 2, 0, 45)],
                        pa.timestamp(unit="ns"),
                    ),
                    "C": pa.array([365.5, -59.96875]),
                }
            ),
            [],
        ),
    )


def test_difference_seconds():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1, 12, 1, 1), dt(2020, 3, 2, 8, 1, 1)],
                        pa.timestamp(unit="ns"),
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1, 2, 3, 4), dt(2020, 1, 2, 1, 2, 3)],
                        pa.timestamp(unit="ns"),
                    ),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="second",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array(
                        [dt(2019, 1, 1, 12, 1, 1), dt(2020, 3, 2, 8, 1, 1)],
                        pa.timestamp(unit="ns"),
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1, 2, 3, 4), dt(2020, 1, 2, 1, 2, 3)],
                        pa.timestamp(unit="ns"),
                    ),
                    "C": pa.array([31500123.0, -5209138.0]),
                }
            ),
            [],
        ),
    )


def test_difference_nulls():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array(
                        [None, dt(2020, 3, 2), None], pa.timestamp(unit="ns")
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1), None, None], pa.timestamp(unit="ns")
                    ),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="day",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array(
                        [None, dt(2020, 3, 2), None], pa.timestamp(unit="ns")
                    ),
                    "B": pa.array(
                        [dt(2020, 1, 1), None, None], pa.timestamp(unit="ns")
                    ),
                    "C": pa.array([None, None, None], pa.float64()),
                }
            ),
            [],
        ),
    )


def test_difference_nanoseconds():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array(
                        [1237342345234234234, 1230000034123123423, None, None],
                        pa.timestamp("ns"),
                    ),
                    "B": pa.array(
                        [1237343345234134214, 1230080234113143429, 123, None],
                        pa.timestamp("ns"),
                    ),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="nanosecond",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array(
                        [1237342345234234234, 1230000034123123423, None, None],
                        pa.timestamp("ns"),
                    ),
                    "B": pa.array(
                        [1237343345234134214, 1230080234113143429, 123, None],
                        pa.timestamp("ns"),
                    ),
                    "C": pa.array([999999899980, 80199990020006, None, None]),
                }
            ),
            [],
        ),
    )


def test_difference_nanoseconds_null():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.array([123, None, None], pa.timestamp("ns")),
                    "B": pa.array([None, 123, None], pa.timestamp("ns")),
                }
            ),
            P(
                operation="difference",
                colname1="A",
                colname2="B",
                unit="nanosecond",
                outcolname="C",
            ),
        ),
        (
            pa.table(
                {
                    "A": pa.array([123, None, None], pa.timestamp("ns")),
                    "B": pa.array([None, 123, None], pa.timestamp("ns")),
                    "C": pa.array([None, None, None], pa.int64()),
                }
            ),
            [],
        ),
    )

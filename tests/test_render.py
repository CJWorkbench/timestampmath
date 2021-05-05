from datetime import datetime as dt
from pathlib import Path

import pyarrow as pa
from cjwmodule.arrow.testing import assert_result_equals, make_column, make_table
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.spec.testing import param_factory
from cjwmodule.testing.i18n import i18n_message
from cjwmodule.types import RenderError

from timestampmath import render_arrow_v1 as render

P = param_factory(Path(__file__).parent.parent / "timestampmath.yaml")


def test_maximum_no_colnames():
    assert_result_equals(
        render(
            make_table(make_column("A", [1, 2, 3], pa.timestamp(unit="ns"))),
            P(operation="maximum", colnames=[], outcolname="B"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [1, 2, 3], pa.timestamp(unit="ns"))),
        ),
    )


def test_maximum_no_outcolname():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([1, 2, 3], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=["A"], outcolname=""),
        ),
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                }
            ),
        ),
    )


def test_maximum_one_colname():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([1, 2, 3], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=["A"], outcolname="B"),
        ),
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                    "B": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2, 3], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, 1], pa.timestamp(unit="ns")),
                    "C": pa.array([3, 1, 2], pa.timestamp(unit="ns")),
                    "D": pa.array([3, 3, 3], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "B": pa.array([2, 3, 1], pa.timestamp(unit="ns")),
                    "A": pa.array([2, 3, 3], pa.timestamp(unit="ns")),  # added to end
                }
            ),
        ),
    )


def test_maximum_zero_chunks():
    assert_result_equals(
        render(
            pa.table({"A": pa.array([], pa.timestamp(unit="ns"))}),
            P(operation="maximum", colnames=["A"], outcolname="B"),
        ),
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([], pa.timestamp(unit="ns")),
                    "B": pa.array([], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, None, 3, None], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, None, None], pa.timestamp(unit="ns")),
                    "C": pa.array([2, 3, 3, None], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, None, 3, None], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3, None, None], pa.timestamp(unit="ns")),
                    "C": pa.array([1, 3, 3, None], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([1, 2], pa.timestamp(unit="ns")),
                    "B": pa.array([2, 3], pa.timestamp(unit="ns")),
                }
            ),
        ),
    )


def test_difference_no_chunks():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.chunked_array([], pa.timestamp(unit="ns")),
                    "B": pa.chunked_array([], pa.timestamp(unit="ns")),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.chunked_array([], pa.timestamp(unit="ns")),
                    "B": pa.chunked_array([], pa.timestamp(unit="ns")),
                    "C": pa.chunked_array([], pa.float64()),
                }
            ),
        ),
    )


def test_difference_no_chunks_nanoseconds():
    assert_result_equals(
        render(
            pa.table(
                {
                    "A": pa.chunked_array([], pa.timestamp(unit="ns")),
                    "B": pa.chunked_array([], pa.timestamp(unit="ns")),
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.chunked_array([], pa.timestamp(unit="ns")),
                    "B": pa.chunked_array([], pa.timestamp(unit="ns")),
                    "C": pa.chunked_array([], pa.int64()),
                }
            ),
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
        ArrowRenderResult(
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
        ArrowRenderResult(
            pa.table(
                {
                    "B": pa.array(
                        [dt(2020, 1, 1), dt(2020, 1, 2)], pa.timestamp(unit="ns")
                    ),
                    "A": pa.array([365.0, -60.0]),  # appended to table
                }
            ),
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
        ArrowRenderResult(
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
        ArrowRenderResult(
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
        ArrowRenderResult(
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
        ArrowRenderResult(
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
        ArrowRenderResult(
            pa.table(
                {
                    "A": pa.array([123, None, None], pa.timestamp("ns")),
                    "B": pa.array([None, 123, None], pa.timestamp("ns")),
                    "C": pa.array([None, None, None], pa.int64()),
                }
            ),
        ),
    )


def test_startof_hour():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A",
                    [
                        dt(2021, 5, 5, 13, 1, 22, 321231),
                        dt(1800, 1, 1, 1, 2, 3, 4),
                        None,
                    ],
                )
            ),
            P(operation="startof", colnames=["A"], roundunit="hour"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [dt(2021, 5, 5, 13), dt(1800, 1, 1, 1), None]))
        ),
    )


def test_startof_out_of_bounds():
    result = render(
        make_table(
            make_column(
                "A",
                [dt(1970, 1, 1), dt(1677, 9, 21, 0, 12, 43, 145500)],
            )
        ),
        P(operation="startof", colnames=["A"], roundunit="minute"),
    )
    print(repr(result.table["A"]))
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A",
                    [dt(1970, 1, 1), dt(1677, 9, 21, 0, 12, 43, 145500)],
                )
            ),
            P(operation="startof", colnames=["A"], roundunit="minute"),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [dt(1970, 1, 1), None])),
            [
                RenderError(
                    i18n_message(
                        "warning.convertedOutOfBoundsToNull",
                        {"timestamp": "1677-09-21T00:12Z"},
                    )
                )
            ],
        ),
    )

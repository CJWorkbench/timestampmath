from typing import List, NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.compute
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.i18n import trans
from cjwmodule.types import RenderError

_NS_PER_UNIT = {
    "nanosecond": 1,
    "microsecond": 1000,
    "millisecond": 1000000,
    "second": 1000000000,
    "minute": 60 * 1000000000,
    "hour": 3600 * 1000000000,
    "day": 86400 * 1000000000,
}


def migrate_params(params):
    if "roundunit" not in params:
        params = _migrate_params_v0_to_v1(params)
    return params


def _migrate_params_v0_to_v1(params):
    """v0 has no roundunit. v1 has it, default="hour"."""
    return {**params, "roundunit": "hour"}


def _render_minimum_or_maximum(table, colnames, outcolname, fn):
    if not colnames:
        return ArrowRenderResult(table)

    out_np_arrays = []

    num_chunks = table[colnames[0]].num_chunks
    for chunk in range(num_chunks):
        in_np_arrays = [
            table[colname].chunk(chunk).to_numpy(zero_copy_only=False)
            for colname in colnames
        ]
        out_np_array = fn.reduce(in_np_arrays)
        out_np_arrays.append(out_np_array)

    if outcolname in table.column_names:
        table = table.remove_column(table.column_names.index(outcolname))

    table = table.append_column(
        outcolname, pa.chunked_array(out_np_arrays, pa.timestamp("ns"))
    )
    return ArrowRenderResult(table)


def _render_maximum(table, colnames, outcolname):
    return _render_minimum_or_maximum(table, colnames, outcolname, np.fmax)


def _render_minimum(table, colnames, outcolname):
    return _render_minimum_or_maximum(table, colnames, outcolname, np.fmin)


def _render_difference(table, colname1, colname2, unit, outcolname):
    if not colname1 or not colname2:
        return ArrowRenderResult(table)

    out_arrays = []
    if unit == "nanosecond":
        out_type = pa.int64()
        out_metadata = {"format": "{:,d}"}
    else:
        out_type = pa.float64()
        out_metadata = {"format": "{:,}"}
    num_chunks = table[colname1].num_chunks
    for chunk in range(num_chunks):
        chunk1 = table[colname1].chunk(chunk).cast(pa.int64())
        chunk2 = table[colname2].chunk(chunk).cast(pa.int64())
        # TODO subtract_checked and report error
        difference_in_ns = pa.compute.subtract(chunk2, chunk1)

        if unit == "nanosecond":
            # Nanosecond differences are integers
            out_array = difference_in_ns
        else:
            out_array = pa.compute.divide(
                difference_in_ns.cast(pa.float64(), safe=False),
                pa.scalar(_NS_PER_UNIT[unit], pa.float64()),
            )
        out_arrays.append(out_array)

    if outcolname in table.column_names:
        table = table.remove_column(table.column_names.index(outcolname))

    table = table.append_column(
        pa.field(outcolname, out_type, metadata=out_metadata),
        pa.chunked_array(out_arrays, out_type),
    )

    return ArrowRenderResult(table)


class StartofColumnResult(NamedTuple):
    column: pa.ChunkedArray
    truncated: bool


def _startof(column: pa.ChunkedArray, unit: str) -> StartofColumnResult:
    factor = pa.scalar(_NS_PER_UNIT[unit], pa.int64())
    timestamp_ints = column.cast(pa.int64())

    # In two's complement, truncation rounds _up_. Subtract before truncating.
    #
    # In decimal, if we're truncating to the nearest 10:
    #
    # 0 => 0
    # -1 => -10
    # -9 => -10
    # -10 => -10
    # -11 => -20
    #
    # ... rule is: subtract 9 from all negative numbers, then truncate.

    negative = pa.compute.less(timestamp_ints, pa.scalar(0, pa.int64()))
    # "offset": -9 for negatives, 0 for others
    offset = pa.compute.multiply(
        negative.cast(pa.int64()), pa.scalar(-1 * _NS_PER_UNIT[unit] + 1, pa.int64())
    )
    # to_truncate may overflow; in that case, to_truncate > timestamp_ints
    to_truncate = pa.compute.add(timestamp_ints, offset)
    truncated = pa.compute.multiply(pa.compute.divide(to_truncate, factor), factor)

    # Mask of [True, None, True, True, None]
    safe_or_null = pa.compute.or_kleene(
        pa.compute.less_equal(to_truncate, timestamp_ints), pa.scalar(None, pa.bool_())
    )

    truncated_or_null = truncated.filter(
        safe_or_null, null_selection_behavior="emit_null"
    )

    return StartofColumnResult(
        column=truncated_or_null.cast(pa.timestamp("ns")),
        truncated=(truncated_or_null.null_count > column.null_count),
    )


def _out_of_bounds_timestamp(unit: str):
    return {
        "microsecond": "1677-09-21T00:12:43.145224Z",
        "millisecond": "1677-09-21T00:12:43.145Z",
        "second": "1677-09-21T00:12:43Z",
        "minute": "1677-09-21T00:12Z",
        "hour": "1677-09-21T00:00Z",
    }[unit]


def _render_startof(
    table: pa.Table, colnames: List[str], unit: str
) -> ArrowRenderResult:
    truncated = False
    for colname in colnames:
        i = table.column_names.index(colname)
        column_result = _startof(table.columns[i], unit)
        table = table.set_column(i, colname, column_result.column)
        if column_result.truncated:
            truncated = True

    if truncated:
        errors = [
            RenderError(
                trans(
                    "warning.convertedOutOfBoundsToNull",
                    "Converted timestamp {timestamp} to null because it is out of bounds.",
                    {"timestamp": _out_of_bounds_timestamp(unit)},
                )
            )
        ]
    else:
        errors = []

    return ArrowRenderResult(table, errors=errors)


def render_arrow_v1(table: pa.Table, params, **kwargs):
    operation = params["operation"]

    if operation in {"minimum", "maximum", "difference"} and not params["outcolname"]:
        # TODO warning? Default parameter?
        return ArrowRenderResult(table)

    if operation == "minimum":
        return _render_minimum(table, params["colnames"], params["outcolname"])
    elif operation == "maximum":
        return _render_maximum(table, params["colnames"], params["outcolname"])
    elif operation == "startof":
        return _render_startof(table, params["colnames"], params["roundunit"])
    else:
        return _render_difference(
            table,
            params["colname1"],
            params["colname2"],
            params["unit"],
            params["outcolname"],
        )

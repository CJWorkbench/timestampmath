import numpy as np
import pyarrow as pa

_NS_PER_UNIT = {
    "nanosecond": 1,
    "microsecond": 1000,
    "millisecond": 1000000,
    "second": 1000000000,
    "minute": 60 * 1000000000,
    "hour": 3600 * 1000000000,
    "day": 86400 * 1000000000,
}


def _render_minimum_or_maximum(arrow_table, colnames, outcolname, fn):
    if not colnames:
        return arrow_table, []

    out_np_arrays = []

    num_chunks = arrow_table[colnames[0]].num_chunks
    for chunk in range(num_chunks):
        in_np_arrays = [
            arrow_table[colname].chunk(chunk).to_numpy(zero_copy_only=False)
            for colname in colnames
        ]
        out_np_array = fn.reduce(in_np_arrays)
        out_np_arrays.append(out_np_array)

    if outcolname in arrow_table.column_names:
        arrow_table = arrow_table.remove_column(
            arrow_table.column_names.index(outcolname)
        )

    return (
        arrow_table.append_column(
            outcolname, pa.chunked_array(out_np_arrays, pa.timestamp("ns"))
        ),
        [],
    )


def _render_maximum(arrow_table, colnames, outcolname):
    return _render_minimum_or_maximum(arrow_table, colnames, outcolname, np.fmax)


def _render_minimum(arrow_table, colnames, outcolname):
    return _render_minimum_or_maximum(arrow_table, colnames, outcolname, np.fmin)


def _render_difference(arrow_table, colname1, colname2, unit, outcolname):
    if not colname1 or not colname2:
        return arrow_table, []

    out_arrays = []
    num_chunks = arrow_table[colname1].num_chunks
    for chunk in range(num_chunks):
        in_np_array1 = arrow_table[colname1].chunk(chunk).to_numpy(zero_copy_only=False)
        in_np_array2 = arrow_table[colname2].chunk(chunk).to_numpy(zero_copy_only=False)
        out_np_timedelta_array = in_np_array2 - in_np_array1
        if unit == "nanosecond":
            # Nanosecond differences are integers
            out_array = pa.array(
                out_np_timedelta_array.astype("int"),
                mask=np.isnat(out_np_timedelta_array),
            )
        else:
            out_array = pa.array(
                out_np_timedelta_array.astype("float64") / _NS_PER_UNIT[unit],
                mask=np.isnat(out_np_timedelta_array),
            )
        out_arrays.append(out_array)

    if outcolname in arrow_table.column_names:
        arrow_table = arrow_table.remove_column(
            arrow_table.column_names.index(outcolname)
        )

    return (
        arrow_table.append_column(outcolname, pa.chunked_array(out_arrays)),
        [],
    )


def render(arrow_table, params, output_path, **kwargs):
    operation = params["operation"]

    if not params["outcolname"]:
        # TODO warning? Default parameter?
        table, errors = arrow_table, []
    elif operation == "minimum":
        table, errors = _render_minimum(
            arrow_table, params["colnames"], params["outcolname"]
        )
    elif operation == "maximum":
        table, errors = _render_maximum(
            arrow_table, params["colnames"], params["outcolname"]
        )
    else:
        table, errors = _render_difference(
            arrow_table,
            params["colname1"],
            params["colname2"],
            params["unit"],
            params["outcolname"],
        )

    if table is not None:
        with pa.ipc.RecordBatchFileWriter(output_path, table.schema) as writer:
            writer.write_table(table)

    return errors

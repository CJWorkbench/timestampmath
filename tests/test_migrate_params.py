from pathlib import Path

from cjwmodule.spec.testing import param_factory

from timestampmath import migrate_params

P = param_factory(Path(__file__).parent.parent / "timestampmath.yaml")


def test_v0_to_v1():
    assert migrate_params(
        dict(
            operation="difference",
            colnames=["A", "B"],
            colname1="C",
            colname2="D",
            unit="minute",
            outcolname="froop",
        )
    ) == P(
        operation="difference",
        colnames=["A", "B"],
        colname1="C",
        colname2="D",
        unit="minute",
        roundunit="hour",
        outcolname="froop",
    )

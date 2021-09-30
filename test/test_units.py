from unittest.mock import Mock

import pytest

from main import main


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        ("2021-01-01", "2021-03-01"),
    ],
    ids=["auto", "manual"],
)
@pytest.mark.timeout(0)
def test_pipelines(start, end):
    data = {
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]

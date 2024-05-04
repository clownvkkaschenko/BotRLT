import pytest

from algorithm import aggregator
from example import DAY, EXAMPLE_DAY, EXAMPLE_HOUR, EXAMPLE_MONTH, HOUR, MONTH


@pytest.mark.asyncio
async def test_async_function():
    month = await aggregator.aggregate_data(*MONTH.values())
    day = await aggregator.aggregate_data(*DAY.values())
    hour = await aggregator.aggregate_data(*HOUR.values())

    assert month == EXAMPLE_MONTH
    assert day == EXAMPLE_DAY
    assert hour == EXAMPLE_HOUR

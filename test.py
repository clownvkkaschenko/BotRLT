from algorithm import aggregator
from example import DAY, EXAMPLE_DAY, EXAMPLE_HOUR, EXAMPLE_MONTH, HOUR, MONTH

assert aggregator.aggregate_data(*MONTH.values()) == EXAMPLE_MONTH
assert aggregator.aggregate_data(*DAY.values()) == EXAMPLE_DAY
assert aggregator.aggregate_data(*HOUR.values()) == EXAMPLE_HOUR
print('Проверка прошла!')

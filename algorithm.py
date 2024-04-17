import os
from datetime import datetime

import pymongo
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()
client_uri = os.environ.get('CLIENT_URI')
db_name = os.environ.get('DB_NAME')
collection_name = os.environ.get('COLLECTION_NAME')


class SalaryAggregator:
    """
    Класс для сбора статистических данных о зарплатах
    сотрудников компании по временным промежуткам.
    """
    def __init__(
            self, client_uri=client_uri,
            db_name=db_name, collection_name=collection_name
    ):
        self.client = pymongo.MongoClient(client_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # Словарь с настройками данных для разных типов агрегации.
        self.settings = {
            'day': {
                'date_format': '%Y-%m-%d',
                'time_format': 'T00:00:00',
                'dateToString': {'format': '%Y-%m-%d', 'date': '$dt'},
            },
            'hour': {
                'date_format': '%Y-%m-%dT%H',
                'time_format': ':00:00',
                'dateToString': {'format': '%Y-%m-%dT%H', 'date': '$dt'}
            },
            'month': {
                'date_format': '%Y-%m',
                'time_format': '-01T00:00:00',
                'dateToString': {'format': '%Y-%m', 'date': '$dt'}
            }
        }

    def _daterange(self, start_date, end_date, aggregation_type):
        """Метод для получения всех дат в заданном диапазоне."""
        range_stop = {
            'day': int((end_date - start_date).days),
            'hour': (int((end_date - start_date).total_seconds())) // 3600,
            'month': (
                relativedelta(end_date, start_date).years * 12 +
                relativedelta(end_date, start_date).months
            ),
        }

        for i in range(range_stop[aggregation_type] + 1):
            interval = relativedelta(**{aggregation_type + 's': i})
            yield (start_date + interval).strftime(self.settings[aggregation_type]['date_format'])

    def _missing_dates(self, start_date, end_date, aggregation_type):
        """
        Метод для получения всех дат, которые есть в заданном диапазоне,
        но отсутствуют в коллекции.
        """
        all_dates = set(self._daterange(start_date, end_date, aggregation_type))

        pipeline = [
            {
                '$match': {
                    'dt': {'$gte': start_date, '$lte': end_date}
                }
            },
            {
                '$group': {
                    '_id': {'$dateToString': self.settings[aggregation_type]['dateToString']},
                }
            }
        ]
        dates_from_db = set(i['_id'] for i in self.collection.aggregate(pipeline))

        return list(all_dates.difference(dates_from_db))

    def aggregate_data(self, start_date, end_date, aggregation_type):
        """Метод для получения конечной информации."""
        missing_dates_lst = self._missing_dates(start_date, end_date, aggregation_type)

        pipeline = [
            {
                '$match': {
                    'dt': {'$gte': start_date, '$lte': end_date}
                }
            },
            {
                '$group': {
                    '_id': {'$dateToString': self.settings[aggregation_type]['dateToString']},
                    'total': {'$sum': '$value'}
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ]

        # Если в диапазоне есть даты, которые отсутсвуют в коллекции, то создаётся новая коллекция
        # с этими датами, и добавляется к датам из коллекции.
        if missing_dates_lst:
            name_collection = f'temp_collection {datetime.now()}'
            temp_collection = self.db[name_collection]
            for date in missing_dates_lst:
                temp_collection.insert_one(
                    {'dt': datetime.strptime(date, self.settings[aggregation_type]['date_format']),
                     'value': 0
                     }
                )

            pipeline.insert(1, {'$unionWith': {'coll': name_collection}})

        result = {'dataset': [], 'labels': []}
        cursor = self.collection.aggregate(pipeline)

        for doc in cursor:
            result['dataset'].append(doc['total'])
            result['labels'].append(doc['_id'] + self.settings[aggregation_type]['time_format'])

        # Удаляем колекцию, созданную для отсутствующих дат.
        if missing_dates_lst:
            temp_collection.drop()
        return result


aggregator = SalaryAggregator()

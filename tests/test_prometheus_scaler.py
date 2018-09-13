# from unittest.mock import patch, Mock, call

import requests_mock
from freezegun import freeze_time

from app.prometheus_scaler import PrometheusScaler


class TestPrometheusScaler:
    input_attrs = {
        'min_instances': 1,
        'max_instances': 2,
        'threshold': 1500,
        'PrometheusScaler': {
            'metric_name': 'requests',
            'labels': {
                'app': 'notify-admin',
                'exported_space': 'test',
                'organisation': 'govuk-notify',
            }
        }
    }

    @freeze_time("2018-09-13 13:50:00")
    def test_requests_to_prometheus(self):
        prometheus_scaler = PrometheusScaler(**self.input_attrs)
        with requests_mock.mock() as m:
            m.get("https://prom-1.monitoring.gds-reliability.engineering/api/v1/query_range?query=sum(increase(requests%7Bapp=%22notify-admin%22,exported_space=%22test%22,organisation=%22govuk-notify%22%7D[60s]))&start=1536842700&end=1536843000&step=15")  # noqa
            prometheus_scaler._query_prometheus()
            print(m.request_history)

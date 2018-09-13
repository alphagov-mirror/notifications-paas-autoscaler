import math

import requests

from app.base_scalers import BaseScaler
from app.config import config


class PrometheusScaler(BaseScaler):
    prometheus_api = "{}{}".format(config['SCALERS']['PROMETHEUS_URL'], "/api/v1")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = kwargs['PrometheusScaler']['metric_name']
        self.labels = kwargs['PrometheusScaler']['labels']
        self.request_count_time_range = kwargs['PrometheusScaler'].get('request_count_time_range', 300)

    def _text_from_labels(self):
        return ",".join(["{}=\"{}\"".format(key, value) for key, value in self.labels.items()])

    def _build_query_string(self):
        labels_text = self._text_from_labels()
        return "query=sum(increase({metric}{{{labels}}}[60s]))&start={start_timestamp}&end={end_timestamp}&step=15".format(  # noqa
            metric=self.metric_name,
            labels=labels_text,
            start_timestamp=int(self._now().timestamp() - self.request_count_time_range),
            end_timestamp=int(self._now().timestamp())
        )

    def _query_prometheus(self):
        query_string = self._build_query_string()
        url = "{prometheus_api}/{endpoint}?{query_string}".format(
            prometheus_api=self.prometheus_api,
            endpoint="query_range",
            query_string=query_string
        )

        res = requests.get(url)
        return res

    def get_desired_instance_count(self):
        prometheus_response = self._query_prometheus()
        self._get_items_from_response(prometheus_response)
        scheduled_items = self.run_query()
        scale_items = scheduled_items * self.scheduled_items_factor
        desired_instance_count = int(math.ceil(scale_items / float(self.threshold)))
        return self.normalize_desired_instance_count(desired_instance_count)

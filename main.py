import os
import boto3
import base64
import math
import sched
import time
import random
import datetime
from functools import reduce
from cloudfoundry_client.client import CloudFoundryClient

class SQSApp:
    def __init__(self, name, queues, messages_per_instance, min_instance_count, max_instance_count):
        self.name = name
        self.queues = queues
        self.messages_per_instance = messages_per_instance
        self.min_instance_count = min_instance_count
        self.max_instance_count = max_instance_count

class ELBApp:
    def __init__(self, name, load_balancer_name, request_per_instance, min_instance_count, max_instance_count):
        self.name = name
        self.load_balancer_name = load_balancer_name
        self.request_per_instance = request_per_instance
        self.min_instance_count = min_instance_count
        self.max_instance_count = max_instance_count

class AutoScaler:
    def __init__(self, sqs_apps, elb_apps):
        self.sqs_apps = sqs_apps
        self.elb_apps = elb_apps

        self.schedule_interval = int(os.environ['SCHEDULE_INTERVAL']) + 0.0
        self.schedule_delay = random.random() * self.schedule_interval
        self.scheduler = sched.scheduler(time.time, time.sleep)

        self.aws_region = os.environ['AWS_REGION']
        self.aws_account_id = boto3.client('sts', region_name=self.aws_region).get_caller_identity()['Account']

        self.sqs_client = boto3.client('sqs', region_name=self.aws_region)
        self.sqs_queue_prefix = os.environ['SQS_QUEUE_PREFIX']

        self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.aws_region)

        self.cf_username = os.environ['CF_USERNAME']
        self.cf_password = os.environ['CF_PASSWORD']
        self.cf_api_url = os.environ['CF_API_URL']
        self.cf_org = os.environ['CF_ORG']
        self.cf_space = os.environ['CF_SPACE']
        self.cf_client = None

    def get_cloudfoundry_client(self):
        if self.cf_client is None:
            proxy = dict(http=os.environ.get('HTTP_PROXY', ''), https=os.environ.get('HTTPS_PROXY', ''))
            cf_client = CloudFoundryClient(self.cf_api_url, proxy=proxy)
            try:
                cf_client.init_with_user_credentials(self.cf_username, self.cf_password)
                self.cf_client = cf_client
            except BaseException as e:
                print('Failed to authenticate: {}, waiting 5 minutes and exiting'.format(str(e)))
                # The sleep is added to avoid automatically banning the user for too many failed login attempts
                time.sleep(5 * 60)

        return self.cf_client

    def reset_cloudfoundry_client(self):
        self.cf_client = None

    def get_paas_apps(self):
        instances = {}

        cf_client = self.get_cloudfoundry_client()
        if cf_client is not None:
            try:
                for organization in self.cf_client.organizations:
                    if organization['entity']['name'] != self.cf_org:
                        continue
                    for space in organization.spaces():
                        if space['entity']['name'] != self.cf_space:
                            continue
                        for app in space.apps():
                            instances[app['entity']['name']] = {
                                'name': app['entity']['name'],
                                'guid': app['metadata']['guid'],
                                'instances': app['entity']['instances']
                            }
            except BaseException as e:
                print('Failed to get stats for app {}: {}'.format(app['entity']['name'], str(e)))
                self.reset_cloudfoundry_client()

        return instances

    def get_sqs_queue_name(self, name):
        return "{}{}".format(self.sqs_queue_prefix, name)

    def get_sqs_queue_url(self, name):
        return "https://sqs.{}.amazonaws.com/{}/{}".format(
            self.aws_region, self.aws_account_id, name)

    def get_sqs_message_count(self, name):
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=self.get_sqs_queue_url(name),
            AttributeNames=['ApproximateNumberOfMessages'])
        result = int(response['Attributes']['ApproximateNumberOfMessages'])
        print('Messages in {}: {}'.format(name, result))
        return result

    def get_highest_message_count(self, queues):
        result = 0
        for queue in queues:
            result = max(result, self.get_sqs_message_count(self.get_sqs_queue_name(queue)))
        return result

    def scale_sqs_app(self, app, paas_app):
        print('Processing {}'.format(app.name))
        highest_message_count = self.get_highest_message_count(app.queues)
        print('Highest message count: {}'.format(highest_message_count))
        desired_instance_count = int(math.ceil(highest_message_count / float(app.messages_per_instance)))

        self.scale_paas_apps(app, paas_app, paas_app['instances'], desired_instance_count)

    def get_load_balancer_request_counts(self, load_balancer_name):
        start_time = datetime.datetime.now() - datetime.timedelta(minutes=5)
        end_time = datetime.datetime.now()
        result = self.cloudwatch_client.get_metric_statistics(
            Namespace='AWS/ELB',
            MetricName='RequestCount',
            Dimensions=[
                {
                    'Name': 'LoadBalancerName',
                    'Value': load_balancer_name
                },
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Sum'],
            Unit='Count'
        )
        datapoints = result['Datapoints']
        datapoints = sorted(datapoints,key=lambda x: x['Timestamp'])
        return [row['Sum'] for row in datapoints]

    def scale_elb_app(self, app, paas_app):
        print('Processing {}'.format(app.name))
        request_counts = self.get_load_balancer_request_counts(app.load_balancer_name)
        if len(request_counts) == 0:
            request_counts = [0]
        print('Request counts (5 min): {}'.format(request_counts))

        # We make sure we keep the highest instance count for 5 minutes
        highest_request_count = max(request_counts)
        print('Highest request count (5 min): {}'.format(highest_request_count))

        desired_instance_count = int(math.ceil(highest_request_count / float(app.request_per_instance)))

        self.scale_paas_apps(app, paas_app, paas_app['instances'], desired_instance_count)

    def scale_paas_apps(self, app, paas_app, current_instance_count, desired_instance_count):
        desired_instance_count = min(app.max_instance_count, desired_instance_count)
        desired_instance_count = max(app.min_instance_count, desired_instance_count)

        # Make sure we don't remove more than 2 instances at a time
        if desired_instance_count < current_instance_count and current_instance_count - desired_instance_count > 2:
            desired_instance_count = current_instance_count - 2

        print('Current/desired instance count: {}/{}'.format(current_instance_count, desired_instance_count))
        if current_instance_count != desired_instance_count:
            print('Scaling {} from {} to {}'.format(app.name, current_instance_count, desired_instance_count))
            try:
                self.cf_client.apps._update(paas_app['guid'], {'instances': desired_instance_count})
            except BaseException as e:
                print('Failed to scale {}: {}'.format(app.name, str(e)))

    def schedule(self):
        current_time = time.time()
        run_at = current_time + self.schedule_interval - ((current_time - self.schedule_delay) % self.schedule_interval)
        self.scheduler.enterabs(run_at, 1, self.run_task)

    def run_task(self):
        paas_apps = self.get_paas_apps()

        for app in self.sqs_apps:
            if not app.name in paas_apps:
                print("Application {} does not exist".format(app.name))
                continue
            self.scale_sqs_app(app, paas_apps[app.name])

        for app in self.elb_apps:
            if not app.name in paas_apps:
                print("Application {} does not exist".format(app.name))
                continue
            self.scale_elb_app(app, paas_apps[app.name])

        self.schedule()

    def run(self):
        print('API endpoint:   {}'.format(self.cf_api_url))
        print('User:           {}'.format(self.cf_username))
        print('Org:            {}'.format(self.cf_org))
        print('Space:          {}'.format(self.cf_space))

        self.schedule()
        while True:
            self.scheduler.run()

min_instance_count = int(os.environ['CF_MIN_INSTANCE_COUNT'])

sqs_apps = []
sqs_apps.append(SQSApp('notify-delivery-worker-database', ['db-sms','db-email','db-letter'], 2000, min_instance_count, 20))
sqs_apps.append(SQSApp('notify-delivery-worker', ['notify', 'retry', 'process-job', 'periodic'], 2000, min_instance_count, 20))
sqs_apps.append(SQSApp('notify-delivery-worker-sender', ['send-sms','send-email'], 2000, min_instance_count, 20))
sqs_apps.append(SQSApp('notify-delivery-worker-research', ['research-mode'], 2000, min_instance_count, 20))

elb_apps = []
elb_apps.append(ELBApp('notify-api', 'notify-paas-proxy', 1500, min_instance_count, 20))

autoscaler = AutoScaler(sqs_apps, elb_apps)
autoscaler.run()
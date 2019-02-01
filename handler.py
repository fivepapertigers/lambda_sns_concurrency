"""
    A lambda handler for profiling concurrent SNS publishing
"""
import os
import json
from functools import partial
from time import time
from concurrent.futures import ThreadPoolExecutor

import boto3


SNS = boto3.client('sns')


def publish(topic_arn, msg):
    """ Publish """
    SNS.publish(Message=json.dumps(msg), TopicArn=topic_arn)


def bulk_publish(max_workers, message_count, topic_arn):
    """ Bulk publish with concurrency """

    # create dummy messages
    messages = ({'start_time': '12345', 'end_time': '234567'}
                for _ in range(message_count))

    # Generate a worker that only requires the message param
    publish_worker = partial(publish, topic_arn)

    # Store start time
    start_time = time()


    # If only using one worker, do not require the overhead of using the
    # ThreadPoolExecutor
    if max_workers == 1:
        iteration = (publish_worker({'topic_arn': topic_arn,
                                     'msg': msg}) for msg in messages)
    else:
        executor = ThreadPoolExecutor(max_workers=max_workers)
        iteration = executor.map(publish_worker, messages)

    # Perform iteration
    list(iteration)

    # Return the time difference
    return time() - start_time


def lambda_handler(event, __):
    """ The lambda handler """
    message_count = event['message_count']
    max_workers = event['max_workers']
    topic_arn = event['topic_arn']
    return {'max_workers': max_workers,
            'time_elapsed': bulk_publish(max_workers=max_workers,
                                         message_count=message_count,
                                         topic_arn=topic_arn),
            'message_count': message_count,
            'memory': os.environ['AWS_LAMBDA_FUNCTION_MEMORY_SIZE']}

"""
    Profile Concurrent SNS Publishing in AWS Lambda
"""

from argparse import ArgumentParser
import json

import boto3

LAMBDA = boto3.client('lambda')


def run_trial(message_count, max_workers, function_name, topic_arn):
    """ Runs trials and yields each result """
    resp = LAMBDA.invoke(FunctionName=function_name,
                         Payload=json.dumps({'message_count': message_count,
                                             'max_workers': max_workers,
                                             'topic_arn': topic_arn}))
    payload = resp['Payload'].read().decode('utf-8')
    return json.loads(payload)


def run_trials_for_mem_size(mem_size, trials, message_count,
                            workers, function_name, topic_arn):
    """ Run all trials for a given memory size """
    LAMBDA.update_function_configuration(FunctionName=function_name,
                                         MemorySize=mem_size)
    for _ in range(trials):
        for max_workers in workers:
            yield run_trial(message_count=message_count,
                            max_workers=max_workers,
                            function_name=function_name,
                            topic_arn=topic_arn)


def collect_results(memory, message_count, workers, trials,
                    function_name, topic_arn):
    """ Collects all results """
    results = []
    for mem_size in memory:
        for result in run_trials_for_mem_size(mem_size=mem_size,
                                              trials=trials,
                                              function_name=function_name,
                                              topic_arn=topic_arn,
                                              workers=workers,
                                              message_count=message_count):
            results.append(result)
    return results


def main():
    """ Main """
    parser = ArgumentParser()
    parser.add_argument('-m', '--memory',
                        nargs='+',
                        type=int,
                        required=True,
                        help='One or more memory sizes to test against')

    parser.add_argument('-n', '--trials',
                        type=int,
                        required=True,
                        help='The number of trials to run for each memory'
                             'size')

    parser.add_argument('-w', '--workers',
                        nargs='+',
                        type=int,
                        required=True,
                        help='One or more max_workers concurrency '
                             'configurations to try for each trial')

    parser.add_argument('-c', '--message-count',
                        required=True,
                        type=int,
                        help='The number of messages to be sent in any given '
                             'trial')

    parser.add_argument('-f', '--function-name',
                        required=True,
                        help='The name of the Lambda function')


    parser.add_argument('-t', '--topic-arn',
                        required=True,
                        help='The ARN of the topic to publish')

    args = parser.parse_args()
    res = collect_results(memory=args.memory,
                          message_count=args.message_count,
                          workers=args.workers,
                          trials=args.trials,
                          function_name=args.function_name,
                          topic_arn=args.topic_arn)

    print(json.dumps(res))


if __name__ == '__main__':
    main()

#!/usr/bin/env python

import boto
import boto.sqs
import boto.sqs.queue
import json
import os
import signal
import yaml

from datadog import statsd
from subprocess import call
from sys import stdout


def out(output):
    stdout.write(str(output) + "\n")
    # have to flush or systemd/journalctl will get messages out of order
    stdout.flush()


class Config:
    sqs_arn = 'arn:aws:sqs:us-east-1:0123456789012:mozilla-tiles-queue'
    print_config = False
    ddfs_master = 'disco://localhost'
    tag_prefix = 'incoming:'

    def __init__(self, config_file='processor.yaml'):
        if os.path.isfile(config_file):
            with open(config_file) as pointer:
                config = yaml.load(pointer.read())
            if config is None:
                return
            for key, value, in config.items():
                if getattr(self, key) != value:
                    setattr(self, key, value)
                if self.print_config:
                    out(self.__dict__)


sigint = signal.getsignal(signal.SIGINT)
stop = False


def sigint_later(signal, frame):
    global stop
    stop = (signal, frame)


def unstoppable():
    signal.signal(signal.SIGINT, sigint_later)


def stoppable():
    global stop
    if stop:
        raise KeyboardInterrupt
    else:
        signal.signal(signal.SIGINT, sigint)


s3 = boto.connect_s3()


def pull_from_s3(bucket, key, destination):
    global s3
    out('Downloading from s3://%s/%s to %s' % (bucket, key, destination))
    s3bucket = s3.get_bucket(bucket, validate=False)
    s3key = s3bucket.get_key(key)
    if s3key:
        s3key.get_contents_to_filename(destination)
    else:
        raise Exception('Error: key not found: s3://%s/%s' % (bucket, key))


def split_by_date(path):
    blobs = set()
    pointers = {}
    try:
        with open(path) as pointer:
            for line in pointer:
                try:
                    date = json.loads(line)['date']
                except:
                    # failed to get date from line, use default
                    date = '0000-00-00'
                blob = path + '.' + date
                # get the open file pointer for blob
                pointer2 = pointers.get(date)
                if pointer2 is None:
                    pointer2 = open(blob, 'w')
                    pointers[date] = pointer2
                # write this log line to blob
                pointer2.write(line)
                # keep track of the blobs
                blobs.add((date, blob))
    finally:
        for pointer in pointers.itervalues():
            pointer.close()
    return blobs


def push_to_ddfs(blobs, master, tag_prefix, label):
    for blob in blobs:
        command = [
            '/usr/bin/ddfs',
            '--master',
            master,
            'chunk',
            tag_prefix + label + ':' + blob[0],
            blob[1]
            ]
	out('Executing: ' + ' '.join(command))
	call(command)


def clean(path, blobs):
    os.remove(path)
    for _, blob in blobs:
        os.remove(blob)


def process(message, body, ddfs_master, tag_prefix):
    destination = '/tmp/' + body['path'].replace("/", ".")
    pull_from_s3(body['bucket'], body['path'], destination)
    blobs = split_by_date(destination)
    unstoppable()
    push_to_ddfs(blobs, ddfs_master, tag_prefix, body['label'])
    message.delete()
    stoppable()
    clean(destination, blobs)


def main(config=Config()):
    try:
        _, _, _, sqs_region, _, sqs_queue = config.sqs_arn.split(':', 5)
    except Exception:
        raise Exception('invalid sqs arn')

    sqs = boto.sqs.connect_to_region(sqs_region)
    queue = sqs.get_queue(sqs_queue)

    if queue is None:
        raise Exception('could not connect to sqs queue: ' + config.sqs_arn)

    while True:
        message = queue.read(wait_time_seconds=20)
        if message is None:
            #out('Queue is empty')
            continue
        try:
            raw_body = message.get_body()
            out('Message received')
        except Exception:
            msg = 'Failed to get message body'
            out(msg)
            statsd.event('SQS Message Error', msg, alert_type='error')
            statsd.increment('tiles.processor.failed_get_body')
            continue
        try:
            body = json.loads(json.loads(raw_body)['Message'].replace("u'",'"').replace("'",'"'))
        except Exception:
            msg = 'Invalid message body: ' + raw_body
            out(msg)
            statsd.event('JSON parse error', msg, alert_type='error')
            statsd.increment('tiles.processor.invalid_body')
            continue
        try:
            out('Processing: ' + str(body))
            process(message, body, config.ddfs_master, config.tag_prefix)
            statsd.increment('tiles.processor.processed')
        except Exception:
            msg = 'Failed to process: ' + str(body)
            out(msg)
            statsd.increment('tiles.processor.failed_to_process')
            statsd.event('Failed Processing message', msg, alert_type='error')
            stoppable()


if __name__ == '__main__':
    from sys import argv

    args = argv[1:]

    if '-h' in args or '--help' in args:
        out('Usage: %s [config.yaml]' % argv[0])
    elif args:
        main(Config(args[0]))
    else:
        main()

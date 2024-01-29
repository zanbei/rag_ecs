# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import io
import asyncio
import base64
import json
import time
import datetime
import requests
import boto3
import logging
import traceback
import uuid
import aioboto3
import difflib
import signal
from aiohttp_client_cache import CachedSession, CacheBackend
from botocore.exceptions import ClientError
from PIL import PngImagePlugin, Image
from requests.adapters import HTTPAdapter, Retry


# aws_default_region = os.getenv("AWS_DEFAULT_REGION")
# sqs_queue_url = os.getenv("SQS_QUEUE_URL")
# sns_topic_arn = os.getenv("SNS_TOPIC_ARN")
aws_default_region = 'us-west-2'
sqs_queue_url = 'https://sqs.us-west-2.amazonaws.com/310850127430/ecs_input'
sns_topic_arn = 'arn:aws:sns:us-west-2:310850127430:ecs_out'

current_model_name = ''
sqsRes = boto3.resource('sqs', region_name=aws_default_region)
snsRes = boto3.resource('sns', region_name=aws_default_region)
ab3_session = aioboto3.Session()

apiBaseUrl = "http://0.0.0.0:8000/v1/"
apiClient = requests.Session()
retries = Retry(
    total=3,
    connect=100,
    backoff_factor=0.1,
    allowed_methods=["POST"])
apiClient.mount('http://', HTTPAdapter(max_retries=retries))
REQUESTS_TIMEOUT_SECONDS = 30

cache = CacheBackend(
    cache_name='memory-cache',
    expire_after=600
)

shutdown = False


def main():
    print_env()
    queue = sqsRes.Queue(sqs_queue_url)
    SQS_WAIT_TIME_SECONDS = 20
    topic = snsRes.Topic(sns_topic_arn)

    # check_readiness()

    while True:
        received_messages = receive_messages(queue, 1, SQS_WAIT_TIME_SECONDS)
        for message in received_messages:
            try:
                snsPayload = json.loads(message.body)
                payload = json.loads(snsPayload['Message'])
                # taskHeader = payload['alwayson_scripts']
                # taskType = taskHeader['task'] if 'task' in taskHeader else None
                # taskId = taskHeader['id_task'] if 'id_task' in taskHeader else None
                # folder = get_prefix(
                #     payload['s3_output_path']) if 's3_output_path' in payload else None
                # print(
                #     f"Start process {taskType} task with ID: {taskId}")
                r = do_invocations(apiBaseUrl, payload)
                # Outputs = post_invocations(folder, r, 80)

            except Exception as e:
                print(e)
            finally:
                publish_message(topic, r)
                delete_message(message)


def print_env():
    print(f'AWS_DEFAULT_REGION={aws_default_region}')
    print(f'SQS_QUEUE_URL={sqs_queue_url}')
    print(f'SNS_TOPIC_ARN={sns_topic_arn}')


# def check_readiness():
#     while True:
#         try:
#             print('Checking service readiness...')
#             # checking with options "sd_model_checkpoint" also for caching current model
#             print('Service is ready.')
#             if "sd_model_checkpoint" in opts:
#                 global current_model_name
#                 current_model_name = opts['sd_model_checkpoint']
#                 print(f'Init model is: {current_model_name}.')
#             break
#         except Exception as e:
#             print(repr(e))
#             time.sleep(1)


def signalHandler(signum, frame):
    global shutdown
    shutdown = True


def get_time(f):

    def inner(*arg, **kwarg):
        s_time = time.time()
        res = f(*arg, **kwarg)
        e_time = time.time()
        print('Used: {:.4f} seconds on api: {}.'.format(
            e_time - s_time, arg[0]))
        return res
    return inner


def receive_messages(queue, max_number, wait_time):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        messages = queue.receive_messages(
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
    except ClientError as error:
        traceback.print_exc()
        raise error
    else:
        return messages


def publish_message(topic, message):
    try:
        response = topic.publish(Message=message)
        message_id = response['MessageId']
    except ClientError as error:
        traceback.print_exc()
        raise error
    else:
        return message_id


def delete_message(message):
    """
    Delete a message from a queue. Clients must delete messages after they
    are received and processed to remove them from the queue.

    :param message: The message to delete. The message's queue URL is contained in
                    the message's metadata.
    :return: None
    """
    try:
        message.delete()
    except ClientError as error:
        traceback.print_exc()
        raise error


def get_bucket_and_key(s3uri):
    pos = s3uri.find('/', 5)
    bucket = s3uri[5: pos]
    key = s3uri[pos + 1:]
    return bucket, key


def get_prefix(path):
    pos = path.find('/')
    return path[pos + 1:]


def str_simularity(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()


def encode_to_base64(buffer):
    return str(base64.b64encode(buffer))[2:-1]


def decode_to_image(encoding):
    image = None
    try:
        image = Image.open(io.BytesIO(base64.b64decode(encoding)))
    except Exception as e:
        raise e
    return image


def export_pil_to_bytes(image, quality):
    with io.BytesIO() as output_bytes:

        use_metadata = False
        metadata = PngImagePlugin.PngInfo()
        for key, value in image.info.items():
            if isinstance(key, str) and isinstance(value, str):
                metadata.add_text(key, value)
                use_metadata = True
        image.save(output_bytes, format="PNG", pnginfo=(
            metadata if use_metadata else None), quality=quality if quality else 80)

        bytes_data = output_bytes.getvalue()

    return bytes_data





# def switch_model(name, find_closest=True):
#     global current_model_name
#     # check current model
#     if find_closest:
#         name = name.lower()
#         current = current_model_name.lower()

#     if name in current:
#         return current_model_name

#     # refresh then check from model list
#     invoke_refresh_checkpoints()
#     models = invoke_get_model_names()
#     found_model = None

#     # exact matching
#     if name in models:
#         found_model = name
#     # find closest
#     elif find_closest:
#         max_sim = 0.0
#         max_model = None
#         for model in models:
#             sim = str_simularity(name, model.lower())
#             if sim > max_sim:
#                 max_sim = sim
#                 max_model = model
#         found_model = max_model

#     if not found_model:
#         raise RuntimeError(f'Model not found: {name}')
#     elif found_model != current_model_name:
#         options = {}
#         options["sd_model_checkpoint"] = found_model
#         invoke_set_options(options)
#         current_model_name = found_model

#     return current_model_name


@get_time
def do_invocations(url, body=None):
    if body is None:
        response = apiClient.get(
            url=url, timeout=(1, REQUESTS_TIMEOUT_SECONDS))
    else:
        response = apiClient.post(
            url=url, json=body, timeout=(1, REQUESTS_TIMEOUT_SECONDS))
    response.raise_for_status()
    return response.json()


def post_invocations(folder, response, quality):
    defaultFolder = datetime.date.today().strftime("%Y-%m-%d")
    if not folder:
        folder = defaultFolder
    images = []
    results = []
    if "images" in response.keys():
        images = [export_pil_to_bytes(decode_to_image(i), quality)
                  for i in response["images"]]
    elif "image" in response.keys():
        images = [export_pil_to_bytes(
            decode_to_image(response["image"]), quality)]

    if len(images) > 0:
        results = [upload_outputs(i, folder, None, 'png') for i in images]

    return results


def upload_outputs(object_bytes, folder, file_name=None, suffix=None):
    try:
        if suffix == 'out':
            content_type = f'application/json'
            if file_name is None:
                file_name = f"response-{uuid.uuid4()}"
        else:
            suffix = 'png'
            content_type = f'image/{suffix}'
            if file_name is None:
                file_name = datetime.datetime.now().strftime(
                    f"%Y%m%d%H%M%S-{uuid.uuid4()}")

        bucket = s3Res.Bucket(s3_bucket)
        bucket.put_object(
            Body=object_bytes, Key=f'{folder}/{file_name}.{suffix}', ContentType=content_type)

        return f's3://{s3_bucket}/{folder}/{file_name}.{suffix}'
    except Exception as error:
        traceback.print_exc()
        raise error


async def async_get(url):
    try:
        if url.startswith("http://") or url.startswith("https://"):
            async with CachedSession(cache=cache) as session:
                async with session.get(url) as res:
                    res.raise_for_status()
                    # todo: need a counter to delete expired responses
                    # await session.delete_expired_responses()
                    # print(res.from_cache, res.created_at, res.expires, res.is_expired)
                    return await res.read()
        elif url.startswith("s3://"):
            bucket, key = get_bucket_and_key(url)
            async with ab3_session.resource("s3") as s3:
                obj = await s3.Object(bucket, key)
                res = await obj.get()
                return await res['Body'].read()
    except Exception as e:
        raise e


async def async_upload(object_bytes, folder, file_name=None, suffix=None):
    try:
        async with ab3_session.resource("s3") as s3:
            if suffix == 'out':
                content_type = f'application/json'
                if file_name is None:
                    file_name = f"response-{uuid.uuid4()}"
            else:
                suffix = 'png'
                content_type = f'image/{suffix}'
                if file_name is None:
                    file_name = datetime.datetime.now().strftime(
                        f"%Y%m%d%H%M%S-{uuid.uuid4()}")

            bucket = await s3.Bucket(s3_bucket)
            await bucket.put_object(
                Body=object_bytes, Key=f'{folder}/{file_name}.{suffix}', ContentType=content_type)

            return f's3://{s3_bucket}/{folder}/{file_name}.{suffix}'
    except Exception as e:
        raise e


async def async_publish_message(content):
    try:
        async with ab3_session.resource("sns") as sns:
            topic = await sns.Topic(sns_topic_arn)
            response = await topic.publish(Message=content)
            return response['MessageId']
    except Exception as e:
        raise e


def exclude_keys(dictionary, keys):
    key_set = set(dictionary.keys()) - set(keys)
    return {key: dictionary[key] for key in key_set}


# Customizable for success responses
def succeed(images, response, header):
    n_iter = response['parameters']['n_iter']
    batch_size = response['parameters']['batch_size']
    parameters = response['parameters']
    parameters['id_task'] = header['id_task']
    parameters['status'] = 1
    parameters['image_url'] = ','.join(
        images[: n_iter * batch_size])
    parameters['image_seed'] = ','.join(
        str(x) for x in json.loads(response['info'])['all_seeds'])
    parameters['error_msg'] = ''
    parameters['image_mask_url'] = ','.join(images[n_iter * batch_size:])
    return {
        'images': [''],
        'parameters': parameters,
        'info': ''
    }


# Customizable for failure responses
def failed(header, exception):
    parameters = {}
    parameters['id_task'] = header['id_task']
    parameters['status'] = 0
    parameters['image_url'] = ''
    parameters['image_seed'] = []
    parameters['error_msg'] = repr(exception)
    parameters['reason'] = exception.response.json() if hasattr(exception, "response") else None
    parameters['image_mask_url'] = ''
    return {
        'images': [''],
        'parameters': parameters,
        'info': ''
    }


# Customizable for request payload
def prepare_payload(body, header):
    try:
        urls = []
        offset = 0
        # img2img image link
        if 'image_link' in header:
            urls.extend(header['image_link'].split(','))
            offset = len(urls)
        # ControlNet image link
        if 'controlnet' in header:
            for x in header['controlnet']['args']:
                if 'image_link' in x:
                    urls.append(x['image_link'])
        # Generate payload including ControlNet units
        if len(urls) > 0:
            loop = asyncio.get_event_loop()
            tasks = [loop.create_task(async_get(u)) for u in urls]
            results = loop.run_until_complete(asyncio.gather(*tasks))
            if offset > 0:
                init_images = [encode_to_base64(x) for x in results[:offset]]
                body.update({"init_images": init_images})

            if 'controlnet' in header:
                for x in header['controlnet']['args']:
                    if 'image_link' in x:
                        x['input_image'] = encode_to_base64(results[offset])
                        offset += 1

        # dbt compatible for override_settings
        override_settings = {}
        if 'sd_vae' in header:
            override_settings.update({'sd_vae': header['sd_vae']})
        if 'override_settings' in header:
            override_settings.update(header['override_settings'])
        if override_settings:
            if 'override_settings' in body:
                body['override_settings'].update(override_settings)
            else:
                body.update({'override_settings': override_settings})

        # dbt compatible for alwayson_scripts
        body.update({'alwayson_scripts': exclude_keys(
            header, ALWAYSON_SCRIPTS_EXCLUDE_KEYS)})
    except Exception as e:
        raise e

    return body


if __name__ == '__main__':
    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
        signal.signal(sig, signalHandler)
    main()

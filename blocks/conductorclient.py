import os
import json
import requests
from requests.exceptions import ConnectionError  # noqa: F401
from urllib.parse import urljoin

CONDUCTOR_BASE_URL = os.environ.get('CONDUCTOR_ENDPOINT', 'http://localhost:3205')


def get(endpoint):
    url = urljoin(CONDUCTOR_BASE_URL, endpoint)
    r = requests.get(url)

    if r.status_code != 200:
        raise Exception('Request failed ({})'.format(r.status_code))

    return r.json()


def post(endpoint, data):
    url = urljoin(CONDUCTOR_BASE_URL, endpoint)
    r = requests.post(
        url,
        headers={'Content-Type': 'application/json'},
        data=json.dumps(data)
    )

    if r.status_code != 200:
        raise Exception('Request failed ({})'.format(r.status_code))

    return r.json()


def ping(uuid):
    return post('/ping', data={'uuid': uuid})


def job_request(uuid, worker_type):
    return post('/job-request', data={'uuid': uuid, 'type': str(worker_type)})


def job_submit(job_uuid):
    return post('/job-submit', data={'job_uuid': job_uuid})

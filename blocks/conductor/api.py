import os
from flask import Flask, request
from blocks.db import BlockModel, TransactionModel
from blocks.config import DSN, LOGGER
from blocks.enums import WorkerType
from blocks.conductor.conductor import Conductor

log = LOGGER.getChild('db')
app = Flask(__name__)
conductor = None
block_model = None
tx_model = None


def response_ok(data=None):
    return {
        'success': True,
        'data': data,
    }


def response_error(message="General error"):
    return {
        'success': False,
        'error': True,
        'message': message,
    }


@app.route('/')
def index():
    return response_ok()


@app.route('/conductor-status')
def conductor_status():
    return response_ok() if conductor.status is True else response_error()


@app.route('/known-blocks')
def known_blocks():
    return response_ok(str(len(conductor.known_block_numbers)))


@app.route('/status')
def status():
    return response_ok({
        'blocks': block_model.count(),
        'transactions': tx_model.count(),
    })


@app.route('/ping', methods=('POST',))
def ping():
    req_obj = request.get_json()

    if (
        req_obj
        and isinstance(req_obj, dict)
        and req_obj.get('uuid') is not None
    ):
        conductor.ping(req_obj['uuid'])
        return response_ok()

    return response_error()


@app.route('/job-request', methods=('POST',))
def job_request():
    req_obj = request.get_json()

    if (
        req_obj
        and isinstance(req_obj, dict)
        and req_obj.get('uuid') is not None
        and req_obj.get('type') is not None
    ):
        job = conductor.generate_job(
            WorkerType.from_string(req_obj['type']),
            req_obj['uuid']
        )
        return response_ok(job.to_dict())

    return response_error()


@app.route('/job-submit', methods=('POST',))
def job_submit():
    req_obj = request.get_json()

    if (
        req_obj
        and isinstance(req_obj, dict)
        and req_obj.get('job_uuid') is not None
    ):
        verified, errors = conductor.verify_job(req_obj['job_uuid'])
        if verified is True:
            return response_ok()

        err_msg = ', '.join(errors)

        return response_error(err_msg)

    return response_error()


@app.route('/job-reject', methods=('POST',))
def job_reject():
    req_obj = request.get_json()

    if (
        req_obj
        and isinstance(req_obj, dict)
        and req_obj.get('job_uuid') is not None
    ):
        # Remove the job from memory, consumer doesn't want it
        conductor.del_job(req_obj['job_uuid'])

        if 'reason' in req_obj:
            # TODO: Do something meaningful with this?
            log.warn('Job {} rejected due to: {}'.format(req_obj['job_uuid'], req_obj['reason']))

        return response_ok()

    return response_error()


def api():
    """ Run the debug server """
    global conductor, block_model, tx_model

    host = os.environ.get('CONDUCTOR_HOST', '127.0.0.1')
    port = os.environ.get('CONDUCTOR_PORT', 3205)

    conductor = Conductor()
    block_model = BlockModel(DSN)
    tx_model = TransactionModel(DSN)

    app.run(host=host, port=port)

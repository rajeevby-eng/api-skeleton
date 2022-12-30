from flask import Blueprint, jsonify, request, make_response
from http import HTTPStatus
import json
from src.extensions import db
from src.models import AppointmentModel, ProviderModel
from datetime import datetime
from webargs import fields
from webargs.flaskparser import use_args, use_kwargs, parser, abort

home = Blueprint('/', __name__)


# Helpful documentation:
# https://webargs.readthedocs.io/en/latest/framework_support.html
# https://flask.palletsprojects.com/en/2.0.x/quickstart/#variable-rules

appointment_args = {
    # Required arguments
    "provider_name": fields.Str(required=True),
    "first_name": fields.Str(required=True),
    "last_name": fields.Str(required=True),
    "start_time": fields.DateTime(required=True),
    "end_time": fields.DateTime(required=True)
}

appointments_args = {
    # Required arguments
    "provider_name": fields.Str(required=True),
    "start_time": fields.DateTime(required=True),
    "end_time": fields.DateTime(required=True)
}


@home.route('/')
def index():
    return {'data': 'OK'}


@home.route('/appointment_model/<id_>', methods=['GET'])
def appointment_model(id_):
    record = AppointmentModel.query.filter_by(id=id_).first()
    if record is not None:
        return record.json()
    else:
        return jsonify(None), HTTPStatus.NOT_FOUND


@home.route('/appointment_model', methods=['POST'])
@use_kwargs(appointment_args)  # Injects keyword arguments
def appointment_model_create(provider_name, start_time, end_time, first_name, last_name):
    print("appointment model create: ", provider_name, start_time, end_time, first_name, last_name)
    provider = ProviderModel.provider(provider_name)
    if not provider:
        return jsonify(None), HTTPStatus.NOT_FOUND
    if not AppointmentModel.isAvailable(provider.id, start_time, end_time):
        return jsonify(None), HTTPStatus.FORBIDDEN
    new_record = AppointmentModel(
        provider_id=provider.id,
        start_time=start_time,
        end_time=end_time,
        first_name=first_name,
        last_name=last_name)
    db.session.add(new_record)
    db.session.commit()
    return jsonify(None), HTTPStatus.OK


@home.route('/appointment_model/appointments', methods=['GET'])
@parser.use_args({"provider_name": fields.Str(required=True), "start_time": fields.Str(required=True), "end_time": fields.Str(required=True)}, location="query")
def appointments(args):
    print("appointments: ", args)
    provider_name = args.get('provider_name')
    start_time = datetime.strptime(args.get('start_time'), '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(args.get('end_time'), '%Y-%m-%d %H:%M:%S')
    provider = ProviderModel.provider(provider_name)
    if not provider:
        return jsonify(None), HTTPStatus.NOT_FOUND

    appointments = AppointmentModel.appointments(provider.id, start_time, end_time).all()
    serialized_appointments = []
    for appointment in appointments:
        serialized_appointments.append(appointment.serialize())
    return make_response(jsonify(appointments=serialized_appointments), HTTPStatus.OK)

@home.route('/appointment_model/first_available', methods=['GET'])
@parser.use_args({"start_time": fields.Str(required=True), "duration": fields.Int(required=True)}, location="query")
def first_available(args):
    print("first_available: ", args)
    start_time = datetime.strptime(args.get('start_time'), '%Y-%m-%d %H:%M:%S')
    duration = args.get('duration')
    first_available = AppointmentModel.firstAvailable(start_time, duration)
    return make_response(jsonify(first_available), HTTPStatus.OK)

# This error handler is necessary for usage with Flask-RESTful
@parser.error_handler
def handle_request_parsing_error(err, req, schema, *, error_status_code, error_headers):
    """webargs error handler that uses Flask-RESTful's abort function to return
    a JSON error response to the client.
    """
    abort(error_status_code, errors=err.messages)

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

first_available_args = {
    # Required arguments
    "start_time": fields.DateTime(required=True),
    "duration": fields.Int(required=True)
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
    print("create: provider id = ", provider.id)
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
@use_kwargs(appointments_args, location="query")
def appointments(provider_name, start_time, end_time):
    print("appointments: ", provider_name, start_time, end_time)
    provider = ProviderModel.provider(provider_name)
    if not provider:
        return jsonify(None), HTTPStatus.NOT_FOUND

    appointments = AppointmentModel.appointments(provider.id, start_time, end_time).all()
    serialized_appointments = []
    for appointment in appointments:
        serialized_appointments.append(appointment.serialize())
    response = make_response(jsonify(appointments=serialized_appointments), HTTPStatus.OK)
    print("appointments response: ", response)
    return response

@home.route('/appointment_model/first_available', methods=['GET'])
@use_kwargs(first_available_args, location="query")
def first_available(start_time, duration):
    print("first_available: ", start_time, duration)
    first_available = AppointmentModel.firstAvailable(start_time, duration)
    print('first_available: ', first_available)
    return make_response(jsonify(first_available), HTTPStatus.OK)

@home.errorhandler(422)
@home.errorhandler(400)
def handle_error(err):
    headers = err.data.get("headers", None)
    messages = err.data.get("messages", ["Invalid request."])
    if messages:
        if headers:
            return jsonify({"errors": messages}), err.code, headers
        else:
            return jsonify({"errors": messages}), err.code
    else:
        return jsonify(None), err.code


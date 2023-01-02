from http import HTTPStatus
from pip._internal.cli.status_codes import SUCCESS


def test_home_api(client):
    response = client.get('/')
    assert response.status_code == HTTPStatus.OK
    # Response is binary string data because data is the raw data of the output.
    # The switch from ' to " is due to json serialization
    assert response.data == b'{"data":"OK"}\n'
    # json allows us to get back a deserialized data structure without us needing to manually do it
    assert response.json == {'data': 'OK'}
    
def appointment_model_create_api(client, json, http_status_code):
    response = client.post('/appointment_model', json=json)
    print("appointment response: ", response)
    print("appointment response json: ", response.json)
    assert response.status_code == http_status_code
    obj = response.json
    assert obj is None

# Run appointment model creation tests by themselves
def appointment_model_create_api_runner(client):
    success_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 15:00:00',
        'end_time': '2022-12-29 16:00:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, success_json, HTTPStatus.OK)
    appointment_model_create_api(client, success_json, HTTPStatus.FORBIDDEN)
    
    failure_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 16:30:00',
        'end_time': '2022-12-29 17:30:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, failure_json, HTTPStatus.FORBIDDEN)

def test_appointment_model_create_api(client):
    appointment_model_create_api_runner(client)
    
def appointment_model_create_api(client, json, http_status_code):
    response = client.post('/appointment_model', json=json)
    assert response.status_code == http_status_code
    assert response.json is None

# Run appointment model creation tests by themselves
def appointment_model_create_api_runner(client):
    success_2pm_strange_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 14:00:00',
        'end_time': '2022-12-29 15:00:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, success_2pm_strange_json, HTTPStatus.OK)
    success_2pm_who_json = {
        'provider_name': 'who',
        'start_time': '2022-12-29 14:00:00',
        'end_time': '2022-12-29 15:00:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, success_2pm_who_json, HTTPStatus.OK)
    success_3pm_strange_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 15:00:00',
        'end_time': '2022-12-29 16:00:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, success_3pm_strange_json, HTTPStatus.OK)
    appointment_model_create_api(client, success_3pm_strange_json, HTTPStatus.FORBIDDEN)
    
    failure_appointment_duration_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 14:00:00',
        'end_time': '2022-12-29 14:30:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, failure_appointment_duration_json, HTTPStatus.FORBIDDEN)
    
    failure_strange_after_hours_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 16:30:00',
        'end_time': '2022-12-29 17:30:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, failure_strange_after_hours_json, HTTPStatus.FORBIDDEN)
    
    failure_strange_before_hours_json = {
        'provider_name': 'strange',
        'start_time': '2022-12-29 8:30:00',
        'end_time': '2022-12-29 9:30:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, failure_strange_before_hours_json, HTTPStatus.FORBIDDEN)
    
    failure_who_saturday_json = {
        'provider_name': 'who',
        'start_time': '2022-12-31 10:30:00',
        'end_time': '2022-12-29 11:30:00',
        'first_name': 'first',
        'last_name': 'last'
    }
    appointment_model_create_api(client, failure_who_saturday_json, HTTPStatus.FORBIDDEN)
    
def appointments_get(client, query_params, expected_json, http_status_code):
    response = client.get('/appointment_model/appointments', query_string=query_params)
    assert response.status_code == http_status_code
    if expected_json:
        assert response.json == expected_json
    else:
        assert response.json is None

def appointments_initial_runner(client):
    missing_start_end_time_response_json = {
        "errors": {
            "query": {
                "end_time": [
                    "Missing data for required field."
                ],
                "start_time": [
                    "Missing data for required field."
                ]
            }
        }
    }
    appointments_get(client, {"provider_name": "strange"}, missing_start_end_time_response_json, HTTPStatus.UNPROCESSABLE_ENTITY)
    missing_end_time_response_json = {
        "errors": {
            "query": {
                    "end_time": [
                    "Missing data for required field."
                ]
            }
        }
    }
    appointments_get(client, {"provider_name": "strange", "start_time": "2022-12-31 10:30:00"}, missing_end_time_response_json, HTTPStatus.UNPROCESSABLE_ENTITY)
    missing_provider_name_response_json = {
        "errors": {
            "query": {
                "provider_name": [
                    "Missing data for required field."
                ]
            }
        }
    }
    appointments_get(client, {"start_time": "2022-12-31 10:30:00", "end_time": "2022-12-29 11:30:00"}, missing_provider_name_response_json, HTTPStatus.UNPROCESSABLE_ENTITY)
    appointments_get(client, {"provider_name": "strange", "start_time": "2022-12-29 1:00:00", "end_time": "2022-12-30 11:30:00"}, {'appointments': []}, HTTPStatus.OK)
    appointments_get(client, {"provider_name": "who", "start_time": "2022-12-29 1:00:00", "end_time": "2023-1-2 11:30:00"}, {'appointments': []}, HTTPStatus.OK)

def appointments_post_create_runner(client):
    strange_appointments_json = {
        "appointments": [
            {
                'provider_name': 'strange',
                'start_time': '2022-12-29T14:00:00',
                'end_time': '2022-12-29T15:00:00',
                'first_name': 'first',
                'last_name': 'last'
            },
            {
                'provider_name': 'strange',
                'start_time': '2022-12-29T15:00:00',
                'end_time': '2022-12-29T16:00:00',
                'first_name': 'first',
                'last_name': 'last'
            }
        ]
    }
    appointments_get(client, {"provider_name": "strange", "start_time": "2021-12-29 7:00:00", "end_time": "2022-12-29 19:00:00"}, strange_appointments_json, HTTPStatus.OK)

    strange_limited_appointments_json = {
        "appointments": [
            {
                'provider_name': 'strange',
                'start_time': '2022-12-29T14:00:00',
                'end_time': '2022-12-29T15:00:00',
                'first_name': 'first',
                'last_name': 'last'
            },
        ]
    }
    appointments_get(client, {"provider_name": "strange", "start_time": "2021-12-29 7:00:00", "end_time": "2022-12-29 15:00:00"}, strange_limited_appointments_json, HTTPStatus.OK)

    who_appointments_json = {
        "appointments": [
            {
                'provider_name': 'who',
                'start_time': '2022-12-29T14:00:00',
                'end_time': '2022-12-29T15:00:00',
                'first_name': 'first',
                'last_name': 'last'
            }
        ]
    }
    appointments_get(client, {"provider_name": "who", "start_time": "2021-12-29 7:00:00", "end_time": "2022-12-29 19:00:00"}, who_appointments_json, HTTPStatus.OK)

def first_available(client, query_params, expected_json, http_status_code):
    response = client.get('/appointment_model/first_available', query_string=query_params)
    assert response.status_code == http_status_code
    if expected_json:
        assert response.json == expected_json
    else:
        assert response.json is None
        
def first_available_runner(client):
    no_appointments_first_available = {
        "start_time": "2022-12-30T12:00:00",
        "provider_name": "strange"
    }
    first_available(client, 
                    {"start_time": "2022-12-30 12:00:00", "duration": 20},
                    no_appointments_first_available,
                    HTTPStatus.OK)

    before_hours_first_available = {
        "start_time": "2022-12-29T08:00:00",
        "provider_name": "who"
    }
    first_available(client, 
                    {"start_time": "2022-12-29 7:00:00", "duration": 20},
                    before_hours_first_available,
                    HTTPStatus.OK)

    after_hours_first_available = {
        "start_time": "2022-12-30T09:00:00",
        "provider_name": "strange"
    }
    first_available(client, 
                    {"start_time": "2022-12-29 17:00:00", "duration": 20},
                    after_hours_first_available,
                    HTTPStatus.OK)

    '''
    saturday_first_available = {
        "start_time": "2023-1-2T08:00:00",
        "provider_name": "who"
    }
    first_available(client, 
                    {"start_time": "2022-12-31T20:00:00", "duration": 20},
                    saturday_first_available,
                    HTTPStatus.OK)

    busy_first_available = {
        "start_time": "2022-12-29T15:00:00",
        "provider_name": "who"
    }
    first_available(client, 
                    {"start_time": "2022-12-29T14:00:00", "duration": 20},
                    busy_first_available,
                    HTTPStatus.OK)
    '''

    partially_busy_first_available = {
        "start_time": "2022-12-29T15:00:00",
        "provider_name": "who"
    }
    first_available(client, 
                    {"start_time": "2022-12-29T15:00:00", "duration": 20},
                    partially_busy_first_available,
                    HTTPStatus.OK)

    
def test_all_apis(client):
    appointments_initial_runner(client)
    appointment_model_create_api_runner(client)
    appointments_post_create_runner(client)
    first_available_runner(client)
    


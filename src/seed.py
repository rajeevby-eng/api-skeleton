from src.extensions import db
from src.models import AppointmentModel, ProviderModel
from datetime import time

"""
NINE_AM = time(9, 0, 0).strftime("%H:%M:%S").strip()
TEN_AM = time(10, 0, 0).strftime("%H:%M:%S").strip()
FIVE_PM = time(17, 0, 0).strftime("%H:%M:%S").strip()
EIGHT_PM = time(20, 0, 0).strftime("%H:%M:%S").strip()
SATURDAY_DAY_OF_WEEK = 6
"""
NINE_AM = time(9, 0, 0)
TEN_AM = time(10, 0, 0)
FIVE_PM = time(17, 0, 0)
EIGHT_PM = time(20, 0, 0)
EIGHT_AM = time(8, 0, 0)
FOUR_PM = time(16, 0, 0)
SATURDAY_DAY_OF_WEEK = 6

def seed_data():
    db.session.execute("INSERT INTO provider_model(last_name) VALUES('strange')")
    strange_provider = ProviderModel.provider('strange')
    for day_of_week in range(1, 6):
        db.session.execute("INSERT INTO availability_model(provider_id, day_of_week, start_time, end_time) VALUES({}, {}, '{}', '{}')".format(
                strange_provider.id, day_of_week, NINE_AM, FIVE_PM))
    db.session.execute("INSERT INTO availability_model(provider_id, day_of_week, start_time, end_time) VALUES({}, {}, '{}', '{}')".format(
        strange_provider.id, SATURDAY_DAY_OF_WEEK, TEN_AM, EIGHT_PM))
    
    db.session.execute("INSERT INTO provider_model(last_name) VALUES('who')")
    who_provider = ProviderModel.provider('who')
    for day_of_week in range(1, 6):
        db.session.execute("INSERT INTO availability_model(provider_id, day_of_week, start_time, end_time) VALUES({}, {}, '{}', '{}')".format(
            who_provider.id, day_of_week, EIGHT_AM, FOUR_PM))

    db.session.commit()


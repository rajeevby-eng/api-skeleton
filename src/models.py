from __future__ import annotations
from src.extensions import db
from flask import jsonify
from sqlalchemy import and_, or_, not_, cast, Date, Time, orm, union, func, literal, union_all, String
from sqlalchemy.orm import aliased
from sqlalchemy.sql.functions import coalesce
from datetime import datetime, timedelta
import json
from typing import List


def python_to_sql_weekday(weekday: int) -> int:
    return (weekday + 1) % 7
    
class ProviderModel(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    first_name = db.Column(db.String)
    last_name = db.Column(db.String, nullable=False)
        
    appointments = db.relationship("AppointmentModel", backref="provider_model", lazy=True, viewonly=True)
    availabilities = db.relationship("AvailabilityModel", backref="provider_model", lazy=True, viewonly=True)

    @staticmethod
    def provider(provider_name: str) -> ProviderModel:
        providers = ProviderModel.query.filter_by(last_name=provider_name)
        count = db.session.query(db.func.count('*')).select_from(providers.subquery()).scalar()
        if not providers or count > 1:
            return None
        return providers.first()
    
    def serialize(self) -> Dict:
        return jsonify({'first_name': self.first_name, 'last_name': self.last_name})
 

    def json(self) -> str:
        """
        :return: Serializes this object to JSON
        """
        return jsonify(self.serialize())
    
    
class AppointmentModel(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    provider_id = db.Column(db.Integer, db.ForeignKey('provider_model.id'), nullable=False, index=True)
    start_time = db.Column(db.DateTime)
    end_time = db.Column(db.DateTime)
    first_name = db.Column(db.String, nullable=False)
    last_name = db.Column(db.String, nullable=False)
    provider = db.relationship("ProviderModel", backref="appointment_model", lazy=True)
    
    @staticmethod
    def isValidSize(duration_in_seconds) -> bool:
        return duration_in_seconds in {1200, 2700, 3600}
    
    @staticmethod
    def availabilityQuery(provider_id, start_time, end_time) -> orm.Query:
        query = db.session.query(AppointmentModel) \
            .filter(or_(
                and_(AppointmentModel.start_time <= start_time,
                     start_time < AppointmentModel.end_time),
                and_(AppointmentModel.start_time < end_time,
                     end_time <= AppointmentModel.end_time)))
        if provider_id is not None:
            query = query.filter(AppointmentModel.provider_id == provider_id)
        return query
    
    @staticmethod
    def isAvailable(provider_id, start_time, end_time) -> bool:
        # 1. start and end times are on the same day
        if start_time.date() != end_time.date() or start_time >= end_time:
            return False
        
        # 2. Size of the appointment is as allowed (20, 45 or 60 minutes)
        if not AppointmentModel.isValidSize((end_time - start_time).total_seconds()):
            return False
        
        # 3. start time is permitted and is available
        # 4. end time is permitted and is available
        """
        SELECT EXISTS(
            SELECT id 
            FROM AvailabilityModel a
            WHERE EXTRACT(DOW FROM TIMESTAMP start_time_value) == a.day_of_week AND
                  a.start_time <= start_time_value AND
                  end_time_value <= a.end_time AND
                  a.provider_id = provider_id)
            AND NOT EXISTS(
                SELECT id
                FROM AppointmentModel b
                WHERE ((b.start_time <= start_time_value AND
                       start_time_value < b.end_time) OR
                      (b.start_time <= end_time_value AND
                       end_time_value < b.end_time)) AND
                      b.provider_id = provider_id
        FROM Dual
        """
        availability_query = AvailabilityModel.availabilityQuery(provider_id, start_time, end_time)
        appointment_query = AppointmentModel.availabilityQuery(provider_id, start_time, end_time)
        return db.session.query(
            and_(availability_query.exists(), not_(appointment_query.exists()))
            ).scalar()

    @staticmethod
    def appointments(provider_id, start_time, end_time) -> orm.Query:
        query = db.session.query(AppointmentModel) \
            .filter(start_time <= AppointmentModel.end_time) \
            .filter(AppointmentModel.start_time < end_time) \
            .filter(AppointmentModel.provider_id == provider_id)
        return query
    
    @staticmethod
    def firstAvailableQuery(start_time, duration_in_minutes) -> orm.Query:
        end_time = start_time + timedelta(minutes=duration_in_minutes)
        
        earliest_start_query = db.session.query(AvailabilityModel.provider_id.label('provider_id'),
                                                   func.min(func.datetime(func.datetime(func.date(start_time, '+' + cast((AvailabilityModel.day_of_week - start_time.weekday() + 7) % 7, String) + ' days'), AvailabilityModel.start_time))).label('start_time')) \
                                            .filter(start_time <= func.datetime(func.datetime(func.date(start_time, '+' + cast((AvailabilityModel.day_of_week - start_time.weekday() + 7) % 7, String) + ' days'), AvailabilityModel.start_time))) \
                                            .group_by(AvailabilityModel.provider_id) \
                                            .subquery()
        availability_model_start = aliased(AvailabilityModel)
        availability_model_next = aliased(AvailabilityModel)
        availability_model_next_next = aliased(AvailabilityModel)
        
        dummy_start_query = db.session.query(AvailabilityModel.provider_id.label('provider_id'),
                                       func.datetime(func.max(start_time, func.min(coalesce(AppointmentModel.start_time,
                                                                       func.datetime(func.date(start_time), availability_model_start.start_time),
                                                                       func.datetime(func.date(start_time, '+1 day'), availability_model_next.start_time),
                                                                       func.datetime(func.date(start_time, '+2 day'), availability_model_next_next.start_time)))),
                                                                        "+" + str(duration_in_minutes) + " minutes").label('start_time'),
                                       func.datetime(func.max(start_time, func.min(coalesce(AppointmentModel.start_time,
                                                                       func.datetime(func.date(start_time), availability_model_start.start_time),
                                                                       func.datetime(func.date(start_time, '+1 day'), availability_model_next.start_time),
                                                                       func.datetime(func.date(start_time, '+2 day'), availability_model_next_next.start_time)))),
                                                                        "+" + str(duration_in_minutes) + " minutes").label('end_time')) \
                                      .outerjoin(availability_model_start,
                                              and_(availability_model_start.provider_id == AvailabilityModel.provider_id,
                                                   availability_model_start.day_of_week == func.strftime('%w', start_time))) \
                                      .outerjoin(availability_model_next,
                                              and_(availability_model_next.provider_id == AvailabilityModel.provider_id,
                                                   availability_model_next.day_of_week == (func.strftime('%w', start_time) + 1)%7)) \
                                      .outerjoin(availability_model_next_next,
                                              and_(availability_model_next_next.provider_id == AvailabilityModel.provider_id,
                                                   availability_model_next_next.day_of_week == (func.strftime('%w', start_time) + 2)%7)) \
                                      .outerjoin(AppointmentModel,
                                                 and_(AppointmentModel.provider_id == AvailabilityModel.provider_id,
                                                      AvailabilityModel.day_of_week == (func.strftime('%w', AppointmentModel.start_time)))) \
                                      .group_by(AvailabilityModel.provider_id)
        dummy_end_query = db.session.query(AvailabilityModel.provider_id.label('provider_id'),
                                       func.datetime(func.max(start_time, func.max(coalesce(AppointmentModel.start_time,
                                                                       func.datetime(func.date(start_time), availability_model_start.start_time),
                                                                       func.datetime(func.date(start_time, '+1 day'), availability_model_next.start_time),
                                                                       func.datetime(func.date(start_time, '+2 day'), availability_model_next_next.start_time)))),
                                                                        "+" + str(duration_in_minutes) + " minutes").label('start_time'),
                                       func.datetime(func.max(start_time, func.max(coalesce(AppointmentModel.start_time,
                                                                       func.datetime(func.date(start_time), availability_model_start.start_time),
                                                                       func.datetime(func.date(start_time, '+1 day'), availability_model_next.start_time),
                                                                       func.datetime(func.date(start_time, '+2 day'), availability_model_next_next.start_time)))),
                                                                        "+" + str(duration_in_minutes) + " minutes").label('end_time')) \
                                      .outerjoin(availability_model_start,
                                              and_(availability_model_start.provider_id == AvailabilityModel.provider_id,
                                                   availability_model_start.day_of_week == func.strftime('%w', start_time))) \
                                      .outerjoin(availability_model_next,
                                              and_(availability_model_next.provider_id == AvailabilityModel.provider_id,
                                                   availability_model_next.day_of_week == (func.strftime('%w', start_time) + 1)%7)) \
                                      .outerjoin(availability_model_next_next,
                                              and_(availability_model_next_next.provider_id == AvailabilityModel.provider_id,
                                                   availability_model_next_next.day_of_week == (func.strftime('%w', start_time) + 2)%7)) \
                                      .outerjoin(AppointmentModel,
                                                 and_(AppointmentModel.provider_id == AvailabilityModel.provider_id,
                                                      AvailabilityModel.day_of_week == (func.strftime('%w', AppointmentModel.start_time)))) \
                                      .group_by(AvailabilityModel.provider_id)
                                              
        appointment_model_query = db.session.query(AppointmentModel.provider_id.label('provider_id'),
                                                      AppointmentModel.start_time.label('start_time'),
                                                      AppointmentModel.end_time.label('end_time'))
        appointments_subquery = appointment_model_query.union(dummy_start_query, dummy_end_query).subquery()
        
                
        availability_model = aliased(AvailabilityModel)
        
        previous_end_subquery = db.session.query(appointments_subquery.c.provider_id,
                                                 func.lag(appointments_subquery.c.end_time, 1, func.max(start_time, func.datetime(func.date(appointments_subquery.c.end_time), availability_model.start_time))).over(
                                                     order_by=appointments_subquery.c.start_time,
                                                     partition_by=appointments_subquery.c.provider_id).label('previous_end_time'),
                                                 func.lag(func.date(func.datetime(appointments_subquery.c.end_time, '+1 day')), 1, func.date(start_time, '+1 day')).over(
                                                     order_by=appointments_subquery.c.start_time,
                                                     partition_by=appointments_subquery.c.provider_id).label('next_day'),
                                                 func.lag(func.date(func.datetime(appointments_subquery.c.end_time, '+2 days')), 1, func.date(start_time, '+2 day')).over(
                                                     order_by=appointments_subquery.c.start_time,
                                                     partition_by=appointments_subquery.c.provider_id).label('next_next_day'),
                                                 appointments_subquery.c.start_time,
                                                 appointments_subquery.c.end_time,
                                                 availability_model.day_of_week.label('day_of_week')) \
                                            .join(availability_model, 
                                                  and_(availability_model.provider_id == appointments_subquery.c.provider_id,
                                                       availability_model.day_of_week == (func.strftime('%w', appointments_subquery.c.end_time)))) \
                                            .filter(start_time <= appointments_subquery.c.end_time) \
                                            .subquery()
        sameday_query = db.session.query(previous_end_subquery.c.provider_id.label('provider_id'),
                                         previous_end_subquery.c.previous_end_time.label('start_time')) \
                                    .join(AvailabilityModel, 
                                          and_(AvailabilityModel.provider_id == previous_end_subquery.c.provider_id,
                                               AvailabilityModel.day_of_week == previous_end_subquery.c.day_of_week)) \
                                    .filter(func.datetime(previous_end_subquery.c.previous_end_time, "+" + str(duration_in_minutes) + " minutes") <= previous_end_subquery.c.start_time) \
                                    .filter(AvailabilityModel.start_time <= func.time(previous_end_subquery.c.previous_end_time)) \
                                    .filter(func.datetime(previous_end_subquery.c.previous_end_time, "+" + str(duration_in_minutes) + " minutes") <= (func.datetime(func.date(previous_end_subquery.c.previous_end_time), AvailabilityModel.end_time))) \
                                    .group_by(previous_end_subquery.c.provider_id) \
        
        next_query = db.session.query(previous_end_subquery.c.provider_id.label('provider_id'),
                                      func.datetime(previous_end_subquery.c.next_day, availability_model.start_time).label('start_time')) \
                                .join(AvailabilityModel, 
                                      and_(AvailabilityModel.provider_id == previous_end_subquery.c.provider_id,
                                           AvailabilityModel.day_of_week == (previous_end_subquery.c.day_of_week + 1) % 7)) \
                                .group_by(previous_end_subquery.c.provider_id)

        next_next_query = db.session.query(previous_end_subquery.c.provider_id.label('provider_id'),
                                           func.datetime(previous_end_subquery.c.next_next_day, availability_model.start_time).label('start_time')) \
                                    .join(AvailabilityModel, 
                                          and_(AvailabilityModel.provider_id == previous_end_subquery.c.provider_id,
                                               AvailabilityModel.day_of_week == (previous_end_subquery.c.day_of_week + 2) % 7)) \
                                    .filter(func.datetime(func.datetime(previous_end_subquery.c.next_day, availability_model.start_time), "+" + str(duration_in_minutes) + " minutes") <= previous_end_subquery.c.start_time) \
                                    .filter(func.time(AvailabilityModel.start_time, "+" + str(duration_in_minutes) + " minutes") <= AvailabilityModel.end_time) \
                                    .group_by(previous_end_subquery.c.provider_id)

        enum_query = sameday_query.union(next_query, next_next_query) \
                                    .order_by('start_time') \
                                    .limit(1)

        return enum_query

    
    @staticmethod
    def firstAvailable(start_time, duration_in_minutes) -> Dict:
        if not AppointmentModel.isValidSize(duration_in_minutes*60):
            return None
        
        first_available = AppointmentModel.firstAvailableQuery(start_time, duration_in_minutes).first()
        provider = db.session.query(ProviderModel).filter(ProviderModel.id == first_available.provider_id).first()
        formatted_datetime = datetime.strptime(first_available.start_time[:-7] if '.' in first_available.start_time else first_available.start_time, '%Y-%m-%d %H:%M:%S').isoformat()
        return dict(provider_name=provider.last_name, start_time=formatted_datetime)
        
    
    def serialize(self) -> Dict:
        return {'provider_name': self.provider.last_name,
                'start_time': self.start_time.isoformat(), 'end_time': self.end_time.isoformat(),
                'first_name': self.first_name, 'last_name': self.last_name}
    def json(self) -> str:
        """
        :return: Serializes this object to JSON
        """
        return jsonify(self.serialize())
        
        
class AvailabilityModel(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    provider_id = db.Column(db.Integer, db.ForeignKey('provider_model.id'), nullable=False, index=True)
    day_of_week = db.Column(db.Integer, nullable=False)
    start_time = db.Column(db.Time)
    end_time = db.Column(db.Time)
    provider = db.relationship("ProviderModel", backref="availability_model", lazy=True)
    
    @staticmethod
    def availabilityQuery(provider_id, start_time, end_time) -> orm.Query:
        query = db.session.query(AvailabilityModel) \
            .filter(python_to_sql_weekday(start_time.weekday()) == AvailabilityModel.day_of_week) \
            .filter(AvailabilityModel.start_time <= start_time.time()) \
            .filter(end_time.time() <= AvailabilityModel.end_time) \
            .filter(AvailabilityModel.provider_id == provider_id)
        return query

    def serialize(self) -> Dict:
        return {'provider_name': self.provider.last_name,
                'day_of_week': self.day_of_week,
                'start_time': self.start_time.isoformat(), 'end_time': self.end_time.isoformat()}
        
    def json(self) -> str:
        """
        :return: Serializes this object to JSON
        """
        return jsonify(self.serialize())

        
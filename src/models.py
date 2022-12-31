from __future__ import annotations
from src.extensions import db
from flask import jsonify
from sqlalchemy import and_, or_, not_, cast, Date, Time, orm, union, func, literal
from sqlalchemy.orm import aliased
from sqlalchemy.sql.functions import coalesce
from datetime import datetime, timedelta
import json
from typing import List

    
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
                  end_time_value < a.end_time AND
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
            .filter(start_time < AppointmentModel.end_time) \
            .filter(AppointmentModel.start_time < end_time) \
            .filter(AppointmentModel.provider_id == provider_id)
        return query
    
    @staticmethod
    def firstAvailableQuery(start_time, duration_in_minutes) -> orm.Query:
        end_time = start_time + timedelta(minutes=duration_in_minutes)
        
        appointment_model = aliased(AppointmentModel)
        availability_model = aliased(AvailabilityModel)
        
        enum_subquery = db.session.query(appointment_model.provider_id,
                                         func.max(start_time, func.lag(appointment_model.end_time).over(order_by=appointment_model.start_time)).label('previous_end_time'),
                                         appointment_model.start_time) \
                        .join(availability_model, 
                              and_(availability_model.provider_id == appointment_model.provider_id,
                                   availability_model.day_of_week == func.strftime('%w', appointment_model.end_time))) \
                        .subquery()
        
        enum_query = db.session.query(enum_subquery.c.provider_id.label('provider_id'), enum_subquery.c.previous_end_time.label('start_time')) \
                    .filter((enum_subquery.c.start_time >= (enum_subquery.c.previous_end_time + 60*duration_in_minutes))) \
                    .order_by(enum_subquery.c.start_time) \
                    .limit(1)

        return enum_query

    
    @staticmethod
    def firstAvailable(start_time, duration_in_minutes) -> Dict:
        if not AppointmentModel.isValidSize(duration_in_minutes*60):
            return None
        
        first_available = AppointmentModel.firstAvailableQuery(start_time, duration_in_minutes).first()
        if first_available:
            return dict(first_available)
        else:
            provider = db.session.query(ProviderModel).first()
            return dict(provider_id=provider.id, start_time=start_time)
        
    
    def serialize(self) -> Dict:
        return {'provider': self.provider.last_name,
                'start_time': self.start_time, 'end_time': self.end_time,
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
            .filter(start_time.weekday() == AvailabilityModel.day_of_week) \
            .filter(AvailabilityModel.start_time <= start_time.time()) \
            .filter(end_time.time() < AvailabilityModel.end_time)
        if provider_id is not None:
            query.filter(AvailabilityModel.provider_id == provider_id)
        return query

    def serialize(self) -> Dict:
        return {'provider': self.provider.last_name,
                'day_of_week': self.day_of_week,
                'start_time': self.start_time, 'end_time': self.end_time}
        
    def json(self) -> str:
        """
        :return: Serializes this object to JSON
        """
        return jsonify(self.serialize())

        
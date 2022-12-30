from __future__ import annotations
from src.extensions import db
from flask import jsonify
from sqlalchemy import and_, or_, not_, cast, Date, Time, orm, union, func, literal
from sqlalchemy.orm import aliased
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
        """
        This can be done using a LAG() OVER as well
        
        SELECT provider_id, start_time
        FROM (
            SELECT b.provider_id, a.end_time as start_time
            FROM AppointmentModel a
            JOIN AvailabilityModel b
            ON ((EXTRACT(DOW FROM TIMESTAMP a.end_time) == b.day_of_week) AND
                (a.provider_id = b.provider_id))
            WHERE NOT EXISTS (
                    SELECT c.start_time
                    FROM AppointmentModel c
                    WHERE b.start_time <= c.start_time AND
                          c.end_time <= b.end_time AND
                          c.start_time < (a.end_time + duration)) AND
                          (a.provider_id = c.provider_id) AND
                ((a.end_time + duration) <= b.end_time) AND
                (start_time <= t.start_time)
            
            UNION
            
            SELECT b.provider_id, (a.start_time - duration) as start_time
            FROM AppointmentModel a
            JOIN AvailabilityModel b
            ON ((EXTRACT(DOW FROM TIMESTAMP a.end_time) == b.day_of_week) AND
                (a.provider_id = b.provider_id))
            WHERE NOT EXISTS (
                    SELECT c.start_time
                    FROM AppointmentModel c
                    WHERE b.start_time <= c.start_time AND
                                              c.end_time <= b.end_time AND
                          c.start_time <= (a.start_time - duration) AND
                          a.provider_id = c.provider_id) AND
                (b.start_time <= (a.start_time - duration)) AND
                (start_time <= t.start_time)
                
            UNION
            
            SELECT b.provider_id, (CAST(a.start_time AS DATE) + 86400 + (a.start_time.time() - b.start_time)) as start_time
            FROM AppointmentModel a
            JOIN AvailabilityModel b
            ON (b.day_of_week == (EXTRACT(DOW FROM TIMESTAMP a.end_time) + 1) % 7) AND
                (a.provider_id = b.provider_id))
            WHERE NOT EXISTS (
                    SELECT c.start_time
                    FROM AppointmentModel c
                    WHERE b.start_time <= c.start_time AND
                          c.end_time <= b.end_time AND
                          a.provider_id = c.provider_id) AND
                (start_time <= t.start_time)

            SELECT b.provider_id, (CAST(a.start_time AS DATE) + 2*86400 + (a.start_time.time() - b.start_time)) as start_time
            FROM AppointmentModel a
            JOIN AvailabilityModel b
            ON (b.day_of_week == (EXTRACT(DOW FROM TIMESTAMP a.end_time) + 2) % 7) AND
                (a.provider_id = b.provider_id))
            WHERE NOT EXISTS (
                    SELECT c.start_time
                    FROM AppointmentModel c
                    WHERE b.start_time <= c.start_time AND
                          c.end_time <= b.end_time AND
                          a.provider_id = c.provider_id) AND
                (start_time <= t.start_time)
        ) as t
        ORDER BY t.start_time
        LIMIT 1
        
        """
        
        end_time = start_time + timedelta(minutes=duration_in_minutes)
        
        appointment_model = aliased(AppointmentModel)
        inner_appointment_model = aliased(AppointmentModel)
        availability_model = aliased(AvailabilityModel)
        after_subquery = db.session.query(inner_appointment_model) \
                            .filter(appointment_model.provider_id == inner_appointment_model.provider_id) \
                            .filter(availability_model.start_time <= inner_appointment_model.start_time) \
                            .filter(inner_appointment_model.start_time < (appointment_model.end_time + 60*duration_in_minutes))
        after_query = db.session.query(appointment_model.end_time.label('start_time'), availability_model.provider_id) \
            .join(availability_model,
                  and_(func.strftime('%w', appointment_model.end_time) == availability_model.day_of_week,
                       appointment_model.provider_id == availability_model.provider_id)) \
            .filter(not_(after_subquery.exists())) \
            .filter((cast((appointment_model.end_time), Time) + 60*duration_in_minutes) <= availability_model.end_time) \
            .filter(start_time <= appointment_model.end_time)

        before_subquery = db.session.query(inner_appointment_model) \
                                    .filter(appointment_model.provider_id == inner_appointment_model.provider_id) \
                                    .filter(availability_model.start_time <= inner_appointment_model.start_time) \
                                    .filter(inner_appointment_model.start_time < (appointment_model.start_time - 60*duration_in_minutes))
        before_query = db.session.query((appointment_model.start_time - 60*duration_in_minutes).label('start_time'), availability_model.provider_id) \
            .join(availability_model,
                  and_(func.strftime('%w', appointment_model.end_time) == availability_model.day_of_week,
                       appointment_model.provider_id == availability_model.provider_id)) \
            .filter(not_(before_subquery.exists())) \
            .filter(availability_model.start_time <= (appointment_model.start_time - 60*duration_in_minutes)) \
            .filter(start_time <= (appointment_model.start_time - 60*duration_in_minutes))

        # This needs some more debugging, but the intent is to return the start_time if there are no appointments
        # that day.
        sameday_subquery = db.session.query(inner_appointment_model) \
                            .filter(availability_model.provider_id == inner_appointment_model.provider_id) \
                            .filter(not_(or_(inner_appointment_model.end_time <= start_time,
                                             end_time <= inner_appointment_model.start_time)))
        sameday_query = db.session.query(literal(start_time).label('start_time'), availability_model.provider_id) \
            .filter(not_(sameday_subquery.exists()))
            
        next_subquery = db.session.query(inner_appointment_model) \
                            .filter(appointment_model.provider_id == inner_appointment_model.provider_id) \
                            .filter(availability_model.start_time <= inner_appointment_model.start_time) \
                            .filter(inner_appointment_model.end_time <= appointment_model.end_time)
        next_query = db.session.query(((cast((appointment_model.start_time), Date) + 86400 + (cast((appointment_model.start_time), Time) - availability_model.start_time))).label('start_time'), availability_model.provider_id) \
            .join(availability_model,
                  and_((availability_model.day_of_week == func.strftime('%w', appointment_model.end_time) + 1) % 7,
                       appointment_model.provider_id == availability_model.provider_id)) \
            .filter(not_(next_subquery.exists())) \
            .filter(((cast((appointment_model.start_time), Date) + 86400 + (cast((appointment_model.start_time), Time) - availability_model.start_time)) <= appointment_model.end_time))

        next_next_subquery = db.session.query(inner_appointment_model) \
                            .filter(appointment_model.provider_id == inner_appointment_model.provider_id) \
                            .filter(availability_model.start_time <= inner_appointment_model.start_time) \
                            .filter(inner_appointment_model.end_time <= appointment_model.end_time)
        next_next_query = db.session.query(((cast((appointment_model.start_time), Date) + 2*86400 + (cast((appointment_model.start_time), Time) - availability_model.start_time))).label('start_time'), availability_model.provider_id) \
            .join(availability_model,
                  and_((availability_model.day_of_week == func.strftime('%w', appointment_model.end_time) + 2) % 7,
                       appointment_model.provider_id == availability_model.provider_id)) \
            .filter(not_(next_next_subquery.exists())) \
            .filter(((cast((appointment_model.start_time), Date) + 86400 + (cast((appointment_model.start_time), Time) - availability_model.start_time)) <= appointment_model.end_time))

        return after_query.union(before_query, sameday_query, next_query, next_next_query) \
                .order_by('start_time') \
                .limit(1)

    
    @staticmethod
    def firstAvailable(start_time, duration_in_minutes) -> Dict:
        if not AppointmentModel.isValidSize(duration_in_minutes*60):
            return None, None
        
        first_available = AppointmentModel.firstAvailableQuery(start_time, duration_in_minutes).first()
        if first_available:
            return dict(first_available)
        else:
            return None, None
        
    
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

        
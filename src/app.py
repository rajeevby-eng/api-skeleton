from flask import Flask
from src.endpoints import home
from json import JSONEncoder

def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    from src.extensions import db
    db.init_app(app)
         
    # We are doing a create all here to set up all the tables. Because we are using an in memory sqllite db, each
    # restart wipes the db clean, but does have the advantage of not having to worry about schema migrations.
    db.create_all(app=app)
    app.register_blueprint(home)
    from src.seed import seed_data
    with app.app_context():
        seed_data()
    return app

from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
from data_manipulation.database_interface import DatabaseInterface
from data_manipulation.profiles import Profiles
import atexit
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
app.url_map.strict_slashes = False
api = Api(app)

# TODO Authentication of Graphica's web client (instead of '*')
cors = CORS(app, resources={r'/log*': {'origins': '*'}, r'/docs/': {'origins': '*'}})

db = DatabaseInterface(username='default', password='password', url='http://167.172.39.249:8123')
profiles = Profiles(db)


@app.before_request
def clear_trailing():
    from flask import redirect, request

    rp = request.path
    if rp != '/' and rp.endswith('/'):
        return redirect(rp[:-1])


class Logs(Resource):
    def post(self):
        parser = reqparse.RequestParser()

        for field in db.get_fields():
            parser.add_argument(field)

        try:
            params = parser.parse_args()
        except TypeError:
            response = {'error': 'Cannot parse POST request.'}
            return response, 415
        data = {}
        for field in db.get_fields():
            data.update([(field, params[field])])

        if data['timestamp'] is None:
            return {'error': 'You must specify timestamp.'}, 415
        try:
            db.insert_log(data)
        finally:
            response = data
            return response, 201


api.add_resource(Logs, '/log/', '/log')


@app.route('/profiles', methods=['GET'])
def get_profiles():
    p = profiles.get_profiles()
    if p is None:
        return {'error': 'No profiles to send. Use /profiles-with-update endpoint to update them before sending.'}, 404
    else:
        return profiles.get_profiles().to_json(), 200


@app.route('/profiles-with-update', methods=['GET'])
def get_profiles_with_update():
    profiles.update_profiles()
    return profiles.get_profiles().to_json(), 200


@app.route('/clustered-profiles', methods=['GET'])
def get_clustered_profiles():
    p = profiles.get_profiles()
    if p is None:
        profiles.update_profiles()

    profiles.update_clustered_profiles()
    return profiles.get_clustered_profiles().to_json(), 200


@app.route('/docs')
def render_static():
    return render_template('index.html')


# Profiles update scheduler:
scheduler = BackgroundScheduler()
scheduler.add_job(func=profiles.update_profiles, trigger='interval', hours=3)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    app.run(debug=True)

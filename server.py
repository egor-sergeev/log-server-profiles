from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
from database_interface import DatabaseInterface

app = Flask(__name__)
api = Api(app)

# TODO Authentication of Graphica's web client (instead of '*')
cors = CORS(app, resources={r'/log*': {'origins': '*'}})

db = DatabaseInterface(username='default', password='password')


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

        try:
            db.insert_log(data)
        finally:
            response = data
            return response, 201


api.add_resource(Logs, '/log/', '/log')


@app.route('/docs/')
def render_static():
    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)

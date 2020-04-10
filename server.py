from flask import Flask
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

        db.insert_log(data)


api.add_resource(Logs, '/log/', '/log')

if __name__ == "__main__":
    app.run(debug=True)

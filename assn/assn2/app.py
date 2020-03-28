from flask import Flask, request
from flask_restplus import Resource, Api
import json
import requests
# import requests_cache
import pandas as pd
from pandas.io import sql
import sqlite3

app = Flask(__name__)
api = Api(app)

# Constants define
Z_ID = 'z5141401'
ENDPOINT = 'http://api.worldbank.org/v2/countries/all/indicators/'
ENDPOINT_POSTFIX = '?date=2012:2017&format=json&per_page=1000'


# Custom class define
class APIError(Exception):
    """An API Error Exception"""

    def __init__(self, status):
        self.status = status

    def __str__(self):
        return "APIError: status={}".format(self.status)

def write_in_sqlite(dataframe, database_file, table_name):
    """
    :param dataframe: The dataframe which must be written into the database
    :param database_file: where the database is stored
    :param table_name: the name of the table
    """

    cnx = sqlite3.connect(database_file)
    sql.to_sql(dataframe, name=table_name, con=cnx)


def read_from_sqlite(database_file, table_name):
    """
    :param database_file: where the database is stored
    :param table_name: the name of the table
    :return: A Dataframe
    """
    cnx = sqlite3.connect(database_file)
    return sql.read_sql('select * from ' + table_name, cnx)

# Other pre works
# requests_cache.install_cache(cache_name='mystuff_cache',backend = 'sqlite', expire_after = 180)
table_name = 'collections'
database_file = Z_ID + '.db'  # name of sqlite db file that will be created

@api.route('/hello')
class hello_world(Resource):
    def get(self):
        return {'hello': 'world'}

@app.route('/collections', methods=['POST'])
def import_collection():
    if request.method == 'POST':
        # import data to db
        indicator_id = request.args.get('indicator_id')

        req_url = ENDPOINT + str(indicator_id) + ENDPOINT_POSTFIX
        resp = requests.get(req_url)
        if resp.status_code != 200:
            # did not get the data
            raise APIError(resp.status_code)
        else:
            resp_json = str(resp.json())
            df = pd.DataFrame(resp_json)

            return df
            pass

if __name__ == '__main__':
    app.run(debug=True)
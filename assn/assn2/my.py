import json

import pandas as pd
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import inputs
from flask_restplus import reqparse

import requests
import datetime

from pandas.io import sql
import pandas.io.json
import sqlite3

app = Flask(__name__)
api = Api(app,
          default="Books",  # Default namespace
          title="Book Dataset",  # Documentation Title
          description="This is just a simple example to show how publish data as a service.")  # Documentation Description

# the schema of indicator_id
indicator_id = api.model('indicator_id', {'indicator_id': fields.String})

df = pd.DataFrame()

# Constants define
Z_ID = 'z5141401'
ENDPOINT = 'http://api.worldbank.org/v2/countries/all/indicators/'
ENDPOINT_POSTFIX = '?date=2012:2017&format=json&per_page=1000'

INDICATOR_ENDPOINT = 'http://api.worldbank.org/v2/indicators?per_page=10000&format=json&page='

# pages of indicator id in world bank webpage
INDICATOR_PAGES = 2
# database record id, starts from 1
ENTRY_ID = 1

indicator_lst = set()


# Custom class define
class APIError(Exception):
    """An API Error Exception"""

    def __init__(self, status):
        self.status = status

    def __str__(self):
        return "APIError: status={}".format(self.status)


def fetch_data_by_id(indicator_id):
    """
    :param indicator_id: Ths id used to fetch from world bank api
    """
    req_url = ENDPOINT + str(indicator_id) + ENDPOINT_POSTFIX
    resp = requests.get(req_url)
    resp_json = resp.json()
    if (not resp.ok) or 'message' in resp_json[0]:
        raise APIError(resp.status_code)
        return resp_json

    # else return as normal
    return resp_json


def get_all_indicator_id(INDICATOR_PAGES):
    """
    :param INDICATOR_PAGES:
    :return: all indicators got from world bank API
    """
    i = 1
    indicator_lst = set()
    while i <= INDICATOR_PAGES:
        req_link = INDICATOR_ENDPOINT + str(i)
        req = requests.get(req_link)
        indicators = req.json()
        for j in indicators[1]:
            indicator_lst.add(j['id'])
        i += 1
    print("Total %d records of indicator id got." % len(indicator_lst))
    return indicator_lst


def process_data_by_id(resp_json, indicator_id, curr_id):
    """
    :param resp_json:
    :param indicator_id: the id which searched
    :return: list of jsonifyed data
    """
    entries = []
    # add to entries
    for i in resp_json[1]:
        country = i['country']['value']
        indicator_value = i['indicator']['value']
        date = i['date']
        value = i['value']
        # Skip null values
        if value is None:
            continue
        convert_data = {"country": country,
                        "date": date,
                        "value": value
                        }
        entries.append(convert_data)
    entries_str = str(entries)
    # format output
    time = datetime.datetime.now()
    date_str = time.strftime("%Y-%m-%d %H:%M:%S")
    record = {'uri': '/collections/' + str(curr_id),
              'id': curr_id,
              'creation_time': date_str,
              'indicator_id': indicator_id,
              'entries': entries_str
              }
    return record


def write_in_sqlite(dataframe, database_file, table_name):
    """
    :param dataframe: The dataframe which must be written into the database
    :param database_file: where the database is stored
    :param table_name: the name of the table
    """

    cnx = sqlite3.connect(database_file)
    sql.to_sql(dataframe, name=table_name, con=cnx, if_exists='append')

def delete_in_sqlite(database_file, table_name, id):
    # cnx = sqlite3.connect(database_file)
    # sql_query = 'DELETE FROM ' + table_name + ' WHERE id=' + str(id)
    # print(sql_query)
    # sql.read_sql(sql_query, cnx)
    # print("OK")

    # method 2
    conn = sqlite3.connect(database_file)
    cur = conn.cursor()
    sql_query = 'DELETE FROM ' + table_name + ' WHERE id=' + str(id)
    cur.execute(sql_query)
    conn.commit()
    cur.close()
    conn.close()

def search_by_id_in_sqlite(database_file, table_name, id):
    """
    :param database_file: where the database is stored
    :param table_name: the name of the table
    :return: A Dataframe
    """
    cnx = sqlite3.connect(database_file)
    sql_query = 'SELECT * FROM ' + table_name + ' WHERE id=' + str(id)
    return sql.read_sql(sql_query, cnx)

def read_from_sqlite(database_file, table_name):
    """
    :param database_file: where the database is stored
    :param table_name: the name of the table
    :return: A Dataframe
    """
    cnx = sqlite3.connect(database_file)
    return sql.read_sql('select * from ' + table_name, cnx)


def read_last_sqlite_index(database_file, table_name):
    """
    :param database_file: where the database is stored
    :param table_name: the name of the table
    :return: A int
    """
    cnx = sqlite3.connect(database_file)
    ret_df = sql.read_sql('select COUNT(*) from ' + table_name, cnx)
    curr_id = ret_df.values.max()
    return curr_id


# The following is the schema of Book
collection_model = api.model('collection', {
    'uri': fields.String,
    'id': fields.Integer,
    'creation_time': fields.DateTime,
    'indicator_id': fields.String
})



@api.route('/collections')
class CollectionsList(Resource):

    query_parser = api.parser()
    query_parser.add_argument('indicator_id', required=True, type=str)
    @api.response(201, 'Book Created Successfully')
    @api.response(400, 'Validation Error')
    @api.doc(description="Add a new book")
    @api.expect(query_parser, validate=True)
    # Q1
    def post(self, query_parser=query_parser):
        args = query_parser.parse_args()
        indicator_id = args['indicator_id']
        # Request to world bank
        resp_json = fetch_data_by_id(indicator_id)
        # get latest id
        try:
            curr_id = read_last_sqlite_index(database_file, table_name)
        except:
            curr_id = 1
        # process data
        record = process_data_by_id(resp_json, indicator_id, curr_id)
        # Change to dataframe and put all entries to TXT format for SQLite DB.
        record_df = pandas.DataFrame(record, index=[ENTRY_ID])
        # Check if it exists
        try:
            write_in_sqlite(record_df, database_file, table_name)
        except:
            # TODO redundant check
            return {"message": "{} has already been posted".format(indicator_id),
                    "location": "/posts/{}".format(indicator_id)}, 200
        return {
            'uri': record['uri'],
            'id': int(record['id']),
            'creation_time': record['creation_time'],
            'indicator_id': record['indicator_id']
        }


@api.param('collection_id', 'The collections identifier')
@api.route('/collections/<id>')
class data(Resource):
    @api.response(200, 'OK')
    @api.response(404, 'Collection does not exist')
    @api.doc(description="Deleting a collection with the data service")
    # Q2
    def delete(self, id):
        # check if record exists
        rst = search_by_id_in_sqlite(database_file, table_name, id)
        if rst.empty is True:
            api.abort(404, "collection: {} doesn't exist".format(id))
        else:

            delete_in_sqlite(database_file, table_name, id)
            return {
                "message" :"Collection = {} is removed from the database!".format(id)
                }, 200

# Q5
@api.route('/collections/<string:id>/<int:year>/<string:country>')
class IndicatorYearCountry(Resource):
    def get(self,id,year,country):
        print("GET_ 1",id,year,country)

        # check if id is valid
        indicatorCollection = db['indicatorCollection']
        document = indicatorCollection.find_one({"id": id})

        if ( document == None):

            return {
                "message" : "collection_id: "+ id +" does not exist",
                }, 404

        # check if year is valid
        if (year < 2012 or year > 2017):
            return {
                "message" : "year need to br from 2012 to 2017",
                }, 404

        for x in document['entries']:
            if x['country'] == country:
                return {
                    "collection_id":    document['id'],
                    "indicator" :       document['id'],
                    "country" :         country,
                    "year" :            year,
                    "value":            x['value'],
                }, 200

        return {
            "message" : "country: "+country+" is invalid",
            }, 404


if __name__ == '__main__':
    table_name = 'collections'
    database_file = Z_ID + '.db'  # name of sqlite db file that will be created

    # pre processing
    # indicator_lst = get_all_indicator_id(INDICATOR_PAGES)
    # for debug use only
    indicator_lst = {'fin33.14.a', 'NY.GDP.MKTP.CD'}

    app.run(debug=True)

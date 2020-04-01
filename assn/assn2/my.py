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

    # Q3
    @api.response(200, 'Successful')
    @api.doc(description="Get all books")
    def get(self):
        # args = get_query_parser.parse_args()
        # retrieve the query parameters
        order_by = args.get('order')
        ascending = args.get('ascending', True)

        if order_by:
            df.sort_values(by=order_by, inplace=True, ascending=ascending)

        json_str = df.to_json(orient='index')

        # convert the string JSON to a real JSON
        ds = json.loads(json_str)
        ret = []

        for idx in ds:
            book = ds[idx]
            book['Identifier'] = int(idx)
            ret.append(book)

        return ret

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


        # check if the given identifier does not exist
        if id in df.index:
            return {"message": "A book with Identifier={} is already in the dataset".format(id)}, 400

        # Put the values into the dataframe
        for key in book:
            if key not in collection_model.keys():
                # unexpected column
                return {"message": "Property {} is invalid".format(key)}, 400
            df.loc[id, key] = book[key]

        # df.append(book, ignore_index=True)
        return {"message": "Book {} is created".format(id)}, 201


@api.route('/books/<int:id>')
@api.param('id', 'The Book identifier')
class Books(Resource):
    @api.response(404, 'Book was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get a book by its ID")
    def get(self, id):
        if id not in df.index:
            api.abort(404, "Book {} doesn't exist".format(id))

        book = dict(df.loc[id])
        return book

    @api.response(404, 'Book was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Delete a book by its ID")
    def delete(self, id):
        if id not in df.index:
            api.abort(404, "Book {} doesn't exist".format(id))

        df.drop(id, inplace=True)
        return {"message": "Book {} is removed.".format(id)}, 200

    @api.response(404, 'Book was not found')
    @api.response(400, 'Validation Error')
    @api.response(200, 'Successful')
    @api.expect(collection_model, validate=True)
    @api.doc(description="Update a book by its ID")
    def put(self, id):

        if id not in df.index:
            api.abort(404, "Book {} doesn't exist".format(id))

        # get the payload and convert it to a JSON
        book = request.json

        # Book ID cannot be changed
        if 'Identifier' in book and id != book['Identifier']:
            return {"message": "Identifier cannot be changed".format(id)}, 400

        # Update the values
        for key in book:
            if key not in collection_model.keys():
                # unexpected column
                return {"message": "Property {} is invalid".format(key)}, 400
            df.loc[id, key] = book[key]

        df.append(book, ignore_index=True)
        return {"message": "Book {} has been successfully updated".format(id)}, 200


if __name__ == '__main__':
    table_name = 'collections'
    database_file = Z_ID + '.db'  # name of sqlite db file that will be created

    # pre processing
    # indicator_lst = get_all_indicator_id(INDICATOR_PAGES)
    # for debug use only
    indicator_lst = {'fin33.14.a', 'NY.GDP.MKTP.CD'}

    app.run(debug=True)

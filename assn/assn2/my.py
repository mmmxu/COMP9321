import json

import flask
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
          default="World bank collections",
          title="Collections Dataset",
          description="The API service for COMP9321.")  # Documentation Description

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
              'country': country,
              'date': date,
              'value': value,
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

def search_by_indicator_id_in_sqlite(database_file, table_name, indicator_id):
    """
    :param database_file: where the database is stored
    :param table_name: the name of the table
    :return: A Dataframe
    """
    cnx = sqlite3.connect(database_file)
    sql_query = 'SELECT * FROM ' + table_name + ' WHERE indicator_id= "' + str(indicator_id) +'"'
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

def read_collection_sqlite(database_file, table_name, id, year=None, country=None):
    """
    Get year and other relevent data from SQLite database
    :param database_file: the database file
    :param table_name:
    :param id:
    :param year:
    :param country:
    :return: dataframe
    """
    conn = sqlite3.connect(database_file)

    # define sql query
    if not year and not country:
        sql_query = f'select country,date,value from {table_name}'

    elif year and country:  # if both year and country condition apply
        sql_query = f'select country,date,value from {table_name} where date={year} and country="{country}"'

    elif year:  # if only year
        sql_query = f'select country,date,value from {table_name} where date={year}'

    elif country:  # if only country
        sql_query = f'select country,date,value from {table_name} where country="{country}"'

    # debug
    rst = sql.read_sql(sql_query, conn)

    return rst

def read_q6_sqlite(database_file, table_name, id, year):
    conn = sqlite3.connect(database_file)

    sql_query = f'select * from {table_name} where date={year} and id="{id}"'

    rst = sql.read_sql(sql_query, conn)

    return rst


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
    order_parser = api.parser()
    query_parser.add_argument('indicator_id', required=False, type=str)
    order_parser.add_argument('order_by', required=False, type=str)
    @api.response(201, 'Book Created Successfully')
    @api.response(400, 'Validation Error')
    @api.doc(description="Add a new book")
    @api.expect(query_parser, validate=True)
    # Q1
    def post(self, query_parser=query_parser ):
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
        # the first record do not need to check
        if curr_id ==1:
            write_in_sqlite(record_df, database_file, table_name)
            return {
                'uri': record['uri'],
                'id': int(record['id']),
                'creation_time': record['creation_time'],
                'indicator_id': record['indicator_id']
            }
        rst = search_by_indicator_id_in_sqlite(database_file, table_name, indicator_id)
        if rst.empty is True:
            write_in_sqlite(record_df, database_file, table_name)
            return {
                'uri': record['uri'],
                'id': int(record['id']),
                'creation_time': record['creation_time'],
                'indicator_id': record['indicator_id']
            }
        else:
            return {"message": "{} has already been posted".format(indicator_id),
                    "location": "/posts/{}".format(indicator_id)}, 200

    # Q3
    order_parser = api.parser()
    order_parser.add_argument('order_by', required=False, type=str)
    @api.response(201, 'Book Created Successfully')
    @api.response(400, 'Validation Error')
    @api.doc(description="Add a new book")
    @api.expect(order_parser, validate=True)
    def get(self, order_parser = order_parser):
        print("Got here")
        # order_by = flask.request.args.get("order_by")
        args = order_parser.parse_args()
        order_by = args['order_by']
        # resolve parameters
        order_by = order_by.strip("{} ").split(",")
        element = [ele.strip("+") for ele in order_by]
        element = [ele.strip("-") for ele in order_by]
        # remove the start space
        element = [ele.strip() for ele in element]
        # rename to sql table name
        element = ['indicator_id' if ele=='indicator' else ele for ele in element]
        ascending = [not '-' in ele for ele in order_by]



        df = read_from_sqlite(database_file, table_name)
        df.columns = df.columns.str.strip()
        # sort data
        df = df.sort_values(by=element, ascending=ascending)


        # output certain columns only
        df["uri"] = "/collections/" + df["id"].astype(str)
        df["indicator"] = df["indicator_id"]
        df = df[["uri", "id", "creation_time", "indicator"]]
        return df.to_dict('r')




@api.param('collection_id', 'The collections identifier')
@api.route('/collections/<int:id>')
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
                "message" :"The collection = {} was removed from the database!".format(id),
                "id" : id
                }, 200

    # Q4
    @api.response(200, 'OK')
    @api.response(404, 'Collection does not exist')
    @api.doc(description="Retrieve a collection")
    def get(self, id):
        table = search_by_id_in_sqlite(database_file, table_name, id)
        if table.empty is True:
            api.abort(404, "collection: {} doesn't exist".format(id))
        else:
            # format the certain columns only
            df = table
            df["indicator"] = df["indicator_id"]
            df = df[["id", "indicator", "creation_time", "entries"]]

            return df.to_dict('r')

    # Q5
    @api.response(200, 'OK')
    @api.response(404, 'Collection does not exist')
    @api.doc(description="Deleting a collection with the data service")
    def advanced_get(self, id, year, country):
        # Read from SQL
        df = read_collection_sqlite(database_file, table_name, id, year, country)
        # Add other necessary columns
        df['id'] = id
        df['indicator'] = search_by_id_in_sqlite(database_file,table_name,id)['indicator_id']
        print(df)
        # !!important! sort the column order
        if df.empty is True:
            api.abort(404, "collection: {} doesn't exist".format(id))
        else:
            df['year'] = df['date']
            df = df[['id', 'indicator', 'country', 'year', 'value']]
            return df.to_dict('r')


    # Q6
    @api.response(200, 'OK')
    @api.response(404, 'Collection does not exist')
    @api.doc(description="Deleting a collection with the data service")
    def advanced_get_by_year(self, id, year, q=None):
        # Read from SQL
        df = read_q6_sqlite(database_file,table_name, id, year)
        # empty check
        if df.empty is True:
            api.abort(404, "collection: {} doesn't exist".format(id))
        # check if query exists
        if q:
            # negative flag, Ture when q is a negative number (q=-10)
            nega_flag = q[0] is '-'
            n = q.strip('+-')
            # sort the dataframe
            df = df.sort_values('value', ascending=nega_flag).head(int(n))

            # format certain output only

            df['indicator'] = df['indicator_id']
            df['indicator_value'] = df['value']
            df = df[['indicator', 'indicator_value', 'entries']]
            return df.to_dict('r')


## Q5
@api.route("/collections/<int:id>/<int:year>/<string:country>")
class Q5(Resource):
    def get(self, id, year, country):
        return data.advanced_get(self, id, year, country), 200

## Q6
@api.route("/collections/<int:id>/<int:year>")
class Q6(Resource):
    def get(self, id, year):
        q = flask.request.args.get('q')
        # digit check
        if q[1:].isdigit():
            result = data.advanced_get_by_year(self, id, year, q)
            return result,200
        else:
            api.abort(400, "Invalid query format, please input a digit.")


if __name__ == '__main__':
    table_name = 'collections'
    database_file = Z_ID + '.db'  # name of sqlite db file that will be created

    # pre processing
    # indicator_lst = get_all_indicator_id(INDICATOR_PAGES)
    # for debug use only
    indicator_lst = {'fin33.14.a', 'NY.GDP.MKTP.CD'}

    app.run(debug=True)

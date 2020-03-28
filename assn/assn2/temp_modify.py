# request data from worldbank as a RESTful client
import requests

def get_data_of_id(indicator_id):
    # http://api.worldbank.org/v2/countries/all/indicators/NY.GDP.MKTP.CD?date=2012:2017&format=json&page=2
    # http://api.worldbank.org/v2/countries/all/indicators/NY.GDP.MKTP.CD?date=2012:2017&format=json&per_page=2000
    r = requests.get('http://api.worldbank.org/v2/countries/all/indicators/' + indicator_id + '?date=2012:2017&format=json&per_page=2000')
    data_of_id = r.json()
    print('Get status Code:' + str(r.status_code))
    if r.ok:
        print('***** get data *****')
        return data_of_id
    else:
        print('***** Error:' + data_of_id['message'] + ' *****')        
        return data_of_id

# build own RESTful service
import json
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import inputs
from flask_restplus import reqparse
#from pymongo import *
import sqlite3
import pandas as pd
from pandas.io import sql
import datetime

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

# Constants define
Z_ID = 'z5141401'
ENDPOINT = 'http://api.worldbank.org/v2/countries/all/indicators/'
ENDPOINT_POSTFIX = '?date=2012:2017&format=json&per_page=1000'
# database record id, starts from 1
ENTRY_ID = 1

# use flask_RESTplus establish an api
app = Flask(__name__)
api = Api(app,
        default="collections",  # Default namespace
        title="Dataset",  # Documentation Title
        description="This is a a Flask-Restplus data service that \
        allows a client to read and store some publicly available economic indicator data for countries around the world, \
        and allow the consumers to access the data through a REST API.")  # Documentation Description
# the schema of indicator_id
indicator_id = api.model('indicator_id', {'indicator_id': fields.String})
# the schema of Book
data_model = api.model('entries', {
    'country': fields.String,
    'date': fields.String,
    'value': fields.String,
})
# the request parser for query
parser = reqparse.RequestParser()
parser.add_argument('query')

@api.route('/collections')
class datalist(Resource):
##############################
#   question 1
##############################
    @api.response(404, 'Indicator id does not exist')
    @api.response(201, 'Create')
    @api.response(200, 'OK')
    @api.doc(description="Import a collection from the data service")
    @api.expect(indicator_id, validate=True)   
    def post(self):
    ###### make a requst with indicator given by client ######
        # indicator id, given by user
        indicator_id = request.json['indicator_id']
        print("***** get request of indicator id: {} *****".format(indicator_id))
        # if not valid indicator
        if indicator_id not in list_of_indicator_id:
            print("***** invalid indicator id: {} *****".format(indicator_id))
            api.abort(404, "Indicator id {} doesn't exist".format(indicator_id))
        # request data of indicator id from worldbank
        data = get_data_of_id(indicator_id)
    ###### handle the request ######
        # if get the error message from worldbank, return the error to the client
        if "message" in data[0]:
            print("***** get error message when request with {} *****".format(indicator_id))
            return data, int(data[0]["message"][0]["id"])
        # else, convert the response from worldbank
        entries = []
        for i in data[1]:
            country = i['country']['value']
            indicator_value = i['indicator']['value']
            date = i['date']
            value = i['value']
            convert_data = {"country": country,
                             "date": date, 
                             "value": value
                             }
            #a = json.dumps(convert_data)
            #b = json.loads(a)
            entries.append(convert_data)
        time = datetime.datetime.now()
        date_str = time.strftime("%Y-%m-%d %H:%M:%S")
        post = {'_id': indicator_id,
            'indicator': indicator_id,
            'indicator_value': indicator_value,
            'creation_time': date_str,
            'entries': entries
            }
    ###### insert the document into mlab ######
        posts = pd.DataFrame(post)
        try:
            write_in_sqlite(posts, database_file, table_name)
        except:
            return {"message" : "{} has already been posted".format(indicator_id),
                    "location" : "/posts/{}".format(indicator_id)}, 200
        return { 
            "location" : "/posts/{}".format(indicator_id), 
            "collection_id" : indicator_id,  
            "creation_time": date_str,
            "indicator" : indicator_id
            }, 201
##############################
#   question 3
##############################
    @api.response(200, 'OK')
    @api.doc(description="Retrieve the list of available collections")
    def get(self):
        availiable_collections = []
        for item in db.posts.find():      
            c_id = item['_id'] 
            time = item['creation_time']
            availiable_collection = { 
                "location" : "/posts/{}".format(c_id), 
                "collection_id" : c_id,  
                "creation_time": time,
                "indicator" : c_id
                }  
            availiable_collections.append(availiable_collection)
        return availiable_collections, 200

@api.param('collection_id', 'The collections identifier')
@api.param('year', 'The date, year')
@api.param('country', 'The country name')
@api.route('/collections/<collection_id>/<year>/<country>')
class data(Resource):
##############################
#   question 5
##############################
    @api.response(200, 'OK')
    @api.response(404, 'No collection matches')
    @api.doc(description="Retrieve economic indicator value for given country and a year")
    def get(self, collection_id, year, country):
        collection = db.posts.find_one({"_id": collection_id})
        if not collection:
            api.abort(404, "Indicator id {} doesn't exist".format(collection_id))
        for country_data in collection["entries"]:
            if (country_data["country"] == country) & (country_data["date"] == year):
                return { 
                   "collection_id": collection_id,
                   "indicator" : collection_id,
                   "country": country, 
                   "year": year,
                   "value": country_data["value"]
                }, 200
        api.abort(404, "the collections with indicator id: {} of year {} country {} doesn't exist".format(collection_id,year,country))



@api.param('collection_id', 'The collections identifier')
@api.route('/collections/<collection_id>')
class data(Resource):
##############################
#   question 2
##############################
    @api.response(200, 'OK')
    @api.response(404, 'Collection does not exist')
    @api.doc(description="Deleting a collection with the data service")
    def delete(self, collection_id):
        delete_id = db.posts.delete_one({"_id": collection_id})
        print("***** delete {} *****".format(collection_id))
        if delete_id.deleted_count:
            return { 
                "message" :"Collection = {} is removed from the database!".format(collection_id)
                }, 200
        # deleted_count = 0, unexisting id 
        api.abort(404, "collection: {} doesn't exist".format(collection_id))
##############################
#   question 4
##############################
    @api.response(200, 'OK')
    @api.response(404, 'collection with id does not exist')
    @api.doc(description="Retrieve a collection")
    def get(self, collection_id):
        collection = db.posts.find_one({"_id": collection_id})
        if not collection:
            api.abort(404, "collection with id {} doesn't exist".format(collection_id))
        return collection, 200

@api.param('collection_id', 'The collections identifier')
@api.param('year', 'The date, year')
@api.route('/collections/<collection_id>/<year>')
class data(Resource):
##############################
#   question 6
##############################
    @api.response(404, 'collections was not found')
    @api.response(400, 'Bad query format')
    @api.response(200, 'OK')
    @api.doc(description="Retrieve top/bottom economic indicator values for a given year")
    @api.expect(parser)
    def get(self, collection_id, year):
    ###### get query and handle it ######
        args = parser.parse_args()
        order_by = args.get('query')
        null_query = False
        ascending = False
        if order_by:
            if 'top' in order_by:
                ascending = True
                try:
                    n = int(order_by[3:])
                except:
                    return {"message": "please follow the query format: top<N> or bottom<N> while N[1, 100]".format(id)}, 400
            elif 'bottom' in order_by:
                ascending = False
                try:
                    n = int(order_by[6:])
                except:
                    return {"message": "please follow the query format: top<N> or bottom<N> while N[1, 100]".format(id)}, 400
            else:
            # query format incorrect, return 400 bad request
                return {"message": "please follow the query format: top<N> or bottom<N> while N[1, 100]".format(id)}, 400
            if n < 1 | n > 100:
                return {"message": "please follow the query format: top<N> or bottom<N> while N[1, 100]".format(id)}, 400
        else:
            null_query = True
    ###### get collection of collection_id ######
        collection = db.posts.find_one({"_id": collection_id})
        if not collection:
            # unexisting collection, return 404 error
            api.abort(404, "collection {} doesn't exist".format(collection_id))
    ###### ordering entries ######
        # get the country and value info in collection with year
        order_dict = {}
        for country_data in collection["entries"]:
            if country_data["date"] == year:
                if country_data["value"]:
                    order_dict[country_data["country"]] = country_data["value"]
                else:
                    order_dict[country_data["country"]] = -1
        # ordering
        sorted_dict = sorted(order_dict.items(), key = lambda item: item[1], reverse = ascending)
    ###### create new entries ######
        if null_query:
            count = len(sorted_dict)
        else:
            if len(sorted_dict) > n:
                count = n
            else:
                count = len(sorted_dict)
        new_entries = []
        i = 0
        for i in range(count):
            if sorted_dict[i][1] == -1:
                new_entries.append({ 
                     "country": sorted_dict[i][0], 
                     "date": year,
                     "value": None
                })
            else:
                new_entries.append({ 
                     "country": sorted_dict[i][0], 
                     "date": year,
                     "value": sorted_dict[i][1]
                })
            i = 1 + i
        return { 
           "indicator": collection_id,
           "indicator_value": "GDP (current US$)",
           "entries" : new_entries 
            }, 200

if __name__ == '__main__':

    # DB conn
    table_name = 'collections'
    database_file = Z_ID + '.db'  # name of sqlite db file that will be created

    # get the list of indicators
    r1 = requests.get('http://api.worldbank.org/v2/indicators?per_page=10000&format=json')
    indicators = r1.json()
    list_of_indicator_id = set()
    for i in indicators[1]:
        list_of_indicator_id.add(i['id'])
    r2 = requests.get('http://api.worldbank.org/v2/indicators?per_page=10000&format=json&page=2')
    indicators = r2.json()
    for i in indicators[1]:
        list_of_indicator_id.add(i['id'])
    print("***** indicator id information has been update *****")
    app.run(debug=True)




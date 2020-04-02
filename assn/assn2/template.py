import datetime
import sqlite3
import os
import pandas
import requests
from pandas.io import sql
import flask
from flask_restplus import Resource, Api

db_name = 'z000.db'


class Data:
    def __init__(self):
        self.current_id = 0
        self.all_tables = list()

        # delete the exist database to avoid problems when rerun the server
        if os.path.exists(db_name):
            os.remove(db_name)

    def import_data_from_worldbank(self, indicator_id):
        # 1. request
        url = f"http://api.worldbank.org/v2/countries/all/indicators/{indicator_id}?date=2012:2017&format=json&per_page=1000"
        r = requests.get(url)
        assert r.status_code == 200

        # load as df
        df = pandas.DataFrame(r.json()[1])

        # indicator --> indicator + value
        df['indicator_value'] = df.indicator.apply(lambda x: x['value'])
        df.indicator = df.indicator.apply(lambda x: x['id'])

        # country name {'id': 'LT', 'value': 'Lithuania'}	--> "Lithuania"
        df.country = df.country.apply(lambda x: x['value'])

        self.current_id += 1

        # save into sql
        conn = sqlite3.connect(db_name)
        sql.to_sql(df, name=f"table{self.current_id}", con=conn)

        # current time
        now = datetime.datetime.now()
        create_time = now.strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "uri": f"/collections/{self.current_id}",
            "id": self.current_id,
            "creation_time": create_time,
            "indicator_id": indicator_id
        }

        self.all_tables.append(result)

        return result

    def drop(self, id):
        # Q2. remove a collection from db and self.all_tables
        # 1. delete from database
        # todo if error
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        table_name = f"table{id}"
        cur.execute(f"DROP TABLE {table_name};")
        conn.commit()
        conn.close()

        # 2. delete from table.all_tables
        self.all_tables = [table for table in self.all_tables if table['id'] != id]

    def sorted(self, order_by):
        '''
        Q3
        :param order_by: str: "{+id, +creation, -indicator, ...}"
        :return:
        '''
        # clean the order_by
        order_by = order_by.strip("{} ").split(",")
        element = [ele.strip("+-") for ele in order_by]
        ascending = [not '-' in ele for ele in order_by]

        df = pandas.DataFrame(self.all_tables)

        return df.sort_values(element, ascending=ascending).to_dict('r')

    def get(self, id) -> dict:
        '''
        Q4 sub function, build the response
        :return:
        '''

        # 1. get the table info by id
        table = self.get_table(id)

        # 2. build the entries
        table['entries'] = retrieve_collection(id).to_dict('r')

        return table

    def get_by_id_year_country(self, id, year, country):
        '''
        Q5 main function: get the data from database according to the condition

        '''

        # 1. get df from db
        df = retrieve_collection(id, year, country)
        # 2. make the data fit the format
        # add two column
        df['id'] = id
        df['indicator'] = self.get_table(id)['indicator_id']
        # sort the column order
        df = df[['id', 'indicator', 'country', 'date', 'value']]

        return df.to_dict('r')

    def get_table(self, id):
        '''
        Q4 subfunction, get the table info by id
        :param id:
        :return:
        '''
        for table in self.all_tables:
            if table['id'] == id:
                return table
        return {}

    def get_n_country_by_year(self, id, year, q=None):
        '''
        Q6 main function: get the top or bot n eco indicator values for a given condition
        :param id: int
        :param year: int
        :param q: str, "+10" means top 10
        :return:
        '''
        # get data from database
        df = retrieve_collection(id, year)

        if q:
            # parse the q: -5 means is_bot=True, n=5 == bottom 5
            is_bot = q[0] is '-'
            n = q.strip('+-')
            # sort the dataframe
            df = df.sort_values('value', ascending=is_bot).head(int(n))
        else:
            pass

        return df.to_dict('r')


def retrieve_collection(id, year=None, country=None):
    # read it back from sql according by id and year
    conn = sqlite3.connect(db_name)
    table_name = f"table{id}"

    if not year and not country:
        sql_query = f'select country,date,value from {table_name}'

    elif year and country:  # if both year and country condition apply
        sql_query = f'select country,date,value from {table_name} where date={year} and country="{country}"'

    elif year:  # if only year
        sql_query = f'select country,date,value from {table_name} where date={year}'

    elif country:  # if only country
        sql_query = f'select country,date,value from {table_name} where country="{country}"'

    df = sql.read_sql(sql_query, conn)

    return df




# 1. init
app = flask.Flask(__name__)
api = Api(app)
data = Data()


# 2. start:
@api.route("/collections")
class Q1nQ3(Resource):
    def post(self):
        # 1. get the user query
        indicator_id = flask.request.args.get('indicator_id')

        # 2. import the data to our database
        result = data.import_data_from_worldbank(indicator_id)

        # todo if the error occurs?
        return result, 201

    def get(self):
        # Q3
        # get the query arg
        order_by = flask.request.args.get("order_by")
        if order_by:
            result = data.sorted(order_by)
            # todo if error
            return result, 200

        return data.all_tables, 200


@api.route("/collections/<int:id>")
class Q2nQ4(Resource):
    def delete(self, id):
        '''
        Q2 delete a collection by id
        :param id:
        :return:
        '''
        # todo if the table exist

        data.drop(id)

        return {"message": f"The collection {id} was removed from the database"}, 200

    def get(self, id):
        '''
        Q4 get a collection by id
        :param id:
        :return:
        '''

        # build the result
        result = data.get(id)

        # todo if error
        return result, 200

@api.route("/collections/<int:id>/<int:year>/<string:country>")
class Q5(Resource):
    def get(self, id, year, country):
        '''
        Q5 get spec data from db according to the condition
        :param id: int
        :param year: int
        :param country: str
        :return: response
        '''
        # todo if error
        return data.get_by_id_year_country(id,year,country), 200

@api.route("/collections/<int:id>/<int:year>")
class Q6(Resource):
    def get(self, id, year):
        '''
        Q6 get a list according to the condition
        :param id:
        :param year:
        :return:
        '''
        # top n or bot n
        q = flask.request.args.get('q') # todo make sure it's q or query

        result = data.get_n_country_by_year(id, year, q)
        return result,200

# 3. run
app.run()
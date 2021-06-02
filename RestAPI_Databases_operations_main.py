from flask import Flask, render_template, request, jsonify
import re
import logging as lg
import mysql.connector as connection
import csv
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

app = Flask(__name__)

lg.basicConfig(filename = "logfile.log", level = lg.INFO, format = '%(asctime)s %(name)s %(message)s')

@app.route('/mysql', methods=['POST']) # for calling the API from Postman/SOAPUI
def mysql_operations():

    try:
        #print("C")
        if (request.method=='POST'):
            #print("D")
            request_type = request.json['request_type']
            #print("E")
            mydb = connection.connect(host="localhost", user="root", passwd="mysql123", database="testdb1",
                                      use_pure=True)

            # print("B")
            # print(request_type)
            if request_type == "create_table":

                table_name = request.json['table_name']
                col_definitions = request.json['column_definitions']

                query = "create table if not exists " + table_name + "("

                for i in col_definitions.items():
                    query = query + i[0] + " " + i[1] + ","

                query = re.split(",$", query)[0]

                query += ")"

                # print(query)

                cur = mydb.cursor()
                cur.execute(query)

                return jsonify("table " + table_name + " created successfully")

            elif request_type == "insert_single":
                table_name = request.json['table_name']
                insert_values = request.json["column_values"]

                query = "insert into " + table_name + " values " + str(tuple(insert_values))
                # print(query)

                cur = mydb.cursor()
                cur.execute(query)
                mydb.commit()

                return jsonify("table " + table_name + " record inserted successfully")

            elif request_type == "update":
                # print("A")
                table_name = request.json['table_name']

                update_query = request.json['update_query']

                # print(update_query)

                cur = mydb.cursor()
                cur.execute(update_query)
                mydb.commit()

                return jsonify("table " + table_name + " record updated successfully")

            elif request_type =="bulk_insert":
                table_name = request.json['table_name']
                csv_file_path = request.json['csv_file_path']

                # print(csv_file_path)

                with open(csv_file_path, 'r') as file:
                    data_csv = csv.reader(file, delimiter='\n')
                    # print(data_csv)
                    for i in data_csv:
                        # print(i)
                        query = "insert into " + table_name + " values ({});".format(', '.join([value for value in i]))
                        query = query.replace('“', '"').replace('”', '"')
                        # print(query)

                        cur = mydb.cursor()
                        cur.execute(query)
                        # print(', '.join([value for value in i]))
                        # print([value for value in i])

                    mydb.commit()

                return jsonify("table " + table_name + " records inserted successfully")

            elif request_type == "delete_from_table":
                table_name = request.json['table_name']

                delete_query = request.json['delete_query']

                # print(delete_query)

                cur = mydb.cursor()
                cur.execute(delete_query)
                mydb.commit()

                return jsonify("table " + table_name + " record deleted successfully")

            elif request_type == "download_table":
                table_name = request.json['table_name']

                download_file_path = request.json['download_file_path']

                # print(download_file_path)

                os.chdir(download_file_path)

                query = "select * from " + table_name
                cur = mydb.cursor()
                cur.execute(query)

                downloaded_data = cur.fetchall()

                file_name = table_name + '.csv'

                with open(file_name, 'w') as downloaded_file:
                    file_writer = csv.writer(downloaded_file)
                    for i in range(len(downloaded_data)):
                        file_writer.writerow(downloaded_data[i])

                file_path = os.path.join(download_file_path, file_name)
                return jsonify("table " + table_name + " downloaded successfully in file location " +
                               file_path)

            else:
                return "invalid request"

    except Exception as e:
        print("Check logs for error")
        lg.error("Error occured here")
        lg.exception(e)

        return jsonify("an error occured")


@app.route('/cassandra', methods=['POST']) # for calling the API from Postman/SOAPUI
def cassandra_operations():

    try:
        #print("C")
        if (request.method=='POST'):
            #print("D")
            request_type = request.json['request_type']
            #print("E")

            cloud_config = {
                'secure_connect_bundle': '/Users/deepakmongia/Documents/Data Science/iNeuron/Databases/Cassandra/secure-connect-testdb.zip'
            }
            auth_provider = PlainTextAuthProvider('UfmZkOkIfTCUdDYyqKlOrFlA',
                                                  'pHSdfvu08j-,7ecZozLkNQOhz7E+yUAn0E.c8XfxndLa3a8sRO_qC7GMg7P9BDWp4P8TsbsS.zOOT0PZonZuKIeoNPWa++zQB_TEoY9,9csiApQLyqQfgb69MbZuhZ34')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            # print("B")
            # print(request_type)
            if request_type == "create_table":

                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                col_definitions = request.json['column_definitions']

                query = "create table if not exists " + keyspace_name + "." + table_name + "("

                for i in col_definitions.items():
                    query = query + i[0] + " " + i[1] + ","

                query = re.split(",$", query)[0]

                query += ");"

                print(query)

                session.execute(query)

                return jsonify("table " + table_name + " created successfully")

            elif request_type == "insert_single":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                insert_values = request.json["column_values"]

                col_names_insert = ""
                col_names_query = " SELECT keyspace_name, column_name FROM system_schema.columns where keyspace_name = '" + \
                    keyspace_name + "'" + " AND table_name = " + "'" + table_name + "' ;"

                print(col_names_query)
                row = session.execute(col_names_query)
                for i in row:
                    col_names_insert += list(i)[1]
                    col_names_insert += ", "

                col_names_insert = re.split(", $", col_names_insert)[0]

                query = "insert into " + keyspace_name + "." + table_name + "(" + col_names_insert + ")" + \
                        " values " + str(tuple(insert_values)) + ";"
                print(query)
                session.execute(query)
                return jsonify("table " + table_name + " record inserted successfully")

            elif request_type == "update":
                # print("A")
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                update_query = request.json['update_query']

                # print(update_query)

                session.execute(update_query)

                return jsonify("table " + keyspace_name + "." + table_name + " record updated successfully")

            elif request_type =="bulk_insert":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']
                csv_file_path = request.json['csv_file_path']

                # print(csv_file_path)

                col_names_insert = ""
                col_names_query = " SELECT keyspace_name, column_name FROM system_schema.columns where keyspace_name = '" + \
                                  keyspace_name + "'" + " AND table_name = " + "'" + table_name + "' ;"

                print(col_names_query)
                row = session.execute(col_names_query)
                for i in row:
                    col_names_insert += list(i)[1]
                    col_names_insert += ", "

                col_names_insert = re.split(", $", col_names_insert)[0]

                with open(csv_file_path, 'r') as file:
                    data_csv = csv.reader(file, delimiter='\n')
                    # print(data_csv)
                    for i in data_csv:

                        query = "insert into " + keyspace_name + "." + table_name + "(" + col_names_insert + ")" + \
                                " values ({});"
                        query = query.format(', '.join([value for value in i]))
                        query = query.replace('“', "'").replace('”', "'")

                        print(query)

                        session.execute(query)

                return jsonify("table " + table_name + " records inserted successfully")

            elif request_type == "delete_from_table":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']

                delete_query = request.json['delete_query']

                # print(delete_query)

                session.execute(delete_query)

                return jsonify("table " + keyspace_name + "." + table_name + " record deleted successfully")

            elif request_type == "download_table":
                keyspace_name = request.json['keyspace_name']
                table_name = request.json['table_name']

                download_file_path = request.json['download_file_path']

                # print(download_file_path)

                os.chdir(download_file_path)

                query = "select * from " + keyspace_name + "." + table_name

                downloaded_data = []

                download_data = session.execute(query)
                for row in download_data:
                    downloaded_data.append(row)

                file_name = table_name + '.csv'

                with open(file_name, 'w') as downloaded_file:
                    file_writer = csv.writer(downloaded_file)
                    for i in range(len(downloaded_data)):
                        file_writer.writerow(downloaded_data[i])

                file_path = os.path.join(download_file_path, file_name)
                return jsonify("table " + table_name + " downloaded successfully in file location " +
                               file_path)

            else:
                return "invalid request"


    except Exception as e:
        print("Check logs for error")
        lg.error("Error occured here")
        lg.exception(e)

        return jsonify("an error occured")


if __name__ == '__main__':
    app.run()

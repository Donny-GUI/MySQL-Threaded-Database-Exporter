from mysql.connector import pooling
from mysql.connector.errors import ProgrammingError
import threading
from queue import Queue
import json
import os
import shutil
from decimal import Decimal
from time import time as seconds
import datetime
import csv
import atexit
import multiprocessing as mp
import time

encodings =  ['ascii', 'cp852', 'cp865', 'cp1255', 'utf_32_be', 'utf_32_le', 'cp1256', 'cp866', 'iso8859_16', 'cp857',  'utf_32', 'cp1253', 'cp1251',  'cp273', 'iso8859_9', 'cp1250', 'cp855', 'cp1257', 'cp864', 'iso8859_2', 'cp869', 'iso8859_3', 'iso8859_6', 'cp858', 'utf_16', 'cp950', 'cp775', 'cp860', 'latin_1', 'iso8859_5', 'cp1252', 'iso2022_jp_3', 'base64_codec', 'iso8859_4', 'cp1125', 'iso8859_13', 'iso8859_7', 'cp862', 'cp037','cp1140', 'cp437', 'iso8859_8', 'cp949', 'iso8859_10', 'utf_16_le', 'cp863', 'utf_8', 'iso8859_11', 'iso2022_jp_1', 'uu_codec', 'utf_16_be', 'cp861', 'iso2022_kr', 'cp1254', 'kz1048', 'cp1026', 'iso8859_14']


class Table:
    def __init__(self, name, database, columns, rows):
        self.columns = columns
        self.name = name
        self.database = database
        self.rows = rows

    def export_to_csv(self):
        fp = os.path.join(os.getcwd(), "csv", self.database)
        os.makedirs(fp, exist_ok=True)
        file_path = os.path.join(fp, self.name)
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self.columns)
            writer.writerows(self.rows)
    
    def export_to_json(self):
        data = []
        fp = os.path.join(os.getcwd(), "json", self.database)
        os.makedirs(fp, exist_ok=True)
        file_path = os.path.join(fp, self.name)
        data = [self.columns]
        for row in self.rows:
            r = []
            for datum in row:
                r.append(convert_mysql_type(datum))
            data.append(r)
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)


def convert_mysql_type(value):
    if value is None:
        return None
    elif isinstance(value, (int, float, bool, Decimal)):
        return str(value)
    elif isinstance(value, datetime.date):
        return value.isoformat()
    elif isinstance(value, datetime.datetime) or isinstance(value, datetime.time):
        return value.isoformat()
    elif isinstance(value, bytes):
        for enc in encodings:
            try:
                return value.decode(enc)
            except:
                continue
    elif isinstance(value, list):
        return "list("+", ".join(value)+")"
    else:
        # Handle other MySQL specific types if needed
        return str(value)
    

def flip_tables(tables, processess, exportedfiles):
     for i in range(exportedfiles//processess):
        try:
            table = tables.pop(0)
            table.export_to_json()
        except:
            break


class ThreadedDatabase:
    def __init__(self, host, user, password, database=None):
        self.start_time = seconds()
        self.delta_time = 0
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection_pool = self.new_connection_pool()
        self.database_queue = Queue()
        self.table_queue = Queue()
        self.map = {}
        self.files_exported = 0
        self.total_files = 0
        self.tables = []
        self.cutter = 0
        self.mpthreads = []
        self.total_Table_threads = 0
        self.__enumerate_database()
        self.__load_time()
        self.__delete_json_folder()

    def new_connection_pool(self, pool_size=32, database: str=None):
        """ Create a connection pool for a database """

        return pooling.MySQLConnectionPool(
            pool_name=f"pool_{database}",
            pool_size=pool_size,
            host=self.host,
            user=self.user,
            password=self.password,
            database=database
        )
    
    
    def __enumerate_database(self):
        databases = self.get_databases()
        threads = []
        for database in databases:
            self.map[database] = {}

        for database in databases:
            tables = self.get_tables(database)

            for table in tables:
                thread = threading.Thread(target=self.__put_columns, args=(database, table))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

        for thread in threads:
            data = self.database_queue.get()
            self.map[data[0]][data[1]] = data[2]

        self.delta_time = seconds() - self.start_time

    def __put_columns(self, database, table):
        columns = self.get_columns(database, table)
        self.database_queue.put((database, table, columns))
    
    def __delete_json_folder(self):
        try:
            shutil.rmtree(os.path.join(os.getcwd(), "json"))
        except FileNotFoundError:
            pass 


    def get_databases(self):
        connection = self.connection_pool.get_connection()
        cursor = connection.cursor()

        cursor.execute("SHOW DATABASES")
        databases = [database[0] for database in cursor.fetchall()]

        cursor.close()
        connection.close()

        return databases

    def get_tables(self, database):
        connection = self.connection_pool.get_connection()
        cursor = connection.cursor()

        cursor.execute(f"SHOW TABLES FROM {database}")
        tables = [table[0] for table in cursor.fetchall()]

        cursor.close()
        connection.close()

        return tables

    def get_columns(self, database, table):
        connection = self.connection_pool.get_connection()
        cursor = connection.cursor()

        cursor.execute(f"DESCRIBE {database}.{table}")
        columns = [column[0] for column in cursor.fetchall()]

        cursor.close()
        connection.close()

        return columns

    def __load_time(self):
        print("\033[42m    LOAD TIME (s)   \033[0m")
        print(self.delta_time)
    
    def table_to_csv(self, database, table, column_names):
        connection = self.connection_pool.get_connection()
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        table = Table(name=table, database=database, rows=rows, columns=column_names)
        table.export_to_csv()
        cursor.close()
        connection.close()
    
    def add_table_to_queue(self, table):
        self.table_queue.put(table)

    
    def process_table_pool(self, tablepool):
        for i in range(tablepool[1]):
            tablepool[0].convert_to_json()

    def export_database_to_json(self):
        self.start_time = seconds()
        os.makedirs("json", exist_ok=True)
        fcount = 0
        for database in list(self.map.keys()):
            conn_pool = self.new_connection_pool(database=database)
            for table in self.map[database]:
                try:
                    conn = conn_pool.get_connection()
                except:
                    conn_pool = self.new_connection_pool(database=database)
                    conn = conn_pool.get_connection()
                cx = conn.cursor()
                cx.execute(f"SELECT * FROM {table}")
                rows = [x for x in cx.description]
                tab = Table(table, database, rows[0], rows)
                tab.export_to_json()
                print("📃", end="")
                fcount+=1
                if fcount > 100:
                    print()
                    fcount=0
        print(f"\nTotal Export JSON Time:\033[42m {seconds()-self.start_time} \033[0m")
    
    def export_database_to_csv(self):
        self.start_time = seconds()
        os.makedirs("csv", exist_ok=True)
        fcount = 0
        for database in list(self.map.keys()):
            conn_pool = self.new_connection_pool(database=database)
            for table in self.map[database]:
                try:
                    conn = conn_pool.get_connection()
                except:
                    conn_pool = self.new_connection_pool(database=database)
                    conn = conn_pool.get_connection()
                cx = conn.cursor()
                cx.execute(f"SELECT * FROM {table}")
                rows = [x for x in cx.description]
                tab = Table(table, database, rows[0], rows)
                tab.export_to_csv()
                print("📃", end="")
                fcount+=1
                if fcount > 100:
                    print()
                    fcount=0
        print(f"\nTotal CSV Export Time:\033[42m {seconds()-self.start_time} \033[0m")
    
# Usage example
db = ThreadedDatabase(host="127.0.0.1", user="donald", password="YOURPASSWORD")
db.export_database_to_json()
db.export_database_to_csv()

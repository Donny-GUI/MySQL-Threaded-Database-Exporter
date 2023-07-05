# MySQL-Threaded-Database-Exporter
Threaded MySQL database manager written entirely in Python 3. Loads in less than a second and exports entire database in about 4-10.


# Example

```Python

from dbman import ThreadedDatabase

db = ThreadedDatabase(host="127.0.0.1", user="donald", password="YOURPASSWORD")
db.export_database_to_json()
db.export_database_to_csv()

# check the json and csv folders respectively

```

# TODO

- xlsx export
- change table values
- email to - functionality
- try and work multiprocessing in there
- GUI using PyDearGUI

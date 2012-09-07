Usage:

    import sqlutils

    connection_string = "... odbc connection string..."
    connection_string2 = "... odbc connection string..."
    db = sqlutils.DbConnections(mydb=connection_string, otherdb=connection_string2)

    @db.choose("mydb")
    def get_record(record_id):
        query = "SELECT * FROM table WHERE id = ?"
        results = db.query(query, record_id)
        ... do something with results ...

    @db.choose("otherdb")
    def get_other_record(record_id):
        query = "SELECT * FROM other_table WHERE id = ?"
        results = db.query(query, record_id)
        ... do something with results ...

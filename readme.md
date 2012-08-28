Usage:

    import sqlutils

    connection_string = "... odbc connection string..."
    connection_string2 = "... odbc connection string..."
    db = sqlutils.DbConnections(mydb=connection_string, otherdb=connection_string2)

    @db.choose("mydb")
    def get_record(record_id):
        query = "SELECT * FROM table WHERE id = ?"
        results = db.run(query, record_id)
        if len(results) > 1:
            ... do something with results ...
        else:
            ... do something else ...

    @db.choose("otherdb")
    def get_other_record(record_id):
        query = "SELECT * FROM other_table WHERE id = ?"
        results = db.run(query, record_id)
        if len(results) > 1:
            ... do something with results ...
        else:
            ... do something else ...

Usage:

    import sqlutils

    connection_string = "... odbc connection string..."
    supercool_db = sqlutils.DbConnection(connection_string)

    def get_record(record_id, db=supercool_db):
        query = "SELECT * FROM table WHERE id = ?"
        results = db.query(query, record_id)
        ... do something with results ...

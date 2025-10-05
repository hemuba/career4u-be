def retrieve_data(conn, sql:str):
    """generates a cursor for the DB connection and executes a query"""
    if not sql:
        raise ValueError("Insert an Oracle SQL query.")
    with conn.cursor() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
    return data
        
    
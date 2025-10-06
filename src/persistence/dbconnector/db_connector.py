import oracledb as odb


def get_connected(f_user:str=None, f_pw:str=None, f_dsn:str=None):
    """returns the oracledb connection"""
    if not (f_user and f_pw and f_dsn):
        raise ValueError("Missing required parameter to connect: please insert user, password and dsn")
    
    connection = odb.connect(user=f_user, password=f_pw, dsn=f_dsn)
    return connection
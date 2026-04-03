import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="olist_intelligence",
        auth_plugin="mysql_native_password"
    )
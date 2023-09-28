import sqlite3

# Función para establecer una conexión con la base de datos SQLite
def crear_conexion():
    try:
        conn = sqlite3.connect('my_database_globant.db')
        return conn
    except sqlite3.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para cerrar la conexión con la base de datos
def cerrar_conexion(conn):
    if conn:
        conn.close()

# Función para ejecutar consultas SQL
def ejecutar_consulta(conn, consulta):
    try:
        cursor = conn.cursor()
        cursor.execute(consulta)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error al ejecutar la consulta SQL: {e}")

# Puedes incluir otras funciones relacionadas con la base de datos según sea necesario

if __name__ == "__main__":
    # Esta parte se puede utilizar para probar o ejecutar scripts independientes relacionados con operaciones de base de datos
    conexion = crear_conexion()
    if conexion:
        # Ejemplo: Crear una tabla
        consulta_creacion_tabla = '''
        CREATE TABLE IF NOT EXISTS ejemplo (
            id INTEGER PRIMARY KEY,
            nombre TEXT
        )
        '''
        ejecutar_consulta(conexion, consulta_creacion_tabla)
        cerrar_conexion(conexion)

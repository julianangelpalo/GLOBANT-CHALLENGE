import sqlite3
import os
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

# Conectarse a la base de datos SQLite
conn = sqlite3.connect('my_database_globant.db')

# Directorio donde se almacenarán las copias de seguridad AVRO
backup_dir = 'backups'

def restaurar_tabla(nombre_tabla, esquema_avro):
    try:
        cursor = conn.cursor()

        # Verificar si la tabla ya existe y, si es así, eliminarla
        cursor.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")

        # Crear la tabla en función del esquema AVRO
        crear_tabla_sql = f"CREATE TABLE {nombre_tabla} ("
        for campo in esquema_avro['fields']:
            nombre_campo = campo['name']
            tipo_campo = campo['type']
            crear_tabla_sql += f"{nombre_campo} {tipo_campo}, "
        crear_tabla_sql = crear_tabla_sql.rstrip(', ') + ")"
        cursor.execute(crear_tabla_sql)

        # Definir la ruta del archivo AVRO
        archivo_copia = os.path.join(backup_dir, f"{nombre_tabla}.avro")

        # Leer datos del archivo AVRO e insertarlos en la tabla
        with open(archivo_copia, "rb") as archivo_avro:
            lector_avro = DataFileReader(archivo_avro, DatumReader())
            for fila in lector_avro:
                campos = ', '.join(fila.keys())
                marcadores_de_posicion = ', '.join(['?'] * len(fila))
                insertar_sql = f"INSERT INTO {nombre_tabla} ({campos}) VALUES ({marcadores_de_posicion})"
                cursor.execute(insertar_sql, tuple(fila.values()))

        conn.commit()
        return True, f"Tabla {nombre_tabla} restaurada exitosamente desde la copia de seguridad"

    except Exception as e:
        return False, f"Error al restaurar la tabla {nombre_tabla}: {str(e)}"

if __name__ == '__main__':
    # Define el esquema AVRO para la tabla 'departments' como un diccionario de Python
    esquema_avro_departments = {
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "department", "type": ["string", "null"]}
        ]
    }

    # Define el esquema AVRO para la tabla 'jobs' como un diccionario de Python
    esquema_avro_jobs = {
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "job", "type": ["string", "null"]}
        ]
    }

    # Define el esquema AVRO para la tabla 'hired_employees' como un diccionario de Python
    esquema_avro_hired_employees = {
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "name", "type": ["string", "null"]},
            {"name": "datetime", "type": ["string", "null"]},
            {"name": "department_id", "type": ["int", "null"]},
            {"name": "job_id", "type": ["int", "null"]}
        ]
    }

    # Ejemplo: Restaurar una tabla utilizando el esquema AVRO correspondiente
    exito_departments, mensaje_departments = restaurar_tabla('departments', esquema_avro_departments)
    if exito_departments:
        print(mensaje_departments)
    else:
        print(f'Error: {mensaje_departments}')

    # Ejemplo: Restaurar otra tabla
    exito_jobs, mensaje_jobs = restaurar_tabla('jobs', esquema_avro_jobs)
    if exito_jobs:
        print(mensaje_jobs)
    else:
        print(f'Error: {mensaje_jobs}')

    # Ejemplo: Restaurar la tabla 'hired_employees'
    exito_hired_employees, mensaje_hired_employees = restaurar_tabla('hired_employees', esquema_avro_hired_employees)
    if exito_hired_employees:
        print(mensaje_hired_employees)
    else:
        print(f'Error: {mensaje_hired_employees}')

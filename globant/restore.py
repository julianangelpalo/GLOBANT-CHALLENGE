import sqlite3
import os
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

# Connect to your SQLite database
conn = sqlite3.connect('my_database_globant.db')

# Directory where AVRO backups will be stored
backup_dir = 'backups'

def restaurar_tabla(nombre_tabla, esquema_avro):
    try:
        cursor = conn.cursor()

        # Verify if the table already exists and if so, drop it
        cursor.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")

        # Create the table based on the AVRO schema
        crear_tabla_sql = f"CREATE TABLE {nombre_tabla} ("
        for campo in esquema_avro['fields']:
            nombre_campo = campo['name']
            tipo_campo = campo['type']
            crear_tabla_sql += f"{nombre_campo} {tipo_campo}, "
        crear_tabla_sql = crear_tabla_sql.rstrip(', ') + ")"
        cursor.execute(crear_tabla_sql)

        # Define the AVRO file path
        archivo_copia = os.path.join(backup_dir, f"{nombre_tabla}.avro")

        # Read data from the AVRO file and insert it into the table
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
    # Define the AVRO schema for the 'departments' table as a Python dictionary
    esquema_avro_departments = {
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "department", "type": ["string", "null"]}
        ]
    }

    # Define the AVRO schema for the 'jobs' table as a Python dictionary
    esquema_avro_jobs = {
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "job", "type": ["string", "null"]}
        ]
    }

    # Define the AVRO schema for the 'hired_employees' table as a Python dictionary
    esquema_avro_hired_employees = {
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "name", "type": ["string", "null"]},
            {"name": "datetime", "type": ["string", "null"]},
            {"name": "department_id", "type": ["int", "null"]},
            {"name": "job_id", "type": ["int", "null"]}
        ]
    }

    # Example: Restore a table using the appropriate AVRO schema
    success_departments, message_departments = restaurar_tabla('departments', esquema_avro_departments)
    if success_departments:
        print(message_departments)
    else:
        print(f'Error: {message_departments}')

    # Example: Restore another table
    success_jobs, message_jobs = restaurar_tabla('jobs', esquema_avro_jobs)
    if success_jobs:
        print(message_jobs)
    else:
        print(f'Error: {message_jobs}')

    # Example: Restore the 'hired_employees' table
    success_hired_employees, message_hired_employees = restaurar_tabla('hired_employees', esquema_avro_hired_employees)
    if success_hired_employees:
        print(message_hired_employees)
    else:
        print(f'Error: {message_hired_employees}')

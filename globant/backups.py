import sqlite3
import os
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Conectarse a la base de datos SQLite
conn = sqlite3.connect('my_database_globant.db')

# Directorio donde se almacenarán las copias de seguridad AVRO
backup_dir = 'backups'

def crear_copia_seguridad(nombre_tabla, avro_esquema):
    try:
        # Crear el directorio de copias de seguridad si no existe
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {nombre_tabla}")

        # Obtener todas las filas de la tabla
        filas = cursor.fetchall()

        # Definir la ruta del archivo AVRO
        archivo_copia = os.path.join(backup_dir, f"{nombre_tabla}.avro")

        # Escribir los datos en el archivo AVRO
        with open(archivo_copia, "wb") as avro_file:
            avro_writer = DataFileWriter(avro_file, DatumWriter(), avro_esquema)
            for fila in filas:
                avro_writer.append({f"{col[0]}": fila[i] for i, col in enumerate(cursor.description)})
            avro_writer.close()

        return True, f"Copia de seguridad de {nombre_tabla} creada con éxito: {archivo_copia}"

    except Exception as e:
        return False, f"Error al crear la copia de seguridad de {nombre_tabla}: {str(e)}"

if __name__ == '__main__':
    # Definir el esquema AVRO para la tabla 'departments'
    esquema_avro_departments = avro.schema.parse('''
    {
        "type": "record",
        "name": "departments",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "department", "type": ["string", "null"]}
        ]
    }
    ''')

    # Ejemplo: Crear una copia de seguridad de la tabla 'departments'
    exito_departments, mensaje_departments = crear_copia_seguridad('departments', esquema_avro_departments)
    if exito_departments:
        print(mensaje_departments)
    else:
        print(f'Error: {mensaje_departments}')

    # Definir el esquema AVRO para la tabla 'jobs'
    esquema_avro_jobs = avro.schema.parse('''
    {
        "type": "record",
        "name": "jobs",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "job", "type": ["string", "null"]}
        ]
    }
    ''')

    # Ejemplo: Crear una copia de seguridad de la tabla 'jobs'
    exito_jobs, mensaje_jobs = crear_copia_seguridad('jobs', esquema_avro_jobs)
    if exito_jobs:
        print(mensaje_jobs)
    else:
        print(f'Error: {mensaje_jobs}')

    # Definir el esquema AVRO para la tabla 'hired_employees'
    esquema_avro_hired_employees = avro.schema.parse('''
    {
        "type": "record",
        "name": "hired_employees",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "name", "type": ["string", "null"]},
            {"name": "datetime", "type": ["string", "null"]},
            {"name": "department_id", "type": ["int", "null"]},
            {"name": "job_id", "type": ["int", "null"]}
        ]
    }
    ''')

    # Ejemplo: Crear una copia de seguridad de la tabla 'hired_employees'
    exito_hired_employees, mensaje_hired_employees = crear_copia_seguridad('hired_employees', esquema_avro_hired_employees)
    if exito_hired_employees:
        print(mensaje_hired_employees)
    else:
        print(f'Error: {mensaje_hired_employees}')

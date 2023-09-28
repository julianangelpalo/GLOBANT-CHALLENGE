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
        return True, f

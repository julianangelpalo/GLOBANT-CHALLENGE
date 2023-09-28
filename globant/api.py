from flask import Flask, request, jsonify, g
import sqlite3  # Suponiendo una base de datos SQLite

app = Flask(__name__, static_folder='static')

# Función para obtener la conexión a la base de datos (crea una nueva conexión si aún no está en contexto)
def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect('my_database_globant.db')
    return g.db

# Función para cerrar la conexión a la base de datos al final de cada solicitud
@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()

# Reglas del diccionario de datos
data_rules = {
    'hired_employees': {
        'id': int,
        'name': str,
        'datetime': str,
        'department_id': int,
        'job_id': int
    },
    'departments': {
        'id': int,
        'department': str
    },
    'jobs': {
        'id': int,
        'job': str
    }
}

@app.route('/insert/hired_employees', methods=['POST'])
def insert_data(table_name):
    try:
        data = request.json  # Datos JSON de la solicitud
        
        # Implementar la validación de datos en función de las reglas del diccionario de datos para 'table_name'.
        if not is_valid_data(data, table_name):
            return jsonify({"error": "Formato de datos no válido"}), 400

        # Implementar la lógica de inserción por lotes aquí.
        if len(data) < 1 or len(data) > 1000:
            return jsonify({"error": "Tamaño de lote no válido"}), 400

        # Insertar datos en la tabla especificada en la base de datos.
        conn = get_db()
        cursor = conn.cursor()
        cursor.executemany(f"INSERT INTO {table_name} (id, name, datetime, department_id, job_id) VALUES (:id, :name, :datetime, :department_id, :job_id)", data)
        conn.commit()

        return jsonify({"message": "Datos insertados exitosamente"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 400

def is_valid_data(data, table_name):
    # Comprobar si la lista 'data' contiene diccionarios con claves y tipos de datos correctos.
    if table_name in data_rules:
        expected_fields = data_rules[table_name]
        for row in data:
            if not isinstance(row, dict):
                return False
            for key, expected_type in expected_fields.items():
                if key not in row or not isinstance(row[key], expected_type):
                    return False
        return True
    return False

if __name__ == '__main__':
    app.run(port=8899)

from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

# Define una ruta para la métrica
@app.route('/metricas/contratacion_empleados', methods=['GET'])
def metricas_contratacion_empleados():
    try:
        # Conéctate a la base de datos SQLite
        conn = sqlite3.connect('my_database_globant.db')
        cursor = conn.cursor()

        # Ejecuta tu consulta SQL para calcular la métrica
        query = """
        -- Tu consulta SQL aquí --
        """

        cursor.execute(query)

        # Obtiene los resultados como una lista de diccionarios
        results = []
        for row in cursor.fetchall():
            # Procesa los resultados aquí
            results.append({
                # Define las claves y valores aquí
            })

        return jsonify({"resultados": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

if __name__ == '__main__':
    app.run(port=8995)

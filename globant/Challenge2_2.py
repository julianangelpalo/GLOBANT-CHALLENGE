from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

# Define a route for the new metric
@app.route('/metrics/department_employees', methods=['GET'])
def department_employees_metrics():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('my_database_globant.db')
        cursor = conn.cursor()

        # Execute your SQL query
        query = """
        SELECT
            d.id AS department_id,
            d.department AS department_name,
            COUNT(he.id) AS num_employees_hired
        FROM
            departments d
        INNER JOIN
            hired_employees he ON d.id = he.department_id
        WHERE
            strftime('%Y', he.datetime) = '2021'
        GROUP BY
            d.id,
            d.department
        HAVING
            COUNT(he.id) > (SELECT AVG(num_hired) FROM (SELECT COUNT(id) AS num_hired FROM hired_employees WHERE strftime('%Y', datetime) = '2021' GROUP BY department_id))
        ORDER BY
            num_employees_hired DESC;
        """

        cursor.execute(query)

        # Fetch the results as a list of dictionaries
        results = []
        for row in cursor.fetchall():
            department_id, department_name, num_employees_hired = row
            results.append({
                "Department ID": department_id,
                "Department Name": department_name,
                "Number of Employees Hired": num_employees_hired
            })

        return jsonify({"results": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

if __name__ == '__main__':
    app.run(port=8995)

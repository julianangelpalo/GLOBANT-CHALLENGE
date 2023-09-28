from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

@app.route('/metrics/employee_hiring', methods=['GET'])
def employee_hiring_metrics():
    try:

        conn = sqlite3.connect('my_database_globant.db')
        cursor = conn.cursor()


        query = """
        SELECT
            d.department,
            j.job,
            SUM(CASE WHEN strftime('%m', he.datetime) BETWEEN '01' AND '03' THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN strftime('%m', he.datetime) BETWEEN '04' AND '06' THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN strftime('%m', he.datetime) BETWEEN '07' AND '09' THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN strftime('%m', he.datetime) BETWEEN '10' AND '12' THEN 1 ELSE 0 END) AS Q4
        FROM
            departments d
        INNER JOIN
            hired_employees he ON d.id = he.department_id
        INNER JOIN
            jobs j ON he.job_id = j.id
        WHERE
            strftime('%Y', he.datetime) = '2021'
        GROUP BY
            d.department,
            j.job
        ORDER BY
            d.department,
            j.job;
        """

        cursor.execute(query)

        results = []
        for row in cursor.fetchall():
            department, job, q1, q2, q3, q4 = row
            results.append({
                "Department": department,
                "Job": job,
                "Q1": q1,
                "Q2": q2,
                "Q3": q3,
                "Q4": q4
            })

        return jsonify({"results": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

if __name__ == '__main__':
    app.run(port=8995)

import sqlite3
import os
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Connect to your SQLite database
conn = sqlite3.connect('my_database_globant.db')

# Directory where AVRO backups will be stored
backup_dir = 'backups'

def create_backup(table_name, avro_schema):
    try:
        # Create the backup directory if it doesn't exist
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")

        # Fetch all rows from the table
        rows = cursor.fetchall()

        # Define the AVRO file path
        backup_file = os.path.join(backup_dir, f"{table_name}.avro")

        # Write the data to the AVRO file
        with open(backup_file, "wb") as avro_file:
            avro_writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)
            for row in rows:
                avro_writer.append({f"{col[0]}": row[i] for i, col in enumerate(cursor.description)})
            avro_writer.close()

        return True, f"Backup of {table_name} created successfully: {backup_file}"

    except Exception as e:
        return False, f"Error creating backup of {table_name}: {str(e)}"

if __name__ == '__main__':
    # Define the AVRO schema for the 'departments' table
    departments_avro_schema = avro.schema.parse('''
    {
        "type": "record",
        "name": "departments",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "department", "type": ["string", "null"]}
        ]
    }
    ''')

    # Example: Create a backup of the 'departments' table
    success_departments, message_departments = create_backup('departments', departments_avro_schema)
    if success_departments:
        print(message_departments)
    else:
        print(f'Error: {message_departments}')

    # Define the AVRO schema for the 'jobs' table
    jobs_avro_schema = avro.schema.parse('''
    {
        "type": "record",
        "name": "jobs",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "job", "type": ["string", "null"]}
        ]
    }
    ''')

    # Example: Create a backup of the 'jobs' table
    success_jobs, message_jobs = create_backup('jobs', jobs_avro_schema)
    if success_jobs:
        print(message_jobs)
    else:
        print(f'Error: {message_jobs}')

    # Define the AVRO schema for the 'hired_employees' table
    hired_employees_avro_schema = avro.schema.parse('''
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

    # Example: Create a backup of the 'hired_employees' table
    success_hired_employees, message_hired_employees = create_backup('hired_employees', hired_employees_avro_schema)
    if success_hired_employees:
        print(message_hired_employees)
    else:
        print(f'Error: {message_hired_employees}')

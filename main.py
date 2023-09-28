import data_import
import backups
import restore
import api
import time
import avro.schema
import json  # Import the json module

if __name__ == "__main__":
    # Point 1: Create tables and import data from XLSX files to the database
    data_import.import_data()
    data_import.create_tables()  
    print("Tables created and data imported successfully.")

    # Define AVRO schemas for each table here

    # Define the AVRO schema for the 'departments' table
    departments_avro_schema_json = {
        "type": "record",
        "name": "departments",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "department", "type": ["string", "null"]}
        ]
    }
    
    # Define the AVRO schema for the 'jobs' table
    jobs_avro_schema_json = {
        "type": "record",
        "name": "jobs",
        "fields": [
            {"name": "id", "type": ["int", "null"]},
            {"name": "job", "type": ["string", "null"]}
        ]
    }
    
    # Define the AVRO schema for the 'hired_employees' table
    hired_employees_avro_schema_json = {
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

    # Convert JSON schema data to AVRO schema objects
    departments_avro_schema = avro.schema.Parse(json.dumps(departments_avro_schema_json))
    jobs_avro_schema = avro.schema.Parse(json.dumps(jobs_avro_schema_json))
    hired_employees_avro_schema = avro.schema.Parse(json.dumps(hired_employees_avro_schema_json))

    # Point 3: Create backups in AVRO format
    success_departments, message_departments = backups.create_backup('departments', departments_avro_schema)
    success_jobs, message_jobs = backups.create_backup('jobs', jobs_avro_schema)
    success_hired_employees, message_hired_employees = backups.create_backup('hired_employees', hired_employees_avro_schema)

    if success_departments:
        print(message_departments)
    else:
        print(f'Error: {message_departments}')

    if success_jobs:
        print(message_jobs)
    else:
        print(f'Error: {message_jobs}')

    if success_hired_employees:
        print(message_hired_employees)
    else:
        print(f'Error: {message_hired_employees}')

    # Point 4: Restore tables from backups
    # Restore each table from its respective AVRO backup
    success_restore_departments, message_restore_departments = restore.restaurar_tabla('departments', departments_avro_schema)
    success_restore_jobs, message_restore_jobs = restore.restaurar_tabla('jobs', jobs_avro_schema)
    success_restore_hired_employees, message_restore_hired_employees = restore.restaurar_tabla('hired_employees', hired_employees_avro_schema)

    if success_restore_departments:
        print(message_restore_departments)
    else:
        print(f'Error: {message_restore_departments}')

    if success_restore_jobs:
        print(message_restore_jobs)
    else:
        print(f'Error: {message_restore_jobs}')

    if success_restore_hired_employees:
        print(message_restore_hired_employees)
    else:
        print(f'Error: {message_restore_hired_employees}')

    # Sleep for a while to ensure that the API has time to start
    time.sleep(5)

    # Point 2: Start the REST API
    api.app.run(port=8995)

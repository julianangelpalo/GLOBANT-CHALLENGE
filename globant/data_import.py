import pandas as pd
import sqlite3  # Suponiendo una base de datos SQLite

# Definir las rutas de archivo para tus archivos XLSX
hired_employees_file = '/Users/julianangel/Downloads/hired_employees.xlsx'
departments_file = '/Users/julianangel/Downloads/departments.xlsx'
jobs_file = '/Users/julianangel/Downloads/jobs.xlsx'

# Conexión a la base de datos
conn = sqlite3.connect('my_database_globant.db')

# Función para crear tablas en la base de datos
def create_tables():
    cursor = conn.cursor()
    
    # Crear la tabla hired_employees si no existe
    cursor.execute('''CREATE TABLE IF NOT EXISTS hired_employees (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        datetime TEXT,
                        department_id INTEGER,
                        job_id INTEGER
                    )''')
    
    # Crear la tabla departments si no existe
    cursor.execute('''CREATE TABLE IF NOT EXISTS departments (
                        id INTEGER PRIMARY KEY,
                        department TEXT
                    )''')
    
    # Crear la tabla jobs si no existe
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY,
                        job TEXT
                    )''')
    
    conn.commit()

# Función para importar datos desde archivos XLSX a la base de datos
def import_data():
    # Crear tablas si no existen
    create_tables()
    
    # Leer datos desde archivos XLSX en DataFrames de Pandas
    hired_employees = pd.read_excel(hired_employees_file, header=None)  # No header in XLSX
    departments = pd.read_excel(departments_file, header=None)  # No header in XLSX
    jobs = pd.read_excel(jobs_file, header=None)  # No header in XLSX
    
    # Agregar nombres de columnas a los DataFrames
    hired_employees.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
    departments.columns = ['id', 'department']
    jobs.columns = ['id', 'job']
    
    hired_employees['department_id'].fillna(0, inplace=True)
    hired_employees['job_id'].fillna(0, inplace=True)

    # Convertir las columnas 'department_id' y 'job_id' a enteros
    hired_employees['id'] = hired_employees['id'].astype(int)
    hired_employees['department_id'] = hired_employees['department_id'].astype(int)
    hired_employees['job_id'] = hired_employees['job_id'].astype(int)

    # Guardar los DataFrames en la base de datos
    hired_employees.to_sql('hired_employees', conn, if_exists='replace', index=False)
    departments.to_sql('departments', conn, if_exists='replace', index=False)
    jobs.to_sql('jobs', conn, if_exists='replace', index=False)
    
if __name__ == '__main__':
    import_data()

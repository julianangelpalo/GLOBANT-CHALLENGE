import subprocess

# Define the order in which to run the scripts
scripts_to_run = [
    "globant/db_config.py",
    "globant/data_import.py",
    "globant/backups.py",
    "globant/restore.py",
    "globant/api.py",
    "globant/challenge2_1.py",
    "globant/challenge2_2.py"
]

# Loop through the scripts and run them one by one
for script in scripts_to_run:
    try:
        subprocess.run(["python", script])
    except Exception as e:
        print(f"Error running {script}: {str(e)}")

print("Orchestration complete.")

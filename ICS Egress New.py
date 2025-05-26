import streamlit as st

import re

import json

from pathlib import Path

from datetime import datetime, timedelta



# ---------- Setup ----------

st.set_page_config(page_title="ICS Egress Framework", layout="wide")

st.title("ICS Egress Framework")



# ---------- Login ----------

if "authenticated" not in st.session_state:

    st.session_state.authenticated = False



if not st.session_state.authenticated:

    st.subheader("Login")

    password = st.text_input("Enter Password", type="password")

    if st.button("Submit"):

        if password == "icsegf2025":

            st.session_state.authenticated = True

        else:

            st.error("Incorrect password.")

    st.stop()



# ---------- Tabs ----------

tabs = st.tabs([

    "Code Preview",

    "SQL Config",

    "Generated SQL",

    "Execution JSON",

    "Generated Export Job",

    "Generated DAG"

])



# ---------- Helper Functions ----------

def parse_sql_and_non_sql(script_text):

    sql_blocks = re.findall(r'(SELECT\s+.*?;)', script_text, re.IGNORECASE | re.DOTALL)

    non_sql = re.sub(r'(SELECT\s+.*?;)', '', script_text, flags=re.IGNORECASE | re.DOTALL).strip()



    source_tables = re.findall(r'FROM\s+([a-zA-Z0-9_.]+)', ' '.join(sql_blocks), re.IGNORECASE)

    columns = re.findall(r'SELECT\s+(.*?)\s+FROM', ' '.join(sql_blocks), re.IGNORECASE | re.DOTALL)

    columns_flat = [col.strip() for sub in columns for col in sub.split(',')]



    target = re.search(r'EXPORT\s+FILE\s*=\s*[\'"]([^\'"]+)[\'"]', script_text, re.IGNORECASE)

    destination = target.group(1) if target else "/tmp/output.csv"



    sql_config = {

        "source_schema": [{"table": tbl, "columns": columns_flat} for tbl in source_tables],

        "target_schema": [{"destination": destination}],

        "sql_logic": ' '.join(sql_blocks).strip()

    }



    execution_json = {

        "job_name": "parsed_egress_job",

        "execution_condition": [line.strip() for line in non_sql.splitlines() if "if" in line.lower() or ".quit" in line.lower()],

        "command_logic": "bteq < job.bteq",

        "schedule": "0 3 * * *",

        "retries": 1,

        "delay_minutes": 5

    }



    return sql_config, execution_json



def generate_export_script(sql_logic, destination):

    return f"""

from google.cloud import bigquery

import pandas as pd



client = bigquery.Client()

query = \"\"\"{sql_logic}\"\"\"

df = client.query(query).to_dataframe()

df.to_csv("{destination}", index=False)

print("Export complete: {destination}")

""".strip()



def generate_dag_code(job_name, export_script, exec_json):

    return f"""

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.python import ShortCircuitOperator

from datetime import datetime, timedelta

import os

import subprocess



def check_export_exists():

    return os.path.exists("{export_script}")



def execute_export_job():

    subprocess.run("python {export_script}", shell=True, check=True)



default_args = {{

    'owner': 'airflow',

    'retries': {exec_json['retries']},

    'retry_delay': timedelta(minutes={exec_json['delay_minutes']})

}}



with DAG(

    dag_id="{job_name}_dag",

    default_args=default_args,

    schedule_interval="{exec_json['schedule']}",

    start_date=datetime(2024, 1, 1),

    catchup=False

) as dag:



    check_file = ShortCircuitOperator(

        task_id="check_export_script",

        python_callable=check_export_exists

    )



    run_export = PythonOperator(

        task_id="run_export_job",

        python_callable=execute_export_job

    )



    check_file >> run_export

""".strip()



# ---------- Tab 1: Code Upload ----------

with tabs[0]:

    st.subheader("Code Preview")

    uploaded_file = st.file_uploader("Upload an egress script", type=["bteq", "sql", "txt", "py", "sh", "java", "cs"])

    if uploaded_file:

        code_text = uploaded_file.read().decode("utf-8")

        st.session_state.code_text = code_text

        st.text_area("Raw Code", code_text, height=300)



# ---------- Process Once Uploaded ----------

if "code_text" in st.session_state:

    code = st.session_state.code_text

    sql_config, exec_json = parse_sql_and_non_sql(code)

    job_name = Path(uploaded_file.name).stem

    st.session_state.job_name = job_name

    st.session_state.sql_config = sql_config

    st.session_state.exec_json = exec_json



    # ---------- Tab 2: SQL Config ----------

    with tabs[1]:

        st.subheader("SQL Config JSON")

        st.json(sql_config)

        st.download_button("Download SQL Config", json.dumps(sql_config, indent=2), file_name="sql_config.json")



    # ---------- Tab 3: Generated SQL ----------

    with tabs[2]:

        st.subheader("SQL Logic")

        st.code(sql_config["sql_logic"], language="sql")

        st.download_button("Download SQL", sql_config["sql_logic"], file_name="egress_query.sql")



    # ---------- Tab 4: Execution JSON ----------

    with tabs[3]:

        st.subheader("Execution JSON")

        st.json(exec_json)

        st.download_button("Download Execution JSON", json.dumps(exec_json, indent=2), file_name="execution.json")



    # ---------- Tab 5: Export Script ----------

    with tabs[4]:

        st.subheader("Generated Export Python Script")

        export_path = sql_config["target_schema"][0]["destination"]

        export_script = generate_export_script(sql_config["sql_logic"], export_path)

        st.code(export_script, language="python")

        st.download_button("Download Export Script", export_script, file_name=f"{job_name}_export.py")



    # ---------- Tab 6: DAG ----------

    with tabs[5]:

        st.subheader("Generated Airflow DAG")

        dag_code = generate_dag_code(job_name, f"{job_name}_export.py", exec_json)

        st.code(dag_code, language="python")

        st.download_button("Download DAG", dag_code, file_name=f"{job_name}_dag.py")
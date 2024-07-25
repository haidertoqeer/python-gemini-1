from dotenv import load_dotenv
load_dotenv()  # Load environment variables

import streamlit as st
import os
import sqlite3
import pandas as pd
import google.generativeai as genai

# Configure our API Key
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Function to Load Google Gemini Model and provide SQL query as response
def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text.strip()

# Function to retrieve query from the SQL database
def read_sql_query(sql, db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    try:
        print(f"Executing SQL query: {sql}")  # Print SQL query for debugging
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [description[0] for description in cur.description]  # Get column names
    except sqlite3.OperationalError as e:
        st.error(f"Error executing query: {e}")
        return [], []
    conn.close()
    return rows, columns

# Function to create a database from a DataFrame
def create_database_from_df(df, db_name):
    conn = sqlite3.connect(db_name)
    df.to_sql('uploaded_data', conn, if_exists='replace', index=False)
    conn.close()

# Function to get table names from the database
def get_table_names(db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    conn.close()
    return [table[0] for table in tables]

# Function to get column names from the database table
def get_column_names(table_name, db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cur.fetchall()]
    conn.close()
    return columns

# Function to generate prompt based on table name and columns
def generate_prompt(table_name, columns):
    columns_str = ", ".join(columns)
    return [
        f"""
You are an expert in converting English question to SQL query!
The SQL database has the table '{table_name}' with the following columns: {columns_str}. For example, \nExample 1 - How many entries of records are present in the table? The SQL command will be something like this SELECT COUNT(*) FROM {table_name}; \nExample 2 - Tell me all the records where the column 'CLASS' has the value 'Data Science', the SQL command will be something like this SELECT * FROM {table_name} WHERE CLASS='Data Science';
also the SQL code should not have ``` in the beginning or end and SQL word in output.
"""
    ]

# Function to list all .db files in the root directory
def list_databases(root_dir):
    return {
        os.path.splitext(file)[0]: os.path.join(root_dir, file)
        for file in os.listdir(root_dir) if file.endswith(".db")
    }

# Streamlit APP
st.set_page_config(page_title="I can Retrieve Any SQL query")
st.header("Gemini App to Retrieve SQL Data")

# List available databases dynamically
available_databases = list_databases(".")

uploaded_file = st.file_uploader("Upload an XLSX or CSV file", type=["xlsx", "csv"])

if uploaded_file:
    if uploaded_file.name.endswith(".xlsx"):
        df = pd.read_excel(uploaded_file)
    elif uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    
    uploaded_db_name = uploaded_file.name.split(".")[0] + ".db"
    create_database_from_df(df, uploaded_db_name)
    available_databases[uploaded_file.name.split(".")[0]] = uploaded_db_name
    st.success(f"Database '{uploaded_file.name.split('.')[0]}' created successfully!")

# Select database
selected_db_name = st.selectbox("Select Database", list(available_databases.keys()))
selected_db = available_databases[selected_db_name]

# Get table names from the selected database
table_names = get_table_names(selected_db)
selected_table = st.selectbox("Select Table", table_names)

# Get column names for the selected table
columns = get_column_names(selected_table, selected_db)

question = st.text_input("Your Question:", key="input")

submit = st.button("Submit question")

if submit:
    with st.spinner("Generating SQL query and retrieving data..."):
        # Generate prompt and get SQL query
        prompt = generate_prompt(selected_table, columns)
        response = get_gemini_response(question, prompt)
        
        # Print the generated SQL query for debugging
        print(f"Generated SQL query: {response}")
        
        # Retrieve data from the SQL database
        data, columns = read_sql_query(response, selected_db)

    st.subheader("The Response is:")
    if data:
        df = pd.DataFrame(data, columns=columns)
        st.table(df)
    else:
        st.write("No data found for the query.")

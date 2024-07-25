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

def generate_prompt(table_name, columns):
    columns_str = ", ".join(columns)
    return [
        f"""
You are an expert in converting English questions to SQL queries!
The SQL database has a table named '{table_name}' with the following columns: {columns_str}.
Here are some examples of how to convert English questions to SQL queries:

Example 1 - How many entries of records are present in the table?
SQL Query: SELECT COUNT(*) FROM {table_name};

Example 2 - Tell me all the records where the column '<COLUMN_NAME>' has the value '<VALUE>'.
SQL Query: SELECT * FROM {table_name} WHERE <COLUMN_NAME> = '<VALUE>';

Example 3 - What is the average value of the column '<COLUMN_NAME>'?
SQL Query: SELECT AVG(<COLUMN_NAME>) FROM {table_name};

Example 4 - List all records where the column '<COLUMN_NAME1>' is '<VALUE1>' and the column '<COLUMN_NAME2>' is greater than <VALUE2>.
SQL Query: SELECT * FROM {table_name} WHERE <COLUMN_NAME1> = '<VALUE1>' AND <COLUMN_NAME2> > <VALUE2>;

Example 5 - Get the sum of the column '<COLUMN_NAME>' for records where '<COLUMN_NAME2>' is '<VALUE>'.
SQL Query: SELECT SUM(<COLUMN_NAME>) FROM {table_name} WHERE <COLUMN_NAME2> = '<VALUE>';

Example 6 - Retrieve the top <N> records with the highest values in the column '<COLUMN_NAME>'.
SQL Query: SELECT * FROM {table_name} ORDER BY <COLUMN_NAME> DESC LIMIT <N>;

Example 7 - Find the maximum value in the column '<COLUMN_NAME>'.
SQL Query: SELECT MAX(<COLUMN_NAME>) FROM {table_name};

Example 8 - Show the count of records grouped by the column '<COLUMN_NAME>'.
SQL Query: SELECT <COLUMN_NAME>, COUNT(*) FROM {table_name} GROUP BY <COLUMN_NAME>;

Example 9 - Display records where the column '<COLUMN_NAME>' is between '<VALUE1>' and '<VALUE2>'.
SQL Query: SELECT * FROM {table_name} WHERE <COLUMN_NAME> BETWEEN '<VALUE1>' AND '<VALUE2>';

Example 10 - Fetch all distinct values in the column '<COLUMN_NAME>'.
SQL Query: SELECT DISTINCT <COLUMN_NAME> FROM {table_name};

Example 11 - List records where the '<COLUMN_NAME1>' is greater than <VALUE1>, the '<COLUMN_NAME2>' is '<VALUE2>', and order by '<COLUMN_NAME3>' in descending order.
SQL Query: SELECT * FROM {table_name} WHERE <COLUMN_NAME1> > <VALUE1> AND <COLUMN_NAME2> = '<VALUE2>' ORDER BY <COLUMN_NAME3> DESC;

Example 12 - Calculate the average '<COLUMN_NAME1>' and the total '<COLUMN_NAME2>' for each '<COLUMN_NAME3>' where '<COLUMN_NAME4>' is '<VALUE>'.
SQL Query: SELECT <COLUMN_NAME3>, AVG(<COLUMN_NAME1>) AS Avg<COLUMN_NAME1>, SUM(<COLUMN_NAME2>) AS Total<COLUMN_NAME2> FROM {table_name} WHERE <COLUMN_NAME4> = '<VALUE>' GROUP BY <COLUMN_NAME3>;

Example 13 - Retrieve records where '<COLUMN_NAME1>' is between <VALUE1> and <VALUE2>, '<COLUMN_NAME2>' is '<VALUE3>', and '<COLUMN_NAME3>' is either '<VALUE4>' or '<VALUE5>'.
SQL Query: SELECT * FROM {table_name} WHERE <COLUMN_NAME1> BETWEEN <VALUE1> AND <VALUE2> AND <COLUMN_NAME2> = '<VALUE3>' AND <COLUMN_NAME3> IN ('<VALUE4>', '<VALUE5>');

Example 14 - Show the top <N> '<COLUMN_NAME1>' with the highest average '<COLUMN_NAME2>', including the count of records in each '<COLUMN_NAME1>'.
SQL Query: SELECT <COLUMN_NAME1>, AVG(<COLUMN_NAME2>) AS Avg<COLUMN_NAME2>, COUNT(*) AS RecordCount FROM {table_name} GROUP BY <COLUMN_NAME1> ORDER BY Avg<COLUMN_NAME2> DESC LIMIT <N>;

Example 15 - Find all records where the '<COLUMN_NAME1>' is greater than the average '<COLUMN_NAME1>' and the '<COLUMN_NAME2>' is less than <VALUE>.
SQL Query: SELECT * FROM {table_name} WHERE <COLUMN_NAME1> > (SELECT AVG(<COLUMN_NAME1>) FROM {table_name}) AND <COLUMN_NAME2> < <VALUE>;

The SQL code should not have ``` in the beginning or end and should not include the word 'SQL' in the output. Ensure that the generated SQL query is accurate and matches the requested parameters precisely.
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


import streamlit as st
import boto3
import pandas as pd
import io
import pathlib
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import snowflake.connector
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector import DictCursor

# Initialize session state
if 'dataframe' not in st.session_state:
    st.session_state.dataframe = None

if 'selectedfile' not in st.session_state:
    st.session_state.selectedfile = None

if 'snowflake_conn' not in st.session_state:
    st.session_state.snowflake_conn = None

# Check if the session state variables are not already defined
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = None
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None

 # Assuming you have configured your AWS credentials using AWS CLI or environment variables
s3 = boto3.resource('s3')   

# Connect to Snowflake
def connect_to_snowflake(username, password, account):
    try:
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
        )
        st.session_state.snowflake_conn = conn
        # st.session_state.snowflake_username = username  # Store username in session state
        # st.session_state.snowflake_password = password  # Store password in session state
        # st.session_state.snowflake_account = account 
        st.success("Successfully connected to Snowflake!")
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")

# Get Snowflake connection or prompt for credentials
def get_snowflake_connection():
    if st.session_state.snowflake_conn is None:
        username = st.text_input("Username", key="username_input")
        password = st.text_input("Password", type="password", key="password_input")
        account = st.text_input("Account", key="account_input")
        if st.button("Connect to Snowflake"):
            connect_to_snowflake(username, password, account)
    return st.session_state.snowflake_conn

def get_column_definitions(df):
    # Map Pandas data types to Snowflake data types
    snowflake_data_types = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'STRING',
        'datetime64': 'TIMESTAMP',
    }

    # Get column definitions with Snowflake data types
    column_definitions = []
    for col in df.columns:
        snowflake_type = snowflake_data_types.get(str(df[col].dtype), 'STRING')  # Default to STRING type
        column_definitions.append(f'"{col}" {snowflake_type}')

    return column_definitions

# Function to create a table in Snowflake if it doesn't exist
def create_table_if_not_exists(conn, table_name, df):
    try:
        cur = conn.cursor()

        # Get column definitions from DataFrame
        column_definitions = get_column_definitions(df)
        column_defs_str = ', '.join(column_definitions)

        # Create or replace table with column definitions
        cur.execute(f'CREATE OR REPLACE TABLE "{table_name}" ({column_defs_str})')

        st.success(f"Table {table_name} created successfully!")
    except Exception as e:
        st.error(f"Failed to create table {table_name}: {str(e)}")

def trigger_snowpipe(table_name, selected_database, selected_schema, file_path):
        try:
            conn = get_snowflake_connection()
            if conn:
             cur = conn.cursor()
             cur.execute(f"USE DATABASE {selected_database}")  # Use the selected database
             cur.execute(f"USE SCHEMA {selected_schema}")  # Use the selected schema

            #  # Check if the target table exists in Snowflake
             cur.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{selected_schema}' AND TABLE_NAME = '{table_name}'")
             table_exists = cur.fetchone()
           
             if not table_exists:
                df = st.session_state.dataframe
                if df is not None:
                       # Create the table with schema based on DataFrame columns (assuming all strings)
                    # column_definitions = get_column_definitions(df)
                    create_table_if_not_exists(conn, table_name, df)
                else:
                    st.error("Dataframe is empty")
             else:
                 st.error("Table already exists")
                           
                        # Copy data from S3 stage to Snowflake table using Snowpipe         
             snowpipe_command = f"""COPY INTO {selected_database}.{selected_schema}.{table_name} FROM @S3_STAGE2/{file_path}
                   FILE_FORMAT = (TYPE='CSV', 
                                  SKIP_HEADER=1, 
                                  FIELD_DELIMITER=',', 
                                  TRIM_SPACE=FALSE, 
                                  REPLACE_INVALID_CHARACTERS=TRUE, 
                                  DATE_FORMAT=AUTO, 
                                  TIME_FORMAT=AUTO,   
                                  TIMESTAMP_FORMAT=AUTO) 
                  ON_ERROR = CONTINUE;"""            
             cur.execute(snowpipe_command)
            st.success(f"Data loaded from S3 to Snowflake table {table_name} using Snowpipe.")
        except Exception as e:
            st.error(f"Failed to load data using Snowpipe: {str(e)}")
        # finally:
        #     if conn:
        #         conn.close()


def get_snowflake_cursor():
    if 'snowflake_cursor' not in st.session_state:
        # Snowflake cursor does not exist, try creating it
        conn = get_snowflake_connection()
        if conn:
            try:
                cursor = conn.cursor()
                st.session_state.snowflake_cursor = cursor  # Store the cursor in session state
                return cursor
            except Exception as e:
                st.error(f"Failed to create Snowflake cursor: {str(e)}")
                return None
        else:
            return None
    else:
        # Snowflake cursor already exists, return it
        return st.session_state.snowflake_cursor
# choice = st.sidebar.radio('',['App', 'Connect To Snowflake', 'About'], key='sidebar_radio', index=st.session_state.get('page', 0))

# Get the current page from session state, defaulting to 'App'
current_page = st.session_state.get('page', 'App')

# Define the options for the sidebar radio button
options = ['App', 'Connect To Snowflake', 'About']

# Display the sidebar radio button and update the current page based on the selection
current_page = st.sidebar.radio('Navigation', options, index=options.index(current_page))

# Store the updated current page in session state
st.session_state.page = current_page

if current_page == 'App': 
   c1, c2 = st.columns([1, 5])  # Adjust column widths as needed

   # First image in the corner
   with c1:
       st.image('awslogo.png', width=230)

   with c2: 
       st.markdown("<h2 style='text-align: center; color: #F2920C; text-decoration: underline;'>WELCOME TO DATA MIGRATOR</h2>", unsafe_allow_html=True)
   st.markdown('---')

   st.markdown("<h2 style='text-align: left; color: #F2920C; text-decoration: underline;'>ENTER YOUR AWS AUTHENTICATION DETAILS</h2>", unsafe_allow_html=True)

   c1, c2, c3 = st.columns([3, 1, 3])  # Adjust column widths as needed

   with c1:
       st.markdown("<h4 style='color: #F2920C; text-align: left;'>Access Key:</h4>", unsafe_allow_html=True)
       aws_access_key_id = st.text_input("", key="access_key")
       st.markdown("<h4 style='color: #F2920C; text-align: left;'>Secret Key:</h4>", unsafe_allow_html=True)
       aws_secret_access_key = st.text_input("", key="secret_key", type="password")
       st.markdown("<h4 style='color: #F2920C; text-align: left;'>Region:</h4>", unsafe_allow_html=True)
       selected_region = st.selectbox("", options=["Please select a region", "us-east-1", "us-west-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "sa-east-1", "ca-central-1"], key="selected_region")

   # It checks if aws_access_key_id and aws_secret_access_key are not empty (truthy values) and if selected_region is not equal to the placeholder string "Please select a region".    
   if aws_access_key_id and aws_secret_access_key and selected_region != "Please select a region":

        s3 = boto3.resource(
         service_name='s3',
         region_name=selected_region,
         aws_access_key_id=aws_access_key_id,
         aws_secret_access_key=aws_secret_access_key  
       )  


        with c2:
          st.markdown('<br><br>', unsafe_allow_html=True)  # Adjust spacing here

        with c3:
          st.image('IMG2.png', width=450)

        st.markdown('<br><br>', unsafe_allow_html=True) 

        # Get a list of bucket names
        bucket_names = [bucket.name for bucket in s3.buckets.all()]

       # Create a dropdown menu to select a bucket name
        selected_bucket = st.selectbox("Bucket Name", options=bucket_names)

       # Get a list of objects in bucket
        bucket = s3.Bucket(selected_bucket)
        file_names = [obj.key for obj in bucket.objects.all()]

      # Create a dropdown menu to select a file
        selected_file = st.selectbox("Select File", options=file_names, key="selected_file")
        st.session_state.selectedfile = selected_file
        
      # Load the selected CSV file from S3
        obj = s3.Object(selected_bucket, selected_file)
        file_content = obj.get()['Body'].read()

      # Display the DataFrame in Streamlit
        df = pd.read_csv(io.BytesIO(file_content))
        st.write(df)
        st.session_state.dataframe = df

   if st.button('Next'):
        current_page = 'Connect To Snowflake'
        st.session_state.page = 'Connect To Snowflake'


if current_page == 'Connect To Snowflake': 
   # Center the image using Streamlit's layout options
   col1, col2, col3 = st.columns([2, 2, 1])  # Adjust column widths as needed

   with col1:
        st.write("")  # Empty placeholder for layout

   with col2:
        st.image("Snowflake_Logo.png", width=310)  # Centered image

   with col3:
        st.write("")  # Empty placeholder for layout
   
   st.markdown("<h2 style='text-align: left; color: #0CC4F2; text-decoration: underline;'>Connect To Snowflake</h2>", unsafe_allow_html=True)
   
   c1, c2 = st.columns([2, 2])

   with c2:
       st.image('IMG3.png', width=700)
   
   with c1:
       cur = get_snowflake_cursor()
   
   if cur:
            cur = get_snowflake_cursor() 
   # Get a list of database names
            cur.execute("SHOW DATABASES")
            database_names = [row[1] for row in cur.fetchall()]
    # Create a dropdown menu to select a database
            selected_database = st.selectbox("Select_Database", options=database_names, key='selected_database')
            # st.session_state.selected_database = selected_database


    # Get a list of schema names in the selected database
            cur.execute(f"SHOW SCHEMAS IN DATABASE {selected_database}")
            schema_names = [row[1] for row in cur.fetchall()]
    # Create a dropdown menu to select a schema
            selected_schema = st.selectbox("Select_Schema", options=schema_names)
            # st.session_state.selected_schema = selected_schema
  
    # Get the DataFrame from AWS S3

            df = st.session_state.dataframe
            selected_file = st.session_state.selectedfile

    # Check if the DataFrame is not None and display it
            if df is not None:
                st.subheader("Staged in Snowflake from AWS S3")
                st.write(df)
            
            # Define the table name input field
            table_name = st.text_input("Enter the table name", key="table_name")     
           
            
            # # Get Snowflake cursor
        # snowflake_cursor = get_snowflake_cursor()
       

            # Trigger Snowpipe when the button is clicked
            if st.button("Push Data To Snowflake"):
              if table_name:
                file_path = selected_file
                # stage_name = 'S3_STAGE'
            
                #    # Get DataFrame from session state
                # df = st.session_state.dataframe

                #   # Create table definition string based on DataFrame columns
                # column_definitions = get_column_definitions(df)
                try:
                  trigger_snowpipe(table_name, selected_database, selected_schema, file_path)
                  st.success("Trigger_Sucessfull")
                except Exception as e:
                  st.error("Failed to trigger Snowpipe.")            
            
if current_page == 'About':
    st.title('About Data Migrator App')
    st.markdown('---')

    st.header('Purpose:')
    st.write('The Data Migrator App is designed to facilitate seamless data migration between AWS S3 and Snowflake, providing users with an intuitive interface to manage data transfer operations.')

    st.header('Key Features:')
    st.markdown('- AWS Authentication: Authenticate with AWS using your access key, secret key, and select the desired region.')
    st.markdown('- Select Data Source: Choose a bucket and file from AWS S3 to load data into the app.')
    st.markdown('- Connect to Snowflake: Enter your Snowflake credentials to establish a connection and push data into Snowflake.')
    st.markdown('- Interactive Interface: Navigate through different pages using the sidebar navigation.')

    st.header('Developed by')
    st.write('- Akhila Bolla: Data Analyst and Developer')

    st.header('Contact Us:')
    st.write('For inquiries or support, please contact us at data.migrator@example.com.')

    st.header('Acknowledgments:')
    st.write('We acknowledge the support of Streamlit and Snowflake in developing this application.')

    st.header('Disclaimer:')
    st.write('This application is for demonstration purposes only. Use it responsibly and ensure compliance with data security and privacy regulations.')

    st.markdown('---')
    st.write('© 2024 Data Migrator App. All rights reserved.')

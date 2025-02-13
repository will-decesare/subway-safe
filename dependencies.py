import os
import time
import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from datetime import datetime

from config import db_path

def start_db():
    print('Connected to database.')
    conn = duckdb.connect(db_path)
    return conn

def close_db(conn):
    print('Ending database connection.')
    conn.close()

def execute_query(conn, query):
    return conn.execute(query)

def run_query(conn, query):
    return conn.sql(query).df()
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import os

# Fix for Windows PySpark
os.environ["PYSPARK_PYTHON"] = "python"

# Create Spark Session
spark = SparkSession.builder.appName("LibraryUI").getOrCreate()

st.title("📚 Smart Library Dashboard")

# Load CSV files using Spark
books_df = spark.read.csv("data/books.csv", header=True, inferSchema=True)
users_df = spark.read.csv("data/users.csv", header=True, inferSchema=True)
trans_df = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)

# Convert to Pandas
books_pd = books_df.toPandas()
users_pd = users_df.toPandas()
trans_pd = trans_df.toPandas()

# Sidebar
option = st.sidebar.selectbox(
    "Select Analysis",
    ["Most Borrowed Books", "Active Users", "Late Returns", "Fine Calculation"]
)

# 📊 1. Most Borrowed Books
if option == "Most Borrowed Books":
    st.subheader("📊 Most Borrowed Books")

    result = trans_pd["book_id"].value_counts().reset_index()
    result.columns = ["book_id", "count"]

    final = pd.merge(result, books_pd, on="book_id", how="left")

    st.dataframe(final)
    st.bar_chart(final["count"])

# 👤 2. Active Users
elif option == "Active Users":
    st.subheader("👤 Top Active Users")

    result = trans_pd["user_id"].value_counts().reset_index()
    result.columns = ["user_id", "count"]

    final = pd.merge(result, users_pd, on="user_id", how="left")

    st.dataframe(final)

# ⏰ 3. Late Returns
elif option == "Late Returns":
    st.subheader("⏰ Late Returns")

    late = trans_pd[trans_pd["return_date"].isna()]

    st.dataframe(late)

# 💰 4. Fine Calculation
elif option == "Fine Calculation":
    st.subheader("💰 Fine Calculation")

    # Convert dates safely
    trans_pd["borrow_date"] = pd.to_datetime(trans_pd["borrow_date"], errors='coerce')
    trans_pd["return_date"] = pd.to_datetime(trans_pd["return_date"], errors='coerce')

    # Calculate days
    trans_pd["days"] = (trans_pd["return_date"] - trans_pd["borrow_date"]).dt.days

    # Fill nulls (not returned)
    trans_pd["days"] = trans_pd["days"].fillna(0)

    # Fine logic
    trans_pd["fine"] = trans_pd["days"].apply(lambda x: x * 5 if x > 7 else 0)

    st.dataframe(trans_pd[["user_id", "book_id", "fine"]])
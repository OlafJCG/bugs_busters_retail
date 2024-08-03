# Libraries -----------------------------------------------------------------------------------
import os
import sys
import numpy as np

import pandas as pd

from functions import detect_outliers as detect_outliers

sys.path.append(os.getcwd())

# Load Dataset -----------------------------------------------------------------------------------
df = pd.read_csv('files/datasets/input/Online_Retail.csv', sep=',', encoding='latin1')

# Standardize Column Names -----------------------------------------------------------------------------------
df.columns = [x.lower() for x in df.columns]

# Data Enrichment -----------------------------------------------------------------------------------
df['total_cost'] = df['quantity'] * df['unit_price']
df['invoice_date'] = pd.to_datetime(df['invoice_date'], format="%d/%m/%Y %H:%M")
df['month'] = df['invoice_date'].dt.month
df['year'] = df['invoice_date'].dt.year

# Data cleaning -----------------------------------------------------------------------------------
df = df.drop_duplicates()
df = df.dropna(subset=['quantity', 'unit_price', 'customer_id', 'total_cost'])
# Eliminar filas con unit_price igual a 0.0
df = df[df['unit_price'] != 0.0]

# Outliers Removing -----------------------------------------------------------------------------------
# Quantity column
lower_quantity, upper_quantity = detect_outliers.IQR_upper_lower_levels(df['quantity'])
# Filter DataFrame
df = df.query("quantity > @lower_quantity & quantity < @upper_quantity")

# unit_price column
lower_unit_price, upper_unit_price = detect_outliers.IQR_upper_lower_levels(df['unit_price'])
# Filter DataFrame
df = df.query("unit_price > @lower_unit_price & unit_price < @upper_unit_price")

# Save DataFrame -----------------------------------------------------------------------------------
df.to_parquet('files/datasets/intermediate/a01_preprocessed.parquet')
import sqlite3
import pandas as pd

class DataPreparation:
    
    def read_db(path):
        # Connect to the SQLite database
        conn = sqlite3.connect(path)

        # Query to get the name of the first table
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        table_name = pd.read_sql_query(query, conn).iloc[0, 0]  # Get the first table name
        # Use pandas to read data from the table
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        # Close the connection
        conn.close()
        return df
    
    def grouped_data(df, group_columns, filter_columns, quarter, year, value_columns=['policies_purchased', 'premium_value']):

        grouped_data = df.groupby(group_columns).agg({col: 'sum' for col in value_columns}).reset_index()

        # Replace hyphen with space and capitalize the display column (the last column in group_columns)
        display_column = group_columns[-1]  # Assuming the last column is the one to display (e.g., state, district, or pincode)
        grouped_data[f'{display_column}_display'] = grouped_data[display_column].str.replace("-", " ").str.title()

        # Filter data by quarter and year
        grouped_data = grouped_data[(grouped_data['year'] == year) & (grouped_data['quarter'] == quarter)]

        return grouped_data
    
    def main(self):
        state_isnurance_df = self.read_db('/content/drive/MyDrive/Phonepe Pulse/state_insurance.db')
        district_insurance_df = self.read_db('/content/drive/MyDrive/Phonepe Pulse/district_insurance.db')
        pincode_insurance_df = self.read_db('/content/drive/MyDrive/Phonepe Pulse/pincode_insurance.db')
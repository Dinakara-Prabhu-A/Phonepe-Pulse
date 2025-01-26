import os
import json
import pandas as pd
import sqlite3
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    state_insurance_db_path: str = os.path.join('artifact', 'state_insurance.db')
    district_insurance_db_path: str = os.path.join('artifact', 'district_insurance.db')
    pincode_insurance_db_path: str = os.path.join('artifact', 'pincode_insurance.db')
    state_transaction_db_path: str = os.path.join('artifact', 'state_transaction.db')
    district_transaction_db_path: str = os.path.join('artifact', 'district_transaction.db')
    pincode_transaction_db_path: str = os.path.join('artifact', 'pincode_transaction.db')

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()
        for path in [self.ingestion_config.state_insurance_db_path, 
                     self.ingestion_config.district_insurance_db_path, 
                     self.ingestion_config.pincode_insurance_db_path]:
            folder = os.path.dirname(path)
            if not os.path.exists(folder):
                os.makedirs(folder)

    def traverse_directory(self, base_path):
        for state in os.listdir(base_path):
            for year in os.listdir(os.path.join(base_path, state)):
                for file in os.listdir(os.path.join(base_path, state, year)):
                    yield state, year, file

    def state_insurance_data(self):
        state_insurance = []
        base_path = "notebook/data/aggregated/insurance/country/india/state"
        for state, year, file in self.traverse_directory(base_path):
            with open(f"{base_path}/{state}/{year}/{file}", 'r') as json_file:
                json_data = json.load(json_file)
                for values in json_data['data']['transactionData']:
                    data = dict(
                        policies_purchased=values['paymentInstruments'][0]['count'],
                        premium_value=values['paymentInstruments'][0]['amount'],
                        year=year,
                        state=state,
                        quarter=file[0]
                    )
                    state_insurance.append(data)
        return state_insurance

    def district_insurance_data(self):
        district_insurance = []
        base_path = "notebook/data/top/insurance/country/india/state"
        for state, year, file in self.traverse_directory(base_path):
            with open(f"{base_path}/{state}/{year}/{file}", 'r') as json_file:
                json_data = json.load(json_file)
                for values in json_data['data'].get('districts', []):
                    data = dict(
                        district=values['entityName'],
                        policies_purchased=values['metric']['count'],
                        premium_value=values['metric']['amount'],
                        year=year,
                        state=state,
                        quarter=file[0]
                    )
                    district_insurance.append(data)
        return district_insurance

    def pincode_insurance_data(self):
        pincode_insurance = []
        base_path = "notebook/data/top/insurance/country/india/state"
        for state, year, file in self.traverse_directory(base_path):
            with open(f"{base_path}/{state}/{year}/{file}", 'r') as json_file:
                json_data = json.load(json_file)
                for values in json_data['data'].get('pincodes', []):
                    data = dict(
                        pincode=values['entityName'],
                        policies_purchased=values['metric']['count'],
                        premium_value=values['metric']['amount'],
                        year=year,
                        state=state,
                        quarter=file[0]
                    )
                    pincode_insurance.append(data)
        return pincode_insurance

    def state_transaction_data(self):
        agg_transaction_list = []
        base_path = "notebook/data/aggregated/transaction/country/india/state"
        for state, year, file in self.traverse_directory(base_path):
            with open(f"{base_path}/{state}/{year}/{file}", 'r') as json_file:
                json_data = json.load(json_file)
                for values in json_data['data']['transactionData']:
                    data = dict(
                        categories=values['name'],
                        all_transactions=values['paymentInstruments'][0]['count'],
                        payment_value=values['paymentInstruments'][0]['amount'],
                        year=year,
                        state=state,
                        quarter=file[0]
                    )
                    agg_transaction_list.append(data)
        return agg_transaction_list

    def district_transaction_data(self):
        district_transaction_list = []
        base_path = "notebook/data/top/transaction/country/india/state"
        for state, year, file in self.traverse_directory(base_path):
            with open(f"{base_path}/{state}/{year}/{file}", 'r') as json_file:
                json_data = json.load(json_file)
                for values in json_data['data'].get('districts', []):
                    data = dict(
                        district=values['entityName'],
                        all_transactions=values['metric']['count'],
                        payment_value=values['metric']['amount'],
                        year=year,
                        state=state,
                        quarter=file[0]
                    )
                    district_transaction_list.append(data)
        return district_transaction_list

    def pincode_transaction_data(self):
        pincode_transaction_list = []
        base_path = "notebook/data/top/transaction/country/india/state"
        for state, year, file in self.traverse_directory(base_path):
            with open(f"{base_path}/{state}/{year}/{file}", 'r') as json_file:
                json_data = json.load(json_file)
                for values in json_data['data'].get('pincodes', []):
                    data = dict(
                        pincode=values['entityName'],
                        all_transactions=values['metric']['count'],
                        payment_value=values['metric']['amount'],
                        year=year,
                        state=state,
                        quarter=file[0]
                    )
                    pincode_transaction_list.append(data)
        return pincode_transaction_list

    def save_to_db(self, data, table_name, db_path):
        conn = sqlite3.connect(db_path)
        df = pd.DataFrame(data)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()

    def insurance_data(self):
        # Collect data for each level
        state_data = self.state_insurance_data()
        district_data = self.district_insurance_data()
        pincode_data = self.pincode_insurance_data()

        # Save data to respective databases
        self.save_to_db(state_data, "state_insurance_data", self.ingestion_config.state_insurance_db_path)
        self.save_to_db(district_data, "district_insurance_data", self.ingestion_config.district_insurance_db_path)
        self.save_to_db(pincode_data, "pincode_insurance_data", self.ingestion_config.pincode_insurance_db_path)

        return "All insurance data has been successfully aggregated and saved to their respective databases."
    
    def transaction_data(self):
        # Collect data for each level
        state_data = self.state_transaction_data()
        district_data = self.district_transaction_data()
        pincode_data = self.pincode_transaction_data()

        # Save data to respective databases
        self.save_to_db(state_data, "state_transaction_data", self.ingestion_config.state_transaction_db_path)
        self.save_to_db(district_data, "district_transaction_data", self.ingestion_config.district_transaction_db_path)
        self.save_to_db(pincode_data, "pincode_transaction_data", self.ingestion_config.pincode_transaction_db_path)

        return "All transaction data has been successfully aggregated and saved to their respective databases."
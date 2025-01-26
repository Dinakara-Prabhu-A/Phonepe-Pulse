from src.data_ingestion import DataIngestion

def main():
    # Initialize DataIngestion
    ingestion = DataIngestion()

    # Perform data aggregation
    print("Starting data aggregation...")
    insurance = ingestion.insurance_data()
    print(insurance)
    transactions = ingestion.transaction_data()
    print(transactions)

if __name__ == "__main__":
    main()
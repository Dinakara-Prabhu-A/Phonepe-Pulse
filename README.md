# PhonePe Pulse 2018-2024 Analysis Dashboard

## Overview

This interactive dashboard provides a comprehensive analysis of PhonePe transactions and insurance data across India from 2018 to 2024. The dashboard enables users to explore state-wise, district-wise, and pincode-wise transaction insights using visualizations such as maps, bar charts, and key metrics. It helps in understanding financial trends, digital payment adoption, and insurance policy purchases at different granular levels.

## Features

* **Interactive Year & Quarter Selection:** Users can filter data dynamically by selecting a specific year and quarter.
* **Dual Mode Analysis:** Supports both Transaction and Insurance data analysis.
* **State-wise Data Visualization:** A choropleth map showcasing transaction or policy trends across Indian states.
* **Key Metrics Display:** Shows total transactions, total payment or premium value, and average transaction or premium value.
* **Top 10 Analysis:**
    * Top 10 States by transactions/policies purchased.
    * Top 10 Districts by transactions/policies purchased.
    * Top 10 Pincodes by transactions/policies purchased.
* **Responsive UI:** Designed for a seamless user experience with Streamlit's wide layout.

## Tech Stack

* **Frontend:** Streamlit for interactive visualizations.
* **Database:** SQLite for efficient data storage and retrieval.
* **Visualization:** Plotly for creating interactive maps and bar charts.
* **Data Processing:** Pandas for efficient data manipulation.

## Installation & Setup

1.  Clone the repository:

    ```bash
    git clone [https://github.com/Dinakara-Prabhu-A/Phonepe-Pulse.git](https://github.com/Dinakara-Prabhu-A/Phonepe-Pulse.git)
    cd phonepe-pulse
    ```

2.  Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3.  Run the application:

    ```bash
    streamlit run app.py
    ```

## Usage Instructions

1.  Select Insurance or Transaction from the navigation bar.
2.  Choose the desired year and quarter from the dropdown filters.
3.  View interactive visualizations, including:
    * State-wise distribution on the map.
    * Total transactions, total payment/premium value, and average transaction/premium value.
    * Top 10 states, districts, and pincodes represented as bar charts.

## Screenshot

![PhonePe Pulse Dashboard]([notebook/localhost_8501_ (1).png](https://github.com/Dinakara-Prabhu-A/Phonepe-Pulse/blob/main/notebook/localhost_8501_%20(1).png))

## Future Enhancements

* Implement drill-down capabilities for district and pincode-level maps.
* Add time-series analysis for trend visualization.
* Enable data export functionality for further analysis.

## Author

[Your Name]

* LinkedIn: (https://www.linkedin.com/in/dinakaraprabhu)
* GitHub: (https://github.com/Dinakara-Prabhu-A/)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

# ETL Project 1 #
## Structure ##
<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        .
        ├── .gitignore
        ├── README.md
        ├── requirements.txt
        ├── config
        │   └── configuration.py
        ├── data
        │   └── Dataset.csv
        ├── docs
        │   ├── Games dashboard.pdf
        │   └── ETL.pdf
        ├── notebooks
        │   ├── EDA.ipynb
        │   └── databse.ini
        └── src
            ├── database.ini
            └── main.py
    </pre>
</div>

## Overview ##
_This project demonstrates how to create and manage games data using PostgreSQL and Python. It covers connecting to a PostgreSQL database, creating tables, importing data from CSV files, and performing basic SQL operations. Also we give an analytic to the data and visualization for a better understanding, here you will know how useful can be de data!_

_Also, *[Detailed Documentation](https://github.com/juancbuitrago/ETL-Project/blob/main/docs/ETL.pdf)* is available that covers everything from data selection to the final visualizations_

## Table of Contents ##
- [Requirements](#requirements)
- [Setup](#setup)
- [Data Analysis](#exploratory-data-analysis)
- [Analysis & Visualizations](#analysis-visualizations)

## Requirements <a name="requirements"></a> ##
- Python 3.x
- SQLAlchemy
- psycopg2
- Matplotlib & Seaborn
- Pandas
- PostgreSQL
- Jupyter Notebook
- database credentials file ("database.ini") with this format:
 
```
[postgresql]
host=your_host
database=games
user=your_user
password=your_password

``` 

## Setup <a name="setup"></a> ##
_First of all, 
ensure you have the following programs installed with which the entire project procedure is carried out:_

   - **[Python](https://www.python.org)**
   - **[PostgreSQL](https://www.postgresql.org/download/)**
   - **[PowerBI](https://powerbi.microsoft.com/es-es/downloads/)**
   - **[VS Code](https://code.visualstudio.com/download)**
   - **[Jupyter](https://jupyter.org/install)**

_Using the **[requirements.txt](https://github.com/juancbuitrago/ETL-Project/blob/main/requirements.txt)**
run the following command in the Terminal (You will need to be sure that you are in the correct folder in the terminal)_

```python
pip install -r requirements.txt
```
_Previous command will install the following necessary libraries for the ETL Project_

```python
- matplotlib
- pandas
- pip
- psycopg2
- seaborn
- sqlalchemy

```

 ## Explorate Data Analysis <a name="exploratory-data-analysis"></a> ##

 _This process was carried out in **[EDA Notebook](https://github.com/juancbuitrago/ETL-Project/blob/main/notebooks/EDA.ipynb)** where the following procedures are being carried out:_

- Identification of the data frame structure
- Identification of columns names
- Identification of data types
- Identification of null data
- Correct the nulls values
- Give the correct structure to the columns
- Exploratory Data Analysis
- Graphics and analysis

## Analysis & Visualization <a name="analysis-visualizations"></a> ###

### These visualizations can be seen in the **[Dashboard Summary](https://github.com/juancbuitrago/ETL-Project/blob/main/docs/Games%20dashboard.pdf)**.

### Also, threre is the **[Published Dashboard](https://app.powerbi.com/links/Ftzdqf7wMF?ctid=693cbea0-4ef9-4254-8977-76e05cb5f556&pbi_source=linkShare&bookmarkGuid=b0b55ff7-280c-4ed7-bc76-83bce60db00f)** for a better interactive experience with the dashboard and the data.
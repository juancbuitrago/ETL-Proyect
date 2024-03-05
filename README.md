# Videogames

## Proyect Structure

The structure of the directories and files is as follows:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        .
        ├── .gitignore
        ├── README.md
        └── src
            ├── .ipynb_checkpoints
            │   └── EDA-checkpoints.ipynb
            ├── Dataset.csv
            ├── EDA.ipynb
            ├── config.py
            └── main.py
    </pre>
</div>

## Installation Requirements

To run the project, these are the libraries you need to have installed

- pandas

- matplotlib

- seaborn

- psycopg2

These can be installed using pip like this:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        pip install pandas
    </pre>
</div>

## Proyect Execution

1.  Specify the location where you want to host the project, then use this command to clone the repository inside the folder:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        git clone <url_del_repositorio>
    </pre>
</div>

1.1 Enter the project with this command (you must run this inside the folder where you are cloning the repository):

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        cd ETL-Proyect
    </pre>
</div>

2.  Execute the file "main.py" to load the dataset from the csv file to the PostgreSQL, you can do it with this command:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        python src/main.py
    </pre>
</div>

3.  Open and run the Jupyter notebook "EDA.ipynb".
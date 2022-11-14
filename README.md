# ETL Pipeline For Virtual Sparta Global Dataset
Data were drawn from S3 bucket and saved as dataframes. Data is transformed and cleaned to fit the schema (see `schema.py` for reference). Then, the data is saved in MySQL database. The code supports use of Docker.

## Set up
1. Open terminal in the directory with projects or create a new directory
2. Clone repository `$ git clone https://github.com/jpatryk7/etl_pipeline_virtual_sparta_v3.git`
3. Cd into the project directory `$ cd etl_pipeline_virtual_sparta_v3`
4. Create a new virtual environment `$ python -m venv my_venv`
5. Configure interpreter settings to add `my_venv` to the project and restart IDE (**make sure that you have my_venv showing in your command line**)
6. Install required packages `$ pip install -r requirements.txt`
7. Make sure that Docker is up and running
8. Run `$ python src/run.py`
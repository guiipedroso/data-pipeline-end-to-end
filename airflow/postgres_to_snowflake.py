from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
 
"""

DAG: postgres_to_snowflake

Project: DrivaMotors

Responsible for performing incremental data loading from Postgres database
(on-premise) to Snowflake, using the primary key of each table (ID_<table>)
as the incrementality criterion.

The process:
1. Query Snowflake for the maximum ID already loaded for each table;
2. Fetch from Postgres only records with ID greater than the existing one;
3. Insert new records into Snowflake.

"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
 
@dag(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule=timedelta(days=1),
    catchup=False
)
def postgres_to_snowflake_etl():
    """
    Main DAG that orchestrates incremental data loading from Postgres to Snowflake.
    
    This function defines the workflow that processes multiple tables from the
    DrivaMotors system, ensuring that only new records are loaded.
    
    Processed tables:
        - veiculos: Available vehicles catalog
        - estados: Brazilian states
        - cidades: Cities and their locations
        - concessionarias: Dealerships
        - vendedores: Sales team
        - clientes: Customer base
        - vendas: Sales transactions
    
    Execution flow:
        For each table, the DAG executes two tasks in sequence:
        1. get_max_id_{table_name}: Fetches the maximum ID already loaded in Snowflake
        2. load_data_{table_name}: Loads records with ID greater than the maximum found
    
    Returns:
        None: The function defines the DAG task graph
    """
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']
 
    for table_name in table_names:
        @task(task_id=f'get_max_id_{table_name}')
        def get_max_primary_key(table_name: str):
            """
            Queries the maximum ID already loaded in Snowflake for a specific table.
            
            This function is essential to ensure load incrementality,
            allowing only new records to be processed.
            
            Args:
                table_name (str): Name of the table to query
            
            Returns:
                int: Maximum ID value found in the table or 0 if the table is empty
            
            Example:
                For the 'vendas' table, fetches the maximum value of 'ID_vendas'
                If the maximum ID is 1500, returns 1500
                If the table is empty, returns 0
            """
            with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT MAX(ID_{table_name}) FROM {table_name}")
                    max_id = cursor.fetchone()[0]
                    return max_id if max_id is not None else 0
 
        @task(task_id=f'load_data_{table_name}')
        def load_incremental_data(table_name: str, max_id: int):
            """
            Performs incremental data loading from Postgres to Snowflake.
            
            This function executes the complete extraction and load process:
            1. Dynamically discovers table columns in Postgres
            2. Extracts only records with ID greater than the maximum already loaded
            3. Inserts new records into Snowflake
            
            Args:
                table_name (str): Name of the table to be processed
                max_id (int): Maximum ID already existing in Snowflake (obtained from previous task)
            
            Returns:
                None: The function performs I/O operations but does not return a value
            
            Detailed process:
                - Queries information_schema to get all table columns
                - Fetches from Postgres records where ID_{table_name} > max_id
                - Inserts row by row into Snowflake maintaining the original structure
            
            Example:
                If max_id=1500 for 'vendas', loads only sales with ID_vendas > 1500
            """
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    primary_key = f'ID_{table_name}'
                    
                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                    columns = [row[0] for row in pg_cursor.fetchall()]
                    columns_list_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))
                    
                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id}")
                    rows = pg_cursor.fetchall()
                    
                    with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cursor:
                            insert_query = f"INSERT INTO {table_name} ({columns_list_str}) VALUES ({placeholders})"
                            for row in rows:
                                sf_cursor.execute(insert_query, row)
 
        max_id = get_max_primary_key(table_name)
        load_incremental_data(table_name, max_id)
 
postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()
 

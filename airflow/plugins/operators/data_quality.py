from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 to_check,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.to_check = to_check,
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table,column in self.to_check:
            self.log.info(f"Checking {table} table")
            records = redshift_hook.get_records(SqlQueries.rows_in_table_sql_format.format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} is empty")
            else:
                self.log.info(f"The {table} table is not empty")
                if column:
                    records = redshift_hook.get_records(SqlQueries.null_in_column_sql_format.format(table))
                    if len(records) < 1 or len(records[0]) > 1:
                        raise ValueError(f"Data quality check failed. {table} contains null values in column {column}")
                    else:
                        self.log.info(f"The {table} table contains no null values in {column} column")

                        
        self.log.info("Data quality checks passed")  

                    
                
                    

            
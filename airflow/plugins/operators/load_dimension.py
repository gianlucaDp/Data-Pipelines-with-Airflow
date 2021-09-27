from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 select_query,
                 truncate=False,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table,
        self.select_query = select_query,
        self.truncate = truncate,
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Truncating {self.table} table")
            formatted_sql = SqlQueries.truncate_sql_format.format(self.table)
            redshift.run(formatted_sql)
            
        self.log.info(f"Inserting data into {self.table} table")
        formatted_sql = SqlQueries.insert_sql_format.format(table,
                                                 select_query)
        redshift.run(formatted_sql)

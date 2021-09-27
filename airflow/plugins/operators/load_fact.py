from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 select_query,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select_query = select_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Inserting data into {self.table} table")
        formatted_sql = SqlQueries.insert_sql_format.format(self.table,
                                                 self.select_query)
        redshift.run(formatted_sql)
        self.log.info(f"Data inserted successfully!")

        


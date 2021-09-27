from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                table,
                formatting,
                s3_key,
                s3_bucket='udacity-dend',
                redshift_conn_id='redshift',
                aws_credentials_id="aws_credentials",
                region='us-west-2',
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table,
        self.formatting=formatting,
        self.redshift_conn_id=redshift_conn_id,
        self.aws_credentials_id= aws_credentials_id,
        self.region=region,
        self.s3_bucket=s3_bucket,
        self.s3_key=s3_key


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Copying data from S3 to Redshift {self.table} table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = SqlQueries.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.formatting
        )
        redshift.run(formatted_sql)







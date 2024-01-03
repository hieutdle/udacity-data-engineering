from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 check_sql_lists=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_sql_lists = check_sql_lists
        
    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        
        error_count = 0
        
        for sql in self.check_sql_lists:
            result = int(redshift_hook.get_first(sql=check_sql_lists['sql'])[0])
            if check_sql_lists['option'] == 'equal':
                if result != check_sql_lists['val']:
                    error_count += 1
                    raise AssertionError(f"Check failed: {result} {check_sql_lists['option']} {check_sql_lists['val']}")
            elif check_sql_lists['op'] == 'not equal':
                if result == check_sql_lists['val']:
                    error_count += 1
                    raise AssertionError(f"Check failed: {result} {check_sql_lists['option']} {check_sql_lists['val']}")
        
        if error_count == 0:
            self.log.info("All data quality checks passed")
        if error_count == 2:
            raise ValueError('All data quality check failed')
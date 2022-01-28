from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
import logging
import snowflake.connector

log = logging.getLogger(__name__)
class SnowflakeCustomOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,     
            sql: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql=sql
     
    def execute(self, context):
    # credentials
        snowflake_username = BaseHook.get_connection('snowflake').login
        snowflake_password = BaseHook.get_connection('snowflake').password
        extra = BaseHook.get_connection('snowflake').extra_dejson
        account = extra.get('account')   
        database = extra.get('database')  
        # Gets the version
        ctx = snowflake.connector.connect(
            user=snowflake_username,
            password=snowflake_password,
            account=account,
            database=database
        )

        cs = ctx.cursor()        
        try:
            cs.execute(self.sql)
            one_row = cs.fetchall()
            print(one_row)
            return one_row
        finally:
            cs.close()

class SnowflakeCustomOperatorPlugin(AirflowPlugin):
   name = "SnowflakeCustomOperatorPlugin"
   operators = [SnowflakeCustomOperator]
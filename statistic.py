from utils import DataFlowBaseOperator
import psycopg2


class PostgresStatistic(DataFlowBaseOperator):
    def __init__(self,config , *args, **kwargs):
        super(PostgresStatistic, self).__init__(
            *args,
            **kwargs
        )
        self.config = config

    def execute(self, context):
        schema_name = "{table}".format(**self.config).split(".")
        self.config.update(
            target_schema=schema_name[0],
            target_table=schema_name[1],
        )

        with psycopg2.connect(self.pg_meta_conn_str) as conn, conn.cursor() as cursor:

            # modify
            cursor.execute(
                """
            select column_name
              from information_schema.columns
             where table_schema = '{target_schema}'
               and table_name = '{target_table}'
               and column_name not in ('launch_id', 'effective_dttm');
            """.format(
                    **self.config
                )
            )
            result = cursor.fetchall()

            columns = ", ".join('"{}"'.format(row) for row, in result)

            cursor.execute(
                """ select count(1) from {target_table}"""
                    .format(**self.config)
            )
            c_all = cursor.fetchone()
            self.config.update(cnt_all=c_all[0])

            job_id = context["task_instance"].job_id
            self.config.update(launch_id=job_id)

            for l_c in columns.split(','):
                self.config.update(column=l_c)

                cursor.execute(
                        """ select count(1) from {target_table} where {column} is null"""
                            .format(**self.config)
                )
                c_nulls = cursor.fetchone()
                self.config.update(cnt_nulls=c_nulls[0])

                self.write_etl_statistic(self.config)

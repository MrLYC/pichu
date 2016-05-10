from contextlib import closing


class BaseEngine(object):

    def __init__(self):
        super(BaseEngine, self).__init__()

    def get_cursor(self, *args, **kwargs):
        raise NotImplemented

    def execute(self, sql_builder):
        with closing(self.get_cursor()) as cursor:
            cursor.execute(
                sql_builder._build_sql(),
                sql_builder._build_parameters(),
            )
            while True:
                result = cursor.fetchone()
                import pdb
                pdb.set_trace()
                if result is None:
                    break
                yield sql_builder._parse_db_result(result)


class SingleConnectionEngine(BaseEngine):

    def __init__(self, connection):
        super(SingleConnectionEngine, self).__init__()
        self.connection = connection

    def get_cursor(self):
        return self.connection.cursor()

from contextlib import closing
from copy import deepcopy

from pichu.utils import chaining_method


class QueryValueError(Exception):
    pass


class BaseQuery(object):

    def __init__(self, engine, model):
        super(BaseQuery, self).__init__()
        self.engine = engine
        self.model = model

    def copy(self):
        return deepcopy(self)

    def _execute_sql(self, cursor):
        execute_params = [self._build_sql()]
        parameters = self._build_parameters()
        if parameters:
            execute_params.append(parameters)

        cursor.execute(*execute_params)

    def execute(self, *args, **kwargs):
        with closing(self.engine.get_cursor(*args, **kwargs)) as cursor:
            self._execute_sql(cursor)
            return self._make_result(cursor)

    def iterate(self, *args, **kwargs):
        with closing(self.engine.get_cursor(*args, **kwargs)) as cursor:
            self._execute_sql(cursor)

            while True:
                result = self._make_iter_result(cursor)
                if result is None:
                    break
                yield result

    def _make_result(self, cursor):
        raise NotImplemented

    def _make_iter_result(self, cursor):
        raise NotImplemented

    def _build_sql(self):
        raise NotImplemented

    def _build_parameters(self):
        return None


class SelectQuery(BaseQuery):
    SortOrderTypes = ["ASC", "DESC"]

    def __init__(self, engine, model):
        super(SelectQuery, self).__init__(engine, model)
        self.where_condition = None
        self.limit_offset = None
        self.limit_count = None
        self.order_by_fields = None
        self.sort_order_type = None

    @chaining_method
    def where(self, where_condition):
        self.where_condition = where_condition

    @chaining_method
    def limit(self, offset, count):
        self.limit_offset = offset
        self.limit_count = count

    @chaining_method
    def order_by(self, *fields):
        if self.sort_order_type is None:
            self.sort_order("ASC")

        self.order_by_fields = fields

    @chaining_method
    def sort_order(self, type_):
        if type_ not in self.SortOrderTypes:
            raise QueryValueError("%s not allowed" % type_)

        self.sort_order_type = type_

    def _build_sql(self):
        sql_parts = ["SELECT"]
        sql_parts.extend([
            f.column
            for f in self.model.X.fields
        ])
        sql_parts.extend([
            "FROM", self.model.X.table,
        ])

        if self.where_condition:
            sql_parts.extend([
                "WHERE", self.where_condition.as_sql(),
            ])

        if self.order_by_fields:
            sql_parts.extend([
                "ORDER BY", self.order_by_fields, self.sort_order_type,
            ])

        return "%s;" % " ".join(sql_parts)

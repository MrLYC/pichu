from copy import deepcopy
from collections import namedtuple

from pichu.utils import chaining_method


class SQLValueError(Exception):
    pass


class SQLPartBuilder(object):

    def _as_sql(self):
        raise NotImplementedError()


class MergeableSQLPartBuilder(SQLPartBuilder):

    def _merge_from(self, part):
        if not part:
            return None
        raise NotImplementedError()


class AppendableSQLPartBuilder(SQLPartBuilder):

    def _append_from(self, part):
        if not part:
            return None
        raise NotImplementedError()


class BaseSQLBuilder(object):

    def __init__(self, model_meta):
        super(BaseSQLBuilder, self).__init__()
        self.model_meta = model_meta

    def copy(self):
        return deepcopy(self)

    def _build_sql(self):
        raise NotImplementedError()

    def _build_parameters(self):
        return tuple()


class WherePartSQLBuilderMixin(object):

    def __init__(self, *args, **kwargs):
        super(WherePartSQLBuilderMixin, self).__init__(*args, **kwargs)
        self.where_condition = None

    @chaining_method
    def where(self, condition):
        self.where_condition = condition._merge_from(self.where_condition)

    def _build_where_sql_parts(self, sql_parts):
        if self.where_condition:
            sql_parts.extend(["WHERE"])
            sql_parts.extend(self.where_condition._as_sql())

    def _build_where_sql_parameters(self):
        if self.where_condition:
            return self.where_condition._as_parameters()
        return tuple()


class SelectSQLBuilder(BaseSQLBuilder, WherePartSQLBuilderMixin):
    _SortOrderTypes = namedtuple("SortOrderTypes", [
        "ASC", "DESC", "DEFAULT",
    ])

    SortOrderTypes = _SortOrderTypes(
        DEFAULT="ASC", ASC="ASC", DESC="DESC",
    )

    def __init__(self, model_meta):
        super(SelectSQLBuilder, self).__init__(model_meta)
        self.limit_offset = None
        self.limit_count = None
        self.order_by_fields = None
        self.sort_order_type = None

    @chaining_method
    def limit(self, offset, count):
        self.limit_offset = offset
        self.limit_count = count

    @chaining_method
    def order_by(self, *fields):
        if self.sort_order_type is None:
            self.sort_order(self.SortOrderTypes.DEFAULT)

        self.order_by_fields = fields

    @chaining_method
    def sort_order(self, type_):
        if type_ not in self.SortOrderTypes:
            raise SQLValueError("%s not allowed" % type_)

        self.sort_order_type = type_

    def _build_sql(self):
        sql_parts = ["SELECT"]
        sql_parts.extend([
            f.column
            for f in self.model_meta.fields
        ])
        sql_parts.extend([
            "FROM", self.model_meta.table,
        ])

        self._build_where_sql_parts(sql_parts)

        if self.order_by_fields:
            sql_parts.extend([
                "ORDER BY", self.order_by_fields, self.sort_order_type,
            ])

        return "%s;" % " ".join(sql_parts)

    def _build_parameters(self):
        return self._build_where_sql_parameters()


class InsertSQLBuilder(BaseSQLBuilder):

    def __init__(self, model_meta):
        super(InsertSQLBuilder, self).__init__(model_meta)
        self.insert_value = None

    @chaining_method
    def insert(self, value):
        self.insert_value = value._append_from(self.insert_value)

    def _build_sql(self):
        if not self.insert_values:
            raise SQLValueError("insert value is empty")

        sql_parts = ["INSERT", "INTO", self.model_meta.table]
        sql_parts.extend([
            f.column
            for f in self.model_meta.fields
        ])
        sql_parts.extend(["VALUES", self.insert_value._as_sql()])
        return "%s;" % " ".join(sql_parts)

    def _build_parameters(self):
        return self.insert_value._as_parameters()


class UpdateSQLBuilder(BaseSQLBuilder, WherePartSQLBuilderMixin):

    def __init__(self, model_meta):
        super(UpdateSQLBuilder, self).__init__(model_meta)
        self.update_value = None

    @chaining_method
    def update(self, value):
        self.update_value = value._append_from(self.update_value)

    def _build_sql(self):
        if not self.update_value:
            raise SQLValueError("update value is empty")

        sql_parts = ["UPDATE", self.model_meta.table]
        sql_parts.extend(["SET", self.update_value._as_sql()])
        self._build_where_sql_parts(sql_parts)

    def _build_parameters(self):
        params = self.update_value._as_parameters()
        return params + self._build_where_sql_parameters()


class DeleteSQLBuilder(BaseSQLBuilder, WherePartSQLBuilderMixin):

    def _build_sql(self):
        sql_parts = ["DELETE", "FROM", self.model_meta.table]
        self._build_where_sql_parts(sql_parts)
        return sql_parts

    def _build_parameters(self):
        return self._build_where_sql_parameters()

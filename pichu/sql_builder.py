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
        return self


class ConditionExpSQLPartBuilder(MergeableSQLPartBuilder):

    def __init__(self, field, operator, value):
        super(ConditionExpSQLPartBuilder, self).__init__()
        self.field = field
        self.operator = operator
        self.value = value

    def _as_sql(self):
        return '("%s"%s?)' % (self.field, self.operator)

    def _as_parameters(self):
        return (self.value,)


class MultiConditionSQLPartBuilder(MergeableSQLPartBuilder):

    def __init__(self, right, operator, left=None):
        super(MultiConditionSQLPartBuilder, self).__init__()
        self.left = left
        self.operator = operator
        self.right = right

    def _merge_from(self, left):
        self.left = left
        return self

    def _as_sql(self):
        if isinstance(
            self.right,
            (ConditionExpSQLPartBuilder, MultiConditionSQLPartBuilder)
        ):
            right_exp = self.right._as_sql()
        else:
            right_exp = self.right

        if not self.left:
            return right_exp

        if isinstance(
            self.left,
            (ConditionExpSQLPartBuilder, MultiConditionSQLPartBuilder)
        ):
            left_exp = self.left._as_sql()
        else:
            left_exp = self.left

        return "(%s %s %s)" % (left_exp, self.operator, right_exp)

    def _as_parameters(self):
        params = self.right._as_parameters()
        if self.left:
            params = self.left._as_parameters() + params
        return params

    @classmethod
    def and_(cls, left, right):
        return cls(left=left, operator="and", right=right)

    @classmethod
    def or_(cls, left, right):
        return cls(left=left, operator="or", right=right)


class AppendableSQLPartBuilder(SQLPartBuilder):

    def _append_from(self, part):
        return self


class UpdateValueSQLPartBuilder(AppendableSQLPartBuilder):

    def __init__(self, model_meta, **kwargs):
        self.model_meta = model_meta
        self.keys = []
        self.values = []
        self.last = None

        for f in model_meta.fields:
            if f.attr not in kwargs:
                continue

            value = kwargs.pop(f.attr)

            self.keys.append(f.column)
            self.values.append(value)

        self.keys = tuple(self.keys)
        self.values = tuple(self.values)

        super(UpdateValueSQLPartBuilder, self).__init__()

    def _append_from(self, last):
        self.last = last
        return self

    def _as_sql(self):
        if self.last:
            values = self.last._as_sql()
        else:
            values = ()

        sql_parts = values + (
            "%s=?" % k
            for k in self.keys
        )
        return ", ".join(sql_parts)

    def _as_parameters(self):
        if self.last:
            values = self.last._as_parameters()
        else:
            values = ()
        return values + self.values


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

    def _parse_db_result(self, result):
        return result


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

    def _parse_db_result(self, result):
        values = {
            f.attr: v
            for f, v in zip(self.model_meta.fields, result)
        }
        return self.model_meta.model(**values)

    def _build_sql(self):
        sql_parts = ["SELECT"]
        sql_parts.append(",".join([
            '"%s"' % f.column
            for f in self.model_meta.fields
        ]))
        sql_parts.extend([
            "FROM", self.model_meta.table,
        ])

        self._build_where_sql_parts(sql_parts)

        if self.order_by_fields:
            sql_parts.extend([
                "ORDER BY", '"%s"' % self.order_by_fields,
                self.sort_order_type,
            ])

        return "%s;" % " ".join(sql_parts)

    def _build_parameters(self):
        return self._build_where_sql_parameters()


class InsertSQLBuilder(BaseSQLBuilder):

    def __init__(self, model_meta):
        super(InsertSQLBuilder, self).__init__(model_meta)
        self.insert_values = []

    @chaining_method
    def insert(self, **kwargs):
        values = []
        for f in self.model_meta.fields:
            if f.attr in kwargs:
                values.append(kwargs.pop(f.attr))
            elif hasattr(f, "default"):
                values.append(f.default)
            else:
                raise SQLValueError(f.attr)
        self.insert_values.append(values)

    def _build_sql(self):
        if not self.insert_values:
            raise SQLValueError("insert value is empty")

        sql_parts = ["INSERT", "INTO", self.model_meta.table]
        sql_parts.append("(%s)" % ",".join([
            '"%s"' % f.column
            for f in self.model_meta.fields
        ]))
        sql_parts.extend(["VALUES"])

        value_statement = "(%s)" % ",".join(
            "?" for i in self.model_meta.fields
        )
        sql_parts.append(",".join(value_statement for i in self.insert_values))
        return "%s;" % " ".join(sql_parts)

    def _build_parameters(self):
        params = []
        for i in self.insert_values:
            params.extend(i)
        return tuple(params)


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


class CreateTableSQLBuilder(BaseSQLBuilder):

    def _build_sql(self):
        sql_parts = [
            "CREATE TABLE IF NOT EXISTS", '"%s"' % self.model_meta.table
        ]
        field_parts = []
        for f in self.model_meta.fields:
            if f.is_primary_key:
                field = '"%s" %s %s' % (f.column, f.DBType, "PRIMARY KEY")
            else:
                field = '"%s" %s' % (f.column, f.DBType)
            field_parts.append(field)

        sql_parts.extend(["(", ",".join(field_parts), ")"])
        return "%s;" % " ".join(sql_parts)

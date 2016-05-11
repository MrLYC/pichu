from unittest import TestCase

from pichu import sql_builder
from .utils import TestModel


class TestMergeableSQLPartBuilder(TestCase):

    def test_usage(self):
        builder = sql_builder.MergeableSQLPartBuilder()
        self.assertIs(builder._merge_from(None), builder)


class TestConditionExpSQLPartBuilder(TestCase):

    def test_as_sql(self):
        builder = sql_builder.ConditionExpSQLPartBuilder("id", "=", "123")
        self.assertEqual(builder._as_sql(), "(\"id\"=?)")

    def test_as_parameters(self):
        builder = sql_builder.ConditionExpSQLPartBuilder("id", "=", 123)
        self.assertTupleEqual(builder._as_parameters(), (123,))


class TestMultiConditionSQLPartBuilder(TestCase):

    def test_usage(self):
        builder1 = sql_builder.MultiConditionSQLPartBuilder(
            sql_builder.ConditionExpSQLPartBuilder("name", "=", "test"),
            "and",
            sql_builder.ConditionExpSQLPartBuilder("id", "=", 123),
        )
        self.assertEqual(builder1._as_sql(), "((\"id\"=?) and (\"name\"=?))")
        self.assertTupleEqual(builder1._as_parameters(), (123, "test"))

        builder2 = sql_builder.MultiConditionSQLPartBuilder(
            right=sql_builder.ConditionExpSQLPartBuilder("value", "=", 1.0),
            operator="or",
        )
        builder2._merge_from(left=builder1)
        self.assertEqual(
            builder2._as_sql(),
            "(((\"id\"=?) and (\"name\"=?)) or (\"value\"=?))"
        )
        self.assertTupleEqual(builder2._as_parameters(), (123, "test", 1.0))

    def test_builtin_operator(self):
        base_build1 = sql_builder.ConditionExpSQLPartBuilder("id", "=", 123)
        base_build2 = sql_builder.ConditionExpSQLPartBuilder(
            "name", "=", "test"
        )

        builder1 = sql_builder.MultiConditionSQLPartBuilder.and_(
            base_build1, base_build2,
        )
        self.assertEqual(builder1._as_sql(), "((\"id\"=?) and (\"name\"=?))")
        self.assertTupleEqual(builder1._as_parameters(), (123, "test"))

        builder2 = sql_builder.MultiConditionSQLPartBuilder.or_(
            base_build1, base_build2,
        )
        self.assertEqual(builder2._as_sql(), "((\"id\"=?) or (\"name\"=?))")
        self.assertTupleEqual(builder1._as_parameters(), (123, "test"))

        builder3 = sql_builder.MultiConditionSQLPartBuilder.or_(
            left=builder1, right=builder2,
        )
        self.assertEqual(
            builder3._as_sql(),
            "(((\"id\"=?) and (\"name\"=?)) or ((\"id\"=?) or (\"name\"=?)))"
        )
        self.assertTupleEqual(
            builder3._as_parameters(), (123, "test", 123, "test")
        )


class TestInsertSQLBuilder(TestCase):

    def test_insert(self):
        builder = sql_builder.InsertSQLBuilder(TestModel.X)
        builder.insert(id=1, name="test1", value=1)
        builder.insert(id=2, name="test2")
        self.assertEqual(
            builder._build_sql(),
            'INSERT INTO %s ("id","name","value") VALUES (?,?,?),(?,?,?);' % (
                TestModel.X.table,
            )
        )
        self.assertTupleEqual(
            builder._build_parameters(),
            (1, "test1", 1, 2, "test2", TestModel.value.default),
        )


class TestUpdateSQLBuilder(TestCase):

    def test_update(self):
        builder = sql_builder.UpdateSQLBuilder(TestModel.X)
        builder.update(name="test", value=2)
        builder.where(sql_builder.MultiConditionSQLPartBuilder.and_(
            sql_builder.ConditionExpSQLPartBuilder("id", ">", 2),
            sql_builder.ConditionExpSQLPartBuilder("value", "=", 1),
        ))
        self.assertEqual(
            builder._build_sql(),
            (
                'UPDATE %s SET name=?,value=? '
                'WHERE (("id">?) and ("value"=?));'
            ) % TestModel.X.table
        )
        self.assertTupleEqual(
            builder._build_parameters(),
            ("test", 2, 2, 1)
        )


class TestDeleteSQLBuilder(TestCase):

    def tedt_delete(self):
        builder = sql_builder.DeleteSQLBuilder(TestModel.X)
        builder.where(sql_builder.ConditionExpSQLPartBuilder("id", "=", 1))
        self.assertEqual(
            builder._build_sql(),
            "DELETE FROM %s WHERE (id=?)" % TestModel.X.table
        )


class TestSelectSQLBuilder(TestCase):

    def test_select_all(self):
        builder = sql_builder.SelectSQLBuilder(TestModel.X)
        self.assertEqual(
            builder._build_sql(),
            'SELECT "id","name","value" FROM %s;' % TestModel.X.table
        )

    def test_where(self):
        builder = sql_builder.SelectSQLBuilder(TestModel.X)
        builder.where(sql_builder.ConditionExpSQLPartBuilder("id", "=", 1))
        self.assertEqual(
            builder._build_sql(),
            (
                'SELECT "id","name","value" FROM %s '
                'WHERE ("id"=?);'
            ) % TestModel.X.table
        )

    def test_limit(self):
        builder = sql_builder.SelectSQLBuilder(TestModel.X)
        builder.limit(5, 10)
        self.assertEqual(
            builder._build_sql(),
            (
                'SELECT "id","name","value" FROM %s '
                'LIMIT 5 OFFSET 10;'
            ) % TestModel.X.table
        )

    def test_order_by(self):
        builder = sql_builder.SelectSQLBuilder(TestModel.X)
        builder.order_by("id", "-name", "+value")
        self.assertEqual(
            builder._build_sql(),
            (
                'SELECT "id","name","value" FROM %s '
                'ORDER BY "id" ASC,"name" DESC,"value" ASC;'
            ) % TestModel.X.table
        )

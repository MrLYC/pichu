from unittest import TestCase

from pichu import sql_builder


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

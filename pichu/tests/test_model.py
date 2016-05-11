from unittest import TestCase

from pichu import model

from .utils import TestModel


class TestModelMetaAttr(TestCase):

    def test_meta_attrs(self):
        self.assertIsInstance(TestModel.X, model.ModelMetaAttrs)
        self.assertIs(TestModel.X.model, TestModel)
        self.assertTupleEqual(TestModel.X.fields, (
            TestModel.id, TestModel.name, TestModel.value,
        ))  # sorted by field column
        self.assertIs(TestModel.X.pk, TestModel.id)
        self.assertEqual(TestModel.X.table, TestModel.__table__)

from unittest import TestCase

from pichu import model


class ModelTestCase(TestCase):

    def setUp(self):
        self.GlobalModels = model.ModelMeta.GlobalModels
        model.ModelMeta.GlobalModels = {}

    def tearDown(self):
        model.ModelMeta.GlobalModels = self.GlobalModels


class TestModelMetaAttr(ModelTestCase):

    def test_meta_attrs(self):
        class TestModel(model.BaseModel):
            __table__ = "test_model"

            id = model.IntFieldType(is_primary_key=True)
            value = model.FloatFieldType()
            name = model.TextFieldType()

        self.assertIsInstance(TestModel.X, model.ModelMetaAttrs)
        self.assertIs(TestModel.X.model, TestModel)
        self.assertTupleEqual(TestModel.X.fields, (
            TestModel.id, TestModel.name, TestModel.value,
        ))  # sorted by field column
        self.assertIs(TestModel.X.pk, TestModel.id)
        self.assertEqual(TestModel.X.table, TestModel.__table__)

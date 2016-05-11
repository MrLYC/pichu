from pichu import model


class TestModel(model.BaseModel):
    __table__ = "test_model"

    id = model.IntFieldType(is_primary_key=True)
    value = model.FloatFieldType(default=0)
    name = model.TextFieldType()

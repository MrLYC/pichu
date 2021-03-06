import six
from copy import deepcopy
import operator


class MultiPrimaryKeyError(Exception):
    pass


class PrimaryKeyNotFound(Exception):
    pass


class ModelNameConflictError(Exception):
    pass


class ModelMetaAttrs(dict):

    def __init__(self, **kwargs):
        self.table = None
        self.fields = []
        self.model = None
        self.pk = None
        self.update(kwargs)

    def __setattr__(self, name, value):
        self[name] = value

    def __getattr__(self, name):
        return self.get(name)

    def copy(self):
        return deepcopy(self)


class BaseFieldType(object):

    def __init__(self, **kwargs):
        self.attr = None
        self.column = None
        self.is_primary_key = False

        for attr, value in kwargs.items():
            setattr(self, attr, value)

    def __str__(self):
        return "<{type} {self.attr} at {id}>".format(
            type=self.__class__.__name__, id=id(self),
            self=self
        )

    def to_database_value(self, value):
        raise NotImplementedError()

    def to_model_value(self, value):
        raise NotImplementedError()


class ModelMeta(type):
    GlobalModels = {}

    @staticmethod
    def make_fields_from_attrs(attrs):
        fields = []
        for attr, value in attrs.items():
            if isinstance(value, BaseFieldType):
                value.attr = attr
                if value.column is None:
                    value.column = attr

                fields.append(value)

        fields.sort(key=operator.attrgetter("column"))

        return tuple(fields)

    @staticmethod
    def get_model_table(cls, name, attrs):
        table_name = attrs.get("__table__")
        if table_name:
            return table_name

        parts = cls.__module__.split(".")
        parts.append(name)
        return "_".join(parts)

    @staticmethod
    def find_primary_key(fields):
        pk = None
        for f in fields:
            if f.is_primary_key:
                if pk is None:
                    pk = f
                else:
                    raise MultiPrimaryKeyError(
                        "primary key conflict: %s and %s" % (
                            pk.attr, f.attr
                        )
                    )
        return pk

    @staticmethod
    def setup_meta_attrs(cls, name, bases, attrs):
        fields = ModelMeta.make_fields_from_attrs(attrs)
        pk = ModelMeta.find_primary_key(fields)
        table = ModelMeta.get_model_table(cls, name, attrs)

        attrs["X"] = ModelMetaAttrs(
            fields=fields, table=table, pk=pk,
        )

    @staticmethod
    def register_model(model):
        name = model.X.table
        if name in ModelMeta.GlobalModels:
            raise ModelNameConflictError("%s" % name)
        ModelMeta.GlobalModels[name] = model
        model.X.model = model

    def __new__(cls, name, bases, attrs):
        ModelMeta.setup_meta_attrs(cls, name, bases, attrs)

        model = type.__new__(cls, name, bases, attrs)
        ModelMeta.register_model(model)
        return model


class BaseModel(six.with_metaclass(ModelMeta, object)):
    X = ModelMetaAttrs()

    def __init__(self, **kwargs):
        self._set_model_value(**kwargs)

    def _set_model_value(self, **kwargs):
        for f in self.X.fields:
            if f.attr in kwargs:
                value = kwargs.pop(f.attr)
            elif hasattr(f, "default"):
                value = f.default
            setattr(self, f.attr, f.to_model_value(value))

    @property
    def pk(self):
        pk_field = self.X.pk
        if pk_field is None:
            raise PrimaryKeyNotFound(
                "primary key field not found in table %s" % self.X.table
            )
        return getattr(self, pk_field.attr)


class SimpleTypeFieldMixin(object):
    ValueConvertor = (lambda x: x)

    def __init__(self, *args, **kwargs):
        if "default" in kwargs:
            value = kwargs.pop("default")
            kwargs["default"] = self.to_model_value(value)
        super(SimpleTypeFieldMixin, self).__init__(*args, **kwargs)

    def to_database_value(self, value):
        return self.ValueConvertor(value)

    def to_model_value(self, value):
        return self.ValueConvertor(value)


class IntFieldType(SimpleTypeFieldMixin, BaseFieldType):
    DBType = "INT"
    ValueConvertor = int


class FloatFieldType(SimpleTypeFieldMixin, BaseFieldType):
    DBType = "DOUBLE"
    ValueConvertor = float


class TextFieldType(BaseFieldType):
    DBType = "TEXT"

    def __init__(self, **kwargs):
        self.encoding = "utf-8"
        super(TextFieldType, self).__init__(**kwargs)

    def to_database_value(self, value):
        if isinstance(value, unicode):
            return value.encode(self.encoding)
        return value

    def to_model_value(self, value):
        if isinstance(value, bytes):
            return value.decode(self.encoding)
        return value

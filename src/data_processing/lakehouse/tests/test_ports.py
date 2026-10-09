from lakehouse.application.ports import ObjectSink, RecordEncoder
from lakehouse.infrastructure.sinks import Boto3Sink, InMemorySink
from lakehouse.tests.fakes import FakeEncoder


def test_in_memory_sink_satisfies_object_sink_port():
    assert isinstance(InMemorySink(), ObjectSink)


def test_boto3_sink_class_exposes_object_sink_methods():
    for name in ("put", "get", "exists", "copy", "delete", "list_keys"):
        assert callable(getattr(Boto3Sink, name))


def test_fake_encoder_satisfies_record_encoder_port():
    assert isinstance(FakeEncoder(), RecordEncoder)

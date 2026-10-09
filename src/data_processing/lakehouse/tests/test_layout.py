from datetime import date

from lakehouse.application.layout import BronzeLayout


def test_partition_and_data_keys():
    layout = BronzeLayout("/bronze/orders/")
    assert layout.dataset == "bronze/orders"
    assert layout.partition(date(2026, 1, 2)) == "bronze/orders/2026/01/02"
    assert (
        layout.data_key(date(2026, 1, 2), "abc")
        == "bronze/orders/2026/01/02/part-abc.parquet"
    )
    assert layout.staging_key("abc") == "bronze/orders/_staging/abc.parquet"


def test_manifest_and_success_keys():
    layout = BronzeLayout("bronze/orders")
    event_date = date(2026, 10, 9)
    assert (
        layout.manifest_key(event_date)
        == "bronze/orders/2026/10/09/_manifest.json"
    )
    assert (
        layout.manifest_temp_key(event_date)
        == "bronze/orders/2026/10/09/_manifest.json.tmp"
    )
    assert layout.success_key(event_date) == "bronze/orders/2026/10/09/_SUCCESS"

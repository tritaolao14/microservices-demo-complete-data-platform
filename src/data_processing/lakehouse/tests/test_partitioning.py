from datetime import date

from lakehouse.domain.partitioning import infer_event_date, partition_path


def test_infer_event_date_from_z_timestamp():
    payload = {"timestamp": "2026-10-09T23:30:00Z"}
    assert infer_event_date(payload, date(2000, 1, 1)) == date(2026, 10, 9)


def test_infer_event_date_from_offset_and_naive():
    assert infer_event_date(
        {"timestamp": "2026-10-09T23:30:00+07:00"}, date(2000, 1, 1)
    ) == date(2026, 10, 9)
    assert infer_event_date(
        {"event_timestamp": "2026-10-09T01:00:00"}, date(2000, 1, 1)
    ) == date(2026, 10, 9)


def test_infer_event_date_normalises_offset_to_utc():
    assert infer_event_date(
        {"timestamp": "2026-10-10T01:00:00+07:00"}, date(2000, 1, 1)
    ) == date(2026, 10, 9)


def test_infer_event_date_prefers_metadata():
    payload = {"_event_date": "2026-01-02", "timestamp": "2026-10-09T00:00:00Z"}
    assert infer_event_date(payload, date(2000, 1, 1)) == date(2026, 1, 2)


def test_infer_event_date_falls_back_to_default():
    assert infer_event_date({}, date(2020, 5, 6)) == date(2020, 5, 6)
    assert infer_event_date({"timestamp": "not-a-date"}, date(2020, 5, 6)) == date(
        2020, 5, 6
    )


def test_partition_path_zero_pads():
    assert partition_path("bronze/orders", date(2026, 1, 2)) == "bronze/orders/2026/01/02"
    assert (
        partition_path("/bronze/orders/", date(2026, 12, 31))
        == "bronze/orders/2026/12/31"
    )

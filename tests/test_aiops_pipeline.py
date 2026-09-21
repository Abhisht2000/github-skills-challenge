import runpy
from pathlib import Path

from src.aiops_pipeline import load_data, run_pipeline
from src.anomaly_detector import AnomalyDetector
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_warning_log_is_detected_as_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:04:00",
        "service": "api-service",
        "response_time_ms": 120,
        "cpu_percent": 45,
        "memory_percent": 50,
        "log_level": "WARNING",
        "message": "Warning: retrying downstream request"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["reasons"] == ["Error log detected"]


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_load_data_reads_service_json():
    data_file = Path(__file__).resolve().parents[1] / "data" / "service_data.json"

    data = load_data(data_file)

    assert isinstance(data, list)
    assert data[0]["service"] == "payment-service"
    assert len(data) == 10


def test_run_pipeline_returns_processed_anomalies_and_consumed_events():
    data_file = Path(__file__).resolve().parents[1] / "data" / "service_data.json"

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert len(result["events_consumed"]) == 2
    assert all(event["type"] == "ANOMALY" for event in result["events_consumed"])


def test_run_pipeline_main_entrypoint_prints_summary(capsys):
    runpy.run_module("src.aiops_pipeline", run_name="__main__")

    captured = capsys.readouterr()
    assert "AIOps Pipeline Result" in captured.out
    assert "Records processed: 10" in captured.out
    assert "Anomalies detected: 2" in captured.out


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1
    assert messages[0]["service"] == "payment-service"
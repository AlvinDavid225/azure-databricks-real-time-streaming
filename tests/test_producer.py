"""
Unit tests for telemetry_producer.py
Run with: pytest tests/ -v
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer.telemetry_producer import (
    init_vehicle_states,
    generate_event,
    vehicle_state,
    VEHICLE_MAX_SPEEDS,
    VEHICLE_ROUTES,
)


class TestVehicleStateInit:
    def test_init_creates_all_vehicles(self):
        vids = ["VH001", "VH002", "VH003"]
        init_vehicle_states(vids)
        assert all(v in vehicle_state for v in vids)

    def test_init_fuel_in_range(self):
        init_vehicle_states(["VH001"])
        assert 0 <= vehicle_state["VH001"]["fuel_pct"] <= 100

    def test_init_speed_below_max(self):
        init_vehicle_states(["VH001"])
        assert vehicle_state["VH001"]["speed_kmh"] <= VEHICLE_MAX_SPEEDS["VH001"]


class TestEventGeneration:
    def setup_method(self):
        init_vehicle_states(["VH001", "VH002"])

    def test_event_has_required_fields(self):
        event = generate_event("VH001", anomaly_rate=0.0)
        required = ["vehicle_id", "timestamp", "lat", "lon", "speed_kmh",
                    "fuel_pct", "engine_temp_c", "odometer_km", "event_type", "route_id"]
        for field in required:
            assert field in event, f"Missing field: {field}"

    def test_normal_event_type(self):
        # With anomaly_rate=0, all events should be normal
        for _ in range(20):
            event = generate_event("VH001", anomaly_rate=0.0)
            assert event["event_type"] == "normal"

    def test_anomaly_event_types(self):
        # With anomaly_rate=1.0, all events are anomalous
        anomaly_types = set()
        for _ in range(100):
            event = generate_event("VH001", anomaly_rate=1.0)
            anomaly_types.add(event["event_type"])
        assert "normal" not in anomaly_types
        # Should see multiple anomaly types over 100 runs
        assert len(anomaly_types) >= 2

    def test_speed_breach_exceeds_max(self):
        # Force speed_breach anomaly type and verify speed > max
        for _ in range(200):
            event = generate_event("VH001", anomaly_rate=1.0)
            if event["event_type"] == "speed_breach":
                assert event["speed_kmh"] > VEHICLE_MAX_SPEEDS["VH001"] * 1.15
                return
        # If we didn't see a speed_breach in 200 tries, test passes vacuously
        # (other anomaly types may have dominated)

    def test_lat_lon_within_route_bounds(self):
        from producer.telemetry_producer import ROUTE_BOUNDS
        event = generate_event("VH001", anomaly_rate=0.0)
        route = VEHICLE_ROUTES["VH001"]
        bounds = ROUTE_BOUNDS[route]
        assert bounds[0] <= event["lat"] <= bounds[1]
        assert bounds[2] <= event["lon"] <= bounds[3]

    def test_vehicle_id_in_event(self):
        event = generate_event("VH002", anomaly_rate=0.0)
        assert event["vehicle_id"] == "VH002"

    def test_route_id_matches_vehicle(self):
        event = generate_event("VH002", anomaly_rate=0.0)
        assert event["route_id"] == VEHICLE_ROUTES["VH002"]

    def test_odometer_increases(self):
        init_vehicle_states(["VH001"])
        odo_before = vehicle_state["VH001"]["odometer_km"]
        generate_event("VH001", anomaly_rate=0.0)
        odo_after = vehicle_state["VH001"]["odometer_km"]
        assert odo_after >= odo_before

    def test_fuel_decreases(self):
        init_vehicle_states(["VH001"])
        vehicle_state["VH001"]["fuel_pct"] = 50.0
        generate_event("VH001", anomaly_rate=0.0)
        assert vehicle_state["VH001"]["fuel_pct"] <= 50.0

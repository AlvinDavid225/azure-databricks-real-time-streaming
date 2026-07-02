"""
P2 Fleet Operations — Telemetry Producer
=========================================
Simulates GPS/IoT telemetry from a fleet of 10 delivery vehicles.
Publishes JSON events to Azure Event Hub using the Kafka-compatible API.

Each event contains:
    vehicle_id, timestamp, lat, lon, speed_kmh,
    fuel_pct, engine_temp_c, odometer_km, event_type

Anomaly injection (configurable rate) covers:
    - speed_breach: speed > vehicle max_speed * 1.15
    - engine_overheat: engine_temp_c > 110
    - fuel_critical: fuel_pct < 10

Usage:
    pip install confluent-kafka python-dotenv
    python producer/telemetry_producer.py --rate 50 --vehicles 10 --duration 300
"""

import argparse
import json
import math
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv("config/.env")

# ── Route bounding boxes (lat_min, lat_max, lon_min, lon_max) ──────────────
ROUTE_BOUNDS = {
    "R101": (12.30, 12.97, 76.65, 77.59),   # Bangalore-Mysore
    "R102": (12.67, 13.08, 77.59, 80.27),   # Bangalore-Chennai
    "R103": (12.97, 17.38, 77.59, 78.47),   # Bangalore-Hyderabad
    "R104": (12.97, 18.52, 73.85, 77.59),   # Bangalore-Pune
    "R105": (12.52, 12.97, 74.85, 77.59),   # Bangalore-Mangalore
}

VEHICLE_ROUTES = {
    "VH001": "R101", "VH002": "R102", "VH003": "R101",
    "VH004": "R103", "VH005": "R104", "VH006": "R102",
    "VH007": "R103", "VH008": "R105", "VH009": "R104",
    "VH010": "R105",
}

VEHICLE_MAX_SPEEDS = {
    "VH001": 80,  "VH002": 100, "VH003": 80,
    "VH004": 75,  "VH005": 100, "VH006": 110,
    "VH007": 80,  "VH008": 75,  "VH009": 110,
    "VH010": 100,
}

# Vehicle runtime state (mutable per-vehicle simulation state)
vehicle_state: dict[str, dict] = {}


def init_vehicle_states(vehicle_ids: list[str]) -> None:
    """Initialize mutable state for each simulated vehicle."""
    for vid in vehicle_ids:
        route = VEHICLE_ROUTES[vid]
        bounds = ROUTE_BOUNDS[route]
        vehicle_state[vid] = {
            "lat": random.uniform(bounds[0], bounds[1]),
            "lon": random.uniform(bounds[2], bounds[3]),
            "speed_kmh": random.uniform(40, VEHICLE_MAX_SPEEDS[vid] * 0.9),
            "fuel_pct": random.uniform(40, 90),
            "engine_temp_c": random.uniform(75, 90),
            "odometer_km": random.uniform(1000, 50000),
        }


def next_position(vid: str) -> tuple[float, float]:
    """Advance vehicle position along its route with small random walk."""
    state = vehicle_state[vid]
    route = VEHICLE_ROUTES[vid]
    bounds = ROUTE_BOUNDS[route]
    
    # Small jitter simulating GPS noise + road curvature
    delta_lat = random.uniform(-0.005, 0.005)
    delta_lon = random.uniform(-0.005, 0.008)  # slight eastward drift on most routes
    
    new_lat = max(bounds[0], min(bounds[1], state["lat"] + delta_lat))
    new_lon = max(bounds[2], min(bounds[3], state["lon"] + delta_lon))
    
    state["lat"] = new_lat
    state["lon"] = new_lon
    return new_lat, new_lon


def generate_event(vid: str, anomaly_rate: float) -> dict:
    """Generate a single telemetry event, with optional anomaly injection."""
    state = vehicle_state[vid]
    lat, lon = next_position(vid)
    
    max_speed = VEHICLE_MAX_SPEEDS[vid]
    inject_anomaly = random.random() < anomaly_rate
    
    # ── Normal drift ───────────────────────────────────────────────────────
    speed = state["speed_kmh"] + random.uniform(-5, 5)
    speed = max(0, min(speed, max_speed * 1.0))
    
    fuel = state["fuel_pct"] - random.uniform(0, 0.05)   # slowly draining
    fuel = max(0, fuel)
    
    engine_temp = state["engine_temp_c"] + random.uniform(-1, 1)
    engine_temp = max(60, min(engine_temp, 95))
    
    odometer = state["odometer_km"] + (speed / 3600)     # km per second
    event_type = "normal"
    
    # ── Anomaly injection ─────────────────────────────────────────────────
    if inject_anomaly:
        anomaly_kind = random.choices(
            ["speed_breach", "engine_overheat", "fuel_critical", "route_deviation"],
            weights=[0.40, 0.30, 0.20, 0.10],
        )[0]
        
        if anomaly_kind == "speed_breach":
            speed = max_speed * random.uniform(1.16, 1.40)
            event_type = "speed_breach"
        elif anomaly_kind == "engine_overheat":
            engine_temp = random.uniform(110, 130)
            event_type = "engine_overheat"
        elif anomaly_kind == "fuel_critical":
            fuel = random.uniform(2, 9)
            event_type = "fuel_critical"
        elif anomaly_kind == "route_deviation":
            # Push lat/lon outside normal route bounds
            bounds = ROUTE_BOUNDS[VEHICLE_ROUTES[vid]]
            lat = bounds[1] + random.uniform(0.1, 0.5)
            event_type = "route_deviation"
    
    # ── Persist state changes ─────────────────────────────────────────────
    state["speed_kmh"] = speed
    state["fuel_pct"] = fuel
    state["engine_temp_c"] = engine_temp
    state["odometer_km"] = odometer
    
    return {
        "vehicle_id": vid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "speed_kmh": round(speed, 2),
        "fuel_pct": round(fuel, 2),
        "engine_temp_c": round(engine_temp, 2),
        "odometer_km": round(odometer, 1),
        "event_type": event_type,
        "route_id": VEHICLE_ROUTES[vid],
    }


def build_kafka_config() -> dict:
    """
    Build Kafka producer config for Azure Event Hub Kafka endpoint.
    
    Event Hub Kafka endpoint format:
        bootstrap.servers = <namespace>.servicebus.windows.net:9093
        security.protocol = SASL_SSL
        sasl.mechanism    = PLAIN
        sasl.username     = $ConnectionString
        sasl.password     = <connection_string>
    """
    conn_str = os.environ["EVENT_HUB_CONNECTION_STRING"]
    namespace = os.environ["EVENT_HUB_NAMESPACE"]
    
    return {
        "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": conn_str,
        "client.id": "fleet-telemetry-producer",
        # Reliability settings
        "acks": "1",                    # Leader ack — balance reliability vs speed
        "linger.ms": "5",              # Micro-batching: collect events for 5ms
        "batch.size": "65536",         # 64KB batch before flush
        "compression.type": "none",     # Compress for high-throughput
        "retries": "3",
        "retry.backoff.ms": "500",
    }


def delivery_report(err, msg):
    """Kafka delivery callback — logs failures only to reduce noise."""
    if err:
        print(f"[PRODUCER ERROR] Delivery failed for {msg.key()}: {err}", file=sys.stderr)


def run_producer(
    events_per_second: int,
    vehicle_count: int,
    anomaly_rate: float,
    duration_seconds: Optional[int],
    dry_run: bool,
) -> None:
    """Main producer loop."""
    vehicle_ids = [f"VH{str(i).zfill(3)}" for i in range(1, vehicle_count + 1)]
    init_vehicle_states(vehicle_ids)
    
    topic = os.environ.get("EVENT_HUB_NAME", "fleet-telemetry")
    interval = 1.0 / events_per_second          # seconds between events
    
    if dry_run:
        print("[DRY RUN] Printing events to stdout — no Kafka connection")
        producer = None
    else:
        config = build_kafka_config()
        producer = Producer(config)
        print(f"[PRODUCER] Connected → topic: {topic}")
    
    # ── Graceful shutdown on Ctrl+C ────────────────────────────────────────
    running = True
    def _signal_handler(sig, frame):
        nonlocal running
        print("\n[PRODUCER] Shutdown signal received — draining buffer...")
        running = False
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    
    start_time = time.time()
    sent = 0
    errors = 0
    
    print(f"[PRODUCER] Starting | vehicles={vehicle_count} | rate={events_per_second}/s | anomaly_rate={anomaly_rate:.0%}")
    print(f"[PRODUCER] Duration: {'unlimited' if duration_seconds is None else f'{duration_seconds}s'}")
    print("-" * 60)
    
    while running:
        if duration_seconds and (time.time() - start_time) >= duration_seconds:
            print(f"[PRODUCER] Duration limit reached ({duration_seconds}s)")
            break
        
        tick_start = time.time()
        
        # Round-robin across vehicles for this tick
        for vid in vehicle_ids:
            event = generate_event(vid, anomaly_rate)
            payload = json.dumps(event).encode("utf-8")
            
            if dry_run:
                print(json.dumps(event, indent=2))
            else:
                try:
                    producer.produce(
                        topic=topic,
                        key=vid.encode("utf-8"),   # partition by vehicle_id
                        value=payload,
                        callback=delivery_report,
                    )
                    sent += 1
                except Exception as e:
                    errors += 1
                    print(f"[ERROR] {vid}: {e}", file=sys.stderr)
            
            if not running:
                break
        
        # Progress log every 10 seconds
        elapsed = time.time() - start_time
        if sent % (events_per_second * 10) == 0 and sent > 0:
            actual_rate = sent / elapsed if elapsed > 0 else 0
            print(f"[PRODUCER] Sent={sent:,} | Errors={errors} | Rate={actual_rate:.1f}/s | Elapsed={elapsed:.0f}s")
        
        if producer:
            producer.poll(0)   # trigger delivery callbacks non-blocking
        
        # Throttle to target rate
        tick_duration = time.time() - tick_start
        sleep_time = interval - tick_duration
        if sleep_time > 0:
            time.sleep(sleep_time)
    
    # ── Flush remaining messages ───────────────────────────────────────────
    if producer:
        print("[PRODUCER] Flushing remaining messages...")
        producer.flush(timeout=30)
    
    elapsed = time.time() - start_time
    print(f"\n[PRODUCER] Done. Sent={sent:,} | Errors={errors} | Duration={elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fleet Telemetry Producer → Azure Event Hub")
    parser.add_argument("--rate",      type=int,   default=50,    help="Events per second total")
    parser.add_argument("--vehicles",  type=int,   default=10,    help="Number of vehicles to simulate")
    parser.add_argument("--anomaly",   type=float, default=0.05,  help="Fraction of anomalous events (0.0–1.0)")
    parser.add_argument("--duration",  type=int,   default=None,  help="Run for N seconds then stop (default: unlimited)")
    parser.add_argument("--dry-run",   action="store_true",       help="Print to stdout without Kafka connection")
    args = parser.parse_args()
    
    run_producer(
        events_per_second=args.rate,
        vehicle_count=args.vehicles,
        anomaly_rate=args.anomaly,
        duration_seconds=args.duration,
        dry_run=args.dry_run,
    )

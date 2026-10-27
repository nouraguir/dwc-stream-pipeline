import json, random, sys, time
from datetime import datetime, timezone
from confluent_kafka import Producer

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 42
SYSTEMS = ["dwc-1", "dwc-2"]
# sensor: (mean, noise, unit). Typical values, verify them in your research phase.
SENSORS = {
    "ph":         (6.0, 0.05, "pH"),
    "ec":         (1.6, 0.03, "mS/cm"),
    "water_temp": (20.0, 0.2, "C"),
    "air_temp":   (24.0, 0.3, "C"),
    "humidity":   (60.0, 1.0, "%"),
    "co2":        (800.0, 15.0, "ppm"),
    "orp":        (350.0, 5.0, "mV"),
    "ppfd":       (400.0, 10.0, "umol/m2/s"),
}

rng = random.Random(SEED)
producer = Producer({"bootstrap.servers": "localhost:9092"})
truth = open(f"data/injected_faults_seed{SEED}.jsonl", "w")
seq = 0
dead_until = {}   # (system, sensor) -> timestamp until which the sensor is silent

def log_fault(ts, system, sensor, kind):
    truth.write(json.dumps({"ts": ts, "system": system,
                            "sensor": sensor, "kind": kind}) + "\n")
    truth.flush()

def reading(system, sensor, now):
    mean, noise, _ = SENSORS[sensor]
    value = rng.gauss(mean, noise)
    r = rng.random()
    if r < 0.01:                       # implausible value
        value = rng.choice([-999.0, 9999.0])
        log_fault(now, system, sensor, "implausible")
    elif r < 0.02:                     # outlier
        value = mean + rng.choice([-1, 1]) * noise * 12
        log_fault(now, system, sensor, "outlier")
    elif r < 0.0203:                   # sensor failure for 40 s
        dead_until[(system, sensor)] = now + 40
        log_fault(now, system, sensor, "dropout")
    return value
interval, nxt = 1.0, time.monotonic()
try:
    while True:
        now = time.time()
        for system in SYSTEMS:
            for sensor, (_, _, unit) in SENSORS.items():
                if dead_until.get((system, sensor), 0) > now:
                    continue
                seq += 1
                msg = {"system_id": system, "sensor": sensor,
                       "value": round(reading(system, sensor, now), 3),
                       "unit": unit, "seq": seq,
                       "ts": datetime.now(timezone.utc).isoformat()}
                producer.produce("sensor-raw", key=system, value=json.dumps(msg))
        producer.poll(0)
        nxt += interval
        time.sleep(max(0, nxt - time.monotonic()))
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    truth.close()
    print("Simulator stopped.")
"""Synthetic UPI payment simulator with injected fraud patterns.

The simulator keeps a population of users (home city, usual devices, typical
spend) and merchants, and emits a time-ordered stream of payment events.
A small fraction of events are fraud, injected as one of three patterns:

- velocity_burst:        many small payments to new payees within ~1 minute
- new_device_high_value: a never-seen device makes a payment many times the usual amount
- geo_jump:              a payment from a city >1000 km away minutes after the previous one

Every event carries ground-truth labels (label_is_fraud, label_pattern). The
scorer never looks at them; they exist so we can measure precision and recall
and train the ML model in phase 3.
"""

from __future__ import annotations

import heapq
import itertools
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

CITIES: dict[str, tuple[float, float]] = {
    "Mumbai": (19.076, 72.8777),
    "Delhi": (28.7041, 77.1025),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.385, 78.4867),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
    "Pune": (18.5204, 73.8567),
    "Ahmedabad": (23.0225, 72.5714),
    "Jaipur": (26.9124, 75.7873),
    "Lucknow": (26.8467, 80.9462),
    "Kochi": (9.9312, 76.2673),
    "Guwahati": (26.1445, 91.7362),
}

# (category, typical amount multiplier, share of normal traffic)
MERCHANT_CATEGORIES: list[tuple[str, float, float]] = [
    ("grocery", 1.0, 0.25),
    ("food_delivery", 0.8, 0.18),
    ("recharge", 0.5, 0.10),
    ("utilities", 2.0, 0.08),
    ("fuel", 1.5, 0.10),
    ("travel", 4.0, 0.05),
    ("electronics", 6.0, 0.04),
    ("gaming", 0.7, 0.05),
    ("jewellery", 8.0, 0.02),
    ("p2p", 1.2, 0.13),
]

BANK_HANDLES = ["okaxis", "oksbi", "okhdfcbank", "okicici", "ybl", "paytm", "ibl"]
DEVICE_OS = ["android", "ios"]
FRAUD_PATTERNS = ("velocity_burst", "new_device_high_value", "geo_jump")


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


@dataclass
class User:
    user_id: str
    vpa: str
    home_city: str
    typical_amount: float
    devices: list[tuple[str, str]]  # (device_id, os)
    ip_prefix: str


@dataclass
class Merchant:
    merchant_id: str
    vpa: str
    category: str
    city: str


@dataclass(order=True)
class _Scheduled:
    ts_ms: int
    seq: int
    event: dict = field(compare=False)


class PaymentSimulator:
    def __init__(
        self,
        num_users: int = 500,
        num_merchants: int = 120,
        fraud_rate: float = 0.01,
        seed: int | None = 42,
        start_ts_ms: int | None = None,
        population_seed: int = 2026,
    ) -> None:
        # The population (users, devices, home cities) is fixed across runs so that
        # profiles the scorer remembers from yesterday still match today's users.
        self.rng = random.Random(population_seed)
        self.fraud_rate = fraud_rate
        self.now_ms = start_ts_ms if start_ts_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
        self._seq = itertools.count()
        self._queue: list[_Scheduled] = []
        self._device_counter = itertools.count(1)
        self.users = [self._make_user(i) for i in range(num_users)]
        self.merchants = [self._make_merchant(i) for i in range(num_merchants)]
        self.rng = random.Random(seed)

    def _new_device(self) -> tuple[str, str]:
        return (f"dev-{next(self._device_counter):07d}", self.rng.choice(DEVICE_OS))

    def _make_user(self, i: int) -> User:
        home = self.rng.choice(list(CITIES))
        n_devices = 1 if self.rng.random() < 0.8 else 2
        return User(
            user_id=f"U{i:05d}",
            vpa=f"user{i:05d}@{self.rng.choice(BANK_HANDLES)}",
            home_city=home,
            typical_amount=round(self.rng.lognormvariate(math.log(450), 0.7), 2),
            devices=[self._new_device() for _ in range(n_devices)],
            ip_prefix=f"{self.rng.randint(49, 223)}.{self.rng.randint(0, 255)}",
        )

    def _make_merchant(self, i: int) -> Merchant:
        categories = [c for c, _, _ in MERCHANT_CATEGORIES if c != "p2p"]
        weights = [w for c, _, w in MERCHANT_CATEGORIES if c != "p2p"]
        category = self.rng.choices(categories, weights=weights)[0]
        return Merchant(
            merchant_id=f"M{i:04d}",
            vpa=f"merchant{i:04d}@{self.rng.choice(BANK_HANDLES)}",
            category=category,
            city=self.rng.choice(list(CITIES)),
        )

    def _event(
        self,
        user: User,
        ts_ms: int,
        amount: float,
        category: str,
        device: tuple[str, str],
        city: str,
        channel: str,
        payee: str | None = None,
        merchant_id: str | None = None,
        is_fraud: bool = False,
        pattern: str = "none",
    ) -> dict:
        if category == "p2p":
            payee_type = "P2P"
            if payee is None:
                other = self.rng.choice(self.users)
                payee = other.vpa if other is not user else f"friend{self.rng.randint(0, 99999):05d}@ybl"
        else:
            payee_type = "MERCHANT"
            if payee is None:
                candidates = [m for m in self.merchants if m.category == category] or self.merchants
                merchant = self.rng.choice(candidates)
                payee, merchant_id = merchant.vpa, merchant.merchant_id
        lat, lon = CITIES[city]
        return {
            "txn_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "event_time": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "event_ts_ms": ts_ms,
            "payer_id": user.user_id,
            "payer_vpa": user.vpa,
            "payee_vpa": payee,
            "payee_type": payee_type,
            "merchant_id": merchant_id,
            "merchant_category": category,
            "amount": round(max(amount, 1.0), 2),
            "currency": "INR",
            "channel": channel,
            "device_id": device[0],
            "device_os": device[1],
            "ip_address": f"{user.ip_prefix}.{self.rng.randint(0, 255)}.{self.rng.randint(1, 254)}",
            "city": city,
            "lat": round(lat + self.rng.uniform(-0.05, 0.05), 5),
            "lon": round(lon + self.rng.uniform(-0.05, 0.05), 5),
            "label_is_fraud": is_fraud,
            "label_pattern": pattern,
        }

    def _schedule(self, event: dict) -> None:
        heapq.heappush(self._queue, _Scheduled(event["event_ts_ms"], next(self._seq), event))

    def _schedule_normal(self, ts_ms: int) -> None:
        user = self.rng.choice(self.users)
        cats = [c for c, _, _ in MERCHANT_CATEGORIES]
        weights = [w for _, _, w in MERCHANT_CATEGORIES]
        category = self.rng.choices(cats, weights=weights)[0]
        multiplier = dict((c, m) for c, m, _ in MERCHANT_CATEGORIES)[category]
        amount = user.typical_amount * multiplier * self.rng.lognormvariate(0, 0.5)
        device = self.rng.choice(user.devices)
        # Legitimate noise so the model has to work: occasional new phones and big purchases.
        if self.rng.random() < 0.004:
            device = self._new_device()
            user.devices.append(device)
        if self.rng.random() < 0.01:
            amount *= self.rng.uniform(4, 10)
        channel = "UPI_QR" if category != "p2p" and self.rng.random() < 0.6 else "UPI_INTENT"
        if category == "p2p" and self.rng.random() < 0.1:
            channel = "UPI_COLLECT"
        self._schedule(self._event(user, ts_ms, amount, category, device, user.home_city, channel))

    def _schedule_fraud(self, ts_ms: int) -> None:
        user = self.rng.choice(self.users)
        pattern = self.rng.choice(FRAUD_PATTERNS)
        if pattern == "velocity_burst":
            device = self.rng.choice(user.devices)
            t = ts_ms
            for _ in range(self.rng.randint(6, 12)):
                t += self.rng.randint(2_000, 9_000)
                self._schedule(
                    self._event(
                        user, t, self.rng.uniform(500, 4_999), "p2p", device, user.home_city,
                        self.rng.choice(["UPI_COLLECT", "UPI_INTENT"]),
                        payee=f"mule{self.rng.randint(0, 9999):04d}@{self.rng.choice(BANK_HANDLES)}",
                        is_fraud=True, pattern=pattern,
                    )
                )
        elif pattern == "new_device_high_value":
            device = (f"dev-x{self.rng.getrandbits(32):08x}", self.rng.choice(DEVICE_OS))
            amount = max(user.typical_amount * self.rng.uniform(15, 60), 20_000)
            self._schedule(
                self._event(
                    user, ts_ms, amount, self.rng.choice(["electronics", "jewellery"]), device,
                    user.home_city, "UPI_INTENT", is_fraud=True, pattern=pattern,
                )
            )
        else:  # geo_jump
            home = CITIES[user.home_city]
            far = [c for c, loc in CITIES.items() if haversine_km(home, loc) > 1000] or [c for c in CITIES if c != user.home_city]
            # A normal payment at home, then one from far away a few minutes later.
            self._schedule(
                self._event(user, ts_ms, user.typical_amount, "grocery", self.rng.choice(user.devices), user.home_city, "UPI_QR")
            )
            jump_ts = ts_ms + self.rng.randint(2, 30) * 60_000
            self._schedule(
                self._event(
                    user, jump_ts, user.typical_amount * self.rng.uniform(3, 10),
                    self.rng.choice(["electronics", "travel", "p2p"]), self.rng.choice(user.devices),
                    self.rng.choice(far), "UPI_INTENT", is_fraud=True, pattern=pattern,
                )
            )

    def stream(self, events_per_second: float = 5.0) -> Iterator[dict]:
        """Yield events forever in timestamp order, on the simulator's own clock."""
        mean_gap_ms = 1000.0 / events_per_second
        while True:
            self.now_ms += max(1, int(self.rng.expovariate(1.0 / mean_gap_ms)))
            if self.rng.random() < self.fraud_rate:
                self._schedule_fraud(self.now_ms)
            else:
                self._schedule_normal(self.now_ms)
            while self._queue and self._queue[0].ts_ms <= self.now_ms:
                yield heapq.heappop(self._queue).event

    def generate(self, count: int, events_per_second: float = 5.0) -> list[dict]:
        return list(itertools.islice(self.stream(events_per_second), count))

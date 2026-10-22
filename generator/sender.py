"""Send simulated payments to the deployed API in real time.

Requests are signed with AWS Signature V4 using your local AWS credentials,
because the API uses IAM authorization (anonymous callers get 403).
"""

from __future__ import annotations

import collections
import json
import time
import urllib.error
import urllib.request

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from generator.simulator import PaymentSimulator


def stack_output(stack: str, key: str, session: boto3.Session | None = None) -> str:
    session = session or boto3.Session()
    outputs = session.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0].get("Outputs", [])
    for o in outputs:
        if o["OutputKey"] == key:
            return o["OutputValue"]
    raise KeyError(f"{stack} has no output {key}; is it deployed?")


class SignedPoster:
    def __init__(self, url: str, session: boto3.Session | None = None) -> None:
        self.url = url
        self.session = session or boto3.Session()
        self.region = self.session.region_name

    def post(self, events: list[dict]) -> dict:
        body = json.dumps({"events": events}).encode()
        req = AWSRequest(method="POST", url=self.url, data=body, headers={"Content-Type": "application/json"})
        SigV4Auth(self.session.get_credentials().get_frozen_credentials(), "execute-api", self.region).add_auth(req)
        http = urllib.request.Request(self.url, data=body, headers=dict(req.headers.items()), method="POST")
        for attempt in range(4):
            try:
                with urllib.request.urlopen(http, timeout=15) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                if e.code in (429, 503) and attempt < 3:
                    time.sleep(0.5 * 2**attempt)
                    continue
                raise RuntimeError(f"HTTP {e.code}: {e.read().decode()[:300]}") from e
        raise RuntimeError("unreachable")


def run_live(
    sim: PaymentSimulator,
    poster: SignedPoster,
    duration_s: float,
    rate: float,
    max_events: int | None = None,
    batch_size: int = 25,
    flush_every_s: float = 0.5,
) -> collections.Counter:
    """Emit events paced to the wall clock (the simulator clock starts at 'now')."""
    stats: collections.Counter = collections.Counter()
    start = time.time()
    sim.now_ms = int(start * 1000)
    batch: list[dict] = []
    last_flush = time.time()

    def flush() -> None:
        nonlocal batch, last_flush
        if batch:
            result = poster.post(batch)
            stats["accepted"] += result.get("accepted", 0)
            stats["rejected"] += len(result.get("rejected", []))
            stats["requests"] += 1
        batch, last_flush = [], time.time()

    for event in sim.stream(rate):
        if time.time() - start >= duration_s or (max_events and stats["sent"] >= max_events):
            break
        delay = event["event_ts_ms"] / 1000 - time.time()
        if delay > 0:
            time.sleep(min(delay, flush_every_s))
        stats["sent"] += 1
        stats[f"pattern:{event['label_pattern']}"] += 1
        batch.append(event)
        if len(batch) >= batch_size or time.time() - last_flush >= flush_every_s:
            flush()
            if stats["requests"] % 20 == 0:
                print(f"  {int(time.time() - start)}s: sent {stats['sent']} events, accepted {stats['accepted']}")
    flush()
    return stats

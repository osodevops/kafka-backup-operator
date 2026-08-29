#!/usr/bin/env python3
"""Turn `kubectl get -w --output-watch-events -o json` into one JSON line per event."""
import datetime
import json
import sys

kind = sys.argv[1]
dec = json.JSONDecoder()
buf = ""
for line in sys.stdin:
    buf += line
    try:
        ev, _ = dec.raw_decode(buf.strip())
    except Exception:
        continue
    buf = ""
    o = ev["object"]
    t = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if kind == "pods":
        conds = {c["type"]: c["status"] for c in o.get("status", {}).get("conditions", [])}
        out = {"t": t, "type": ev["type"], "name": o["metadata"]["name"],
               "phase": o.get("status", {}).get("phase"), "ready": conds.get("Ready"),
               "deleting": o["metadata"].get("deletionTimestamp")}
    else:
        s = o.get("spec", {})
        out = {"t": t, "type": ev["type"], "holder": s.get("holderIdentity"),
               "transitions": s.get("leaseTransitions"), "acquire": s.get("acquireTime"),
               "renew": s.get("renewTime")}
    print(json.dumps(out), flush=True)

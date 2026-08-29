#!/usr/bin/env python3
"""Small JSON extractors used by the e2e shell helpers (avoids quoting pain in bash)."""
import json
import sys

what = sys.argv[1]
doc = json.load(sys.stdin)

if what == "apply-time":
    times = [f["time"] for f in doc["metadata"].get("managedFields", [])
             if f.get("manager") == "kafka-backup-operator" and f.get("operation") == "Apply"]
    print(times[0] if times else "-")
elif what == "managers":
    for f in doc["metadata"].get("managedFields", []):
        print("\t".join([str(f.get("manager")), str(f.get("operation")), str(f.get("time"))]))
elif what == "pod-names":
    for p in doc["items"]:
        print(p["metadata"]["name"])
elif what == "pod-times":
    for p in doc["items"]:
        cs = (p["status"].get("containerStatuses") or [{}])[0]
        st = cs.get("state", {})
        last = cs.get("lastState", {})
        started = (st.get("running") or st.get("terminated") or {}).get("startedAt", "-")
        finished = (st.get("terminated") or {}).get("finishedAt", "-")
        print("\t".join([
            p["metadata"]["name"],
            "created=" + p["metadata"]["creationTimestamp"],
            "started=" + str(started),
            "finished=" + str(finished),
            "deleting=" + str(p["metadata"].get("deletionTimestamp", "-")),
            "restarts=" + str(cs.get("restartCount", 0)),
            "lastExit=" + str((last.get("terminated") or {}).get("exitCode", "-")),
            "phase=" + str(p["status"].get("phase")),
        ]))
elif what == "watch-pods":
    pass
else:
    sys.exit("unknown query " + what)

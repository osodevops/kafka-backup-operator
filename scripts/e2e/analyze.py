#!/usr/bin/env python3
"""Analyse the JSONL watch logs written by lib.sh's watch_*_bg helpers."""
import datetime
import json
import sys


def parse(t):
    if t is None:
        return None
    return datetime.datetime.fromisoformat(t.replace("Z", "+00:00"))


def load(path):
    out = []
    with open(path, errors="replace") as f:
        for l in f:
            l = l.strip().strip("\x00")
            if not l:
                continue
            try:
                out.append(json.loads(l))
            except json.JSONDecodeError:
                continue  # a torn or foreign line must not sink the analysis
    return out


def overlap(path):
    """Did two operator pods ever run at the same time? Prints one line per pod
    with its first Running and its DELETED time, then overlap=yes|no."""
    evs = load(path)
    first_running, deleted, created = {}, {}, {}
    for e in evs:
        n, t = e["name"], parse(e["t"])
        created.setdefault(n, t)
        if e.get("phase") == "Running" and n not in first_running:
            first_running[n] = t
        if e["type"] == "DELETED":
            deleted[n] = t
    pods = sorted(created, key=lambda n: created[n])
    for n in pods:
        print(f"{n}\tcreated={created[n].isoformat()}\trunning={first_running.get(n) and first_running[n].isoformat()}\tdeleted={deleted.get(n) and deleted[n].isoformat()}")
    over = False
    for i, a in enumerate(pods):
        for b in pods[i + 1:]:
            a_end = deleted.get(a)
            b_start = first_running.get(b) or created[b]
            if a_end is None or b_start < a_end:
                over = True
    print("overlap=" + ("yes" if over else "no"))


def lease(path):
    """Holder sequence, number of holder changes, transitions and handover time
    (from the moment the previous holder disappeared, or its last renewal, to
    the first event showing the new holder)."""
    evs = load(path)
    holders, changes = [], []
    prev = None
    last_seen = {}
    for e in evs:
        h = e.get("holder") or ""
        t = parse(e["t"])
        if h:
            last_seen[h] = t
        if h != prev:
            holders.append(h)
            if prev is not None:
                changes.append((prev, h, t))
            prev = h
    real = [c for c in changes if c[1]]
    summary = {
        "holder_sequence": holders,
        "non_empty_holder_changes": len(real),
        "transitions_first": evs[0].get("transitions") if evs else None,
        "transitions_last": evs[-1].get("transitions") if evs else None,
    }
    if real:
        prev_holder, new_holder, t_new = real[-1]
        # the previous non-empty holder before this change
        prev_non_empty = None
        for h in reversed(holders[: holders.index(new_holder) if new_holder in holders else len(holders)]):
            if h and h != new_holder:
                prev_non_empty = h
                break
        released_at = None
        for a, b, t in changes:
            if a == prev_non_empty and b == "":
                released_at = t
        base = released_at or (last_seen.get(prev_non_empty) if prev_non_empty else None)
        summary["new_holder"] = new_holder
        summary["previous_holder"] = prev_non_empty
        summary["released_at"] = released_at.isoformat() if released_at else None
        summary["acquired_at"] = t_new.isoformat()
        summary["handover_s"] = round((t_new - base).total_seconds(), 2) if base else None
    print(json.dumps(summary))


def ready_before(path, pod, iso):
    """Did <pod> report Ready=True before <iso>?"""
    limit = parse(iso)
    for e in load(path):
        if e["name"] == pod and e.get("ready") == "True" and parse(e["t"]) <= limit:
            print("yes")
            return
    print("no")


cmd = sys.argv[1]
if cmd in ("overlap", "lease", "ready_before"):
    {"overlap": overlap, "lease": lease, "ready_before": ready_before}[cmd](*sys.argv[2:])


def dupstarts(*logs):
    """backup_ids whose engine start was logged by more than one pod. Log file
    names must be <run>-<pod>.log as written by save_logs."""
    import collections, re
    starts = collections.defaultdict(set)
    for f in logs:
        pod = f.rsplit("/", 1)[-1].split("-", 1)[1][:-4]
        for l in open(f, errors="replace"):
            if '"Starting backup engine"' in l or '"Building backup configuration"' in l:
                m = re.search(r'"backup_id":"([^"]+)"', l)
                if m:
                    starts[m.group(1)].add(pod)
    both = sorted(b for b, p in starts.items() if len(p) > 1)
    print(json.dumps({"backup_ids": len(starts), "started_in_two_pods": both,
                      "pods": sorted({pp for p in starts.values() for pp in p})}))


if __name__ == "__main__" and sys.argv[1] == "dupstarts":
    dupstarts(*sys.argv[2:])

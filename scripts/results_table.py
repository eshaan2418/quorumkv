#!/usr/bin/env python3
"""Render bench/results/*.jsonl and chaos summaries as Markdown tables."""
import glob
import json
import os
import re
import statistics

R = "bench/results"


def load_table():
    rows = ["| Configuration | ops/s | write p50 | write p99 | read p50 | read p99 |",
            "|---|---:|---:|---:|---:|---:|"]
    for line in open(os.path.join(R, "load.jsonl")):
        d = json.loads(line)
        m = d["median"]
        ms = lambda us: f"{us / 1000:.2f} ms"
        rows.append(f"| {d['name']} | {m['ops_per_sec']:,.0f} | {ms(m['write_p50_us'])} | {ms(m['write_p99_us'])} "
                    f"| {ms(m['read_p50_us'])} | {ms(m['read_p99_us'])} |")
    return "\n".join(rows)


def failover_table():
    rows = ["| Cluster | kills | median | min | max |", "|---|---:|---:|---:|---:|"]
    for line in open(os.path.join(R, "failover.jsonl")):
        d = json.loads(line)
        xs = d["failover_ms"]
        rows.append(f"| {d['nodes']} nodes | {len(xs)} | {statistics.median(xs):.0f} ms | {min(xs):.0f} ms | {max(xs):.0f} ms |")
    return "\n".join(rows)


def fsync_line():
    ns = [int(m) for m in re.findall(r"(\d+) ns/op", open(os.path.join(R, "fsync.txt")).read())]
    return f"WAL append + fsync: median {statistics.median(ns) / 1e6:.2f} ms over {len(ns)} runs of 200 ({os.path.join(R, 'fsync.txt')})"


def chaos_table():
    rows = ["| Read mode | runs | linearizable | ops checked (total) | faults injected (total) |", "|---|---:|---:|---:|---:|"]
    for mode in ["index", "log", "unsafe"]:
        files = sorted(glob.glob(os.path.join(R, "chaos", f"{mode}-seed*.json")))
        if not files:
            continue
        ds = [json.load(open(f)) for f in files]
        ok = sum(d["linearizable"] for d in ds)
        ops = sum(d["ops_recorded"] for d in ds)
        faults = sum(sum(d["faults"].values()) - d["faults"].get("heal", 0) for d in ds)
        rows.append(f"| {mode} | {len(ds)} | {ok}/{len(ds)} | {ops:,} | {faults} |")
    return "\n".join(rows)


if __name__ == "__main__":
    print(load_table(), "", failover_table(), "", fsync_line(), "", chaos_table(), sep="\n")

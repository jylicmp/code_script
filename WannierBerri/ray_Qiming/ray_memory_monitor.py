#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ray_memory_monitor.py

A lightweight helper for WannierBerri + Ray jobs. Tested with WannierBerri v1.8

Author: Jiayu Li@HKU
Version: v1.0
Last modified: 2026-06-19

Usage in the main WannierBerri script:

    import wannierberri as wberri
    from ray_memory_monitor import (
        init_ray_for_wberri,
        start_ray_memory_watch,
        ray_shutdown_safe,
        print_ray_cluster_info,
    )

    init_ray_for_wberri(
        num_cpus=64,
        temp_dir="YOUR/PATH/TO/ray_tmp",
        include_dashboard=False,
        object_store_memory_gb=20,   # optional
    )

    print_ray_cluster_info()

    mem_watch = start_ray_memory_watch(
        log_file="YOUR/PATH/TO/ray_memory_watch.jsonl",
        interval_sec=10,
        topn=20,
    )

    try:
        result = wberri.run(
            system,
            grid=grid,
            calculators=calculators,
            parallel=True,
            ...
        )
    finally:
        mem_watch.stop()
        ray_shutdown_safe()

Notes:
- Put this file in the same directory as your main running script, or add its
  directory to PYTHONPATH.
- Install psutil if needed: pip install psutil
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def set_thread_env(num_threads: int = 1, force: bool = False) -> None:
    """
    Set common BLAS/OpenMP thread environment variables.

    For Ray parallelism, it is usually safer to use one BLAS/OpenMP thread per
    Ray worker. Call this before importing numpy/scipy-heavy modules if possible.
    """
    value = str(int(num_threads))
    keys = [
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "BLIS_NUM_THREADS",
    ]
    for key in keys:
        if force or key not in os.environ:
            os.environ[key] = value


def init_ray_for_wberri(
    num_cpus: int,
    temp_dir: str,
    include_dashboard: bool = False,
    object_store_memory_gb: Optional[float] = None,
    set_blas_threads: bool = True,
    blas_threads: int = 1,
    **ray_kwargs: Any,
) -> None:
    """
    Initialize Ray through WannierBerri's current helper.

    Parameters
    ----------
    num_cpus
        Number of Ray CPU slots to expose.
    temp_dir
        Ray temporary directory. Use a fast scratch/work directory, not /tmp.
    include_dashboard
        Whether to start Ray dashboard.
    object_store_memory_gb
        Optional Ray object store memory limit in GB.
    set_blas_threads
        If True, set OMP/MKL/OpenBLAS/etc. thread numbers before Ray starts.
    blas_threads
        Thread count used when set_blas_threads=True.
    ray_kwargs
        Additional keyword arguments passed to wannierberri.parallel.ray_init.
    """
    if set_blas_threads:
        set_thread_env(blas_threads, force=False)

    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    from wannierberri.parallel import ray_init

    kwargs = dict(
        num_cpus=int(num_cpus),
        _temp_dir=str(temp_dir),
        include_dashboard=include_dashboard,
    )

    if object_store_memory_gb is not None:
        kwargs["object_store_memory"] = int(float(object_store_memory_gb) * 1024**3)

    kwargs.update(ray_kwargs)
    ray_init(**kwargs)


def ray_shutdown_safe() -> None:
    """Shutdown Ray via WannierBerri helper, with fallback to ray.shutdown()."""
    try:
        from wannierberri.parallel import ray_shutdown

        ray_shutdown()
    except Exception:
        try:
            import ray

            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass


def print_ray_cluster_info() -> None:
    """Print Ray cluster and node information."""
    import ray

    print("\n===== Ray initialized =====")
    print("ray.is_initialized() =", ray.is_initialized())
    if not ray.is_initialized():
        return

    print("cluster_resources =", ray.cluster_resources())
    print("available_resources =", ray.available_resources())

    print("\n===== Ray nodes =====")
    for i, node in enumerate(ray.nodes()):
        print(f"\n--- node {i} ---")
        print(json.dumps(node, indent=2, default=str))


def run_ray_worker_probe(num_probe: int, sleep_time: float = 5.0) -> List[Dict[str, Any]]:
    """
    Submit simple Ray tasks to check how many unique worker processes are actually used.

    Useful before calling wberri.run(...).
    """
    import ray

    @ray.remote(num_cpus=1)
    def _probe_worker(i: int, sleep_time_inner: float) -> Dict[str, Any]:
        import json
        import os
        import socket
        import time

        import ray

        ctx = ray.get_runtime_context()

        info = {
            "probe_id": i,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "cwd": os.getcwd(),
            "node_id": ctx.get_node_id(),
            "worker_id": ctx.get_worker_id(),
            "assigned_resources": ctx.get_assigned_resources(),
            "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS"),
            "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS"),
            "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS"),
        }

        print("[RAY_PROBE] " + json.dumps(info, sort_keys=True), flush=True)
        time.sleep(sleep_time_inner)
        return info

    infos = ray.get([_probe_worker.remote(i, sleep_time) for i in range(int(num_probe))])
    unique_workers = {(x["hostname"], x["pid"]) for x in infos}

    print("\n===== Ray worker probe summary =====")
    print(f"requested probes = {num_probe}")
    print(f"unique worker processes observed = {len(unique_workers)}")

    return infos


def _bytes_to_gb(x: Optional[int]) -> Optional[float]:
    if x is None:
        return None
    return float(x) / 1024**3


def _short_cmd(cmdline: Any, max_len: int = 300) -> str:
    if isinstance(cmdline, list):
        cmd = " ".join(str(x) for x in cmdline)
    else:
        cmd = str(cmdline or "")
    return cmd[:max_len]


try:
    import ray

    @ray.remote(num_cpus=0)
    class RayNodeMemoryMonitor:
        """One actor per Ray node, scanning local Ray-related processes."""

        def __init__(self) -> None:
            self.hostname = socket.gethostname()

        def snapshot(self, topn: int = 20) -> Dict[str, Any]:
            import os
            import socket
            import time

            import psutil

            rows: List[Dict[str, Any]] = []

            for p in psutil.process_iter(
                attrs=["pid", "ppid", "name", "cmdline", "username", "status", "create_time"]
            ):
                try:
                    info = p.info
                    cmdline = _short_cmd(info.get("cmdline"))
                    name = info.get("name") or ""

                    # Ray workers and Ray core processes.  WannierBerri remote
                    # tasks normally appear as "ray::paralfunc" in cmdline/logs.
                    is_ray_process = (
                        "ray::" in cmdline
                        or "ray_worker" in cmdline
                        or "default_worker.py" in cmdline
                        or "raylet" in cmdline
                        or "plasma" in cmdline
                        or "gcs_server" in cmdline
                    )

                    if not is_ray_process:
                        continue

                    mem = p.memory_info()
                    rss = getattr(mem, "rss", 0)
                    shared = getattr(mem, "shared", 0)
                    heap_est = max(rss - shared, 0)

                    uss = None
                    pss = None
                    try:
                        full = p.memory_full_info()
                        uss = getattr(full, "uss", None)
                        pss = getattr(full, "pss", None)
                    except Exception:
                        pass

                    rows.append(
                        {
                            "pid": info.get("pid"),
                            "ppid": info.get("ppid"),
                            "name": name,
                            "status": info.get("status"),
                            "rss_GB": _bytes_to_gb(rss),
                            "shared_GB": _bytes_to_gb(shared),
                            "heap_est_RSS_minus_SHR_GB": _bytes_to_gb(heap_est),
                            "uss_GB": _bytes_to_gb(uss),
                            "pss_GB": _bytes_to_gb(pss),
                            "cmd": cmdline,
                        }
                    )

                except Exception:
                    continue

            rows.sort(key=lambda x: x.get("rss_GB") or 0.0, reverse=True)

            vm = psutil.virtual_memory()
            return {
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "hostname": socket.gethostname(),
                "node_total_GB": _bytes_to_gb(vm.total),
                "node_used_GB": _bytes_to_gb(vm.used),
                "node_available_GB": _bytes_to_gb(vm.available),
                "node_percent": vm.percent,
                "top_processes": rows[: int(topn)],
            }

except Exception:
    RayNodeMemoryMonitor = None  # type: ignore


@dataclass
class MemoryWatchHandle:
    """Handle returned by start_ray_memory_watch(...)."""

    stop_event: threading.Event
    thread: threading.Thread
    log_file: str

    def stop(self, timeout: float = 5.0) -> None:
        self.stop_event.set()
        self.thread.join(timeout=timeout)


def _make_node_monitors() -> List[Any]:
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    if RayNodeMemoryMonitor is None:
        raise RuntimeError(
            "RayNodeMemoryMonitor could not be defined. "
            "Check that ray is importable before using start_ray_memory_watch()."
        )

    monitors: List[Any] = []
    for node in ray.nodes():
        if not node.get("Alive", False):
            continue

        node_id = node["NodeID"]
        mon = RayNodeMemoryMonitor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
        ).remote()
        monitors.append(mon)

    if not monitors:
        raise RuntimeError("No alive Ray nodes found. Did you call ray_init first?")

    return monitors


def start_ray_memory_watch(
    log_file: str,
    interval_sec: float = 10.0,
    topn: int = 20,
    warn_node_used_percent: float = 90.0,
    print_topn: int = 5,
    append: bool = True,
) -> MemoryWatchHandle:
    """
    Start a background thread that records Ray process memory usage.

    Parameters
    ----------
    log_file
        JSONL output file. One JSON record per interval.
    interval_sec
        Monitoring interval.
    topn
        Number of largest Ray-related processes to store per node.
    warn_node_used_percent
        Print warning when node memory usage exceeds this percentage.
    print_topn
        Number of largest processes printed to stdout per node.
    append
        If True append to existing log; otherwise overwrite.

    Returns
    -------
    MemoryWatchHandle
        Call handle.stop() after wberri.run finishes.
    """
    import ray

    if not ray.is_initialized():
        raise RuntimeError("Ray is not initialized. Call init_ray_for_wberri(...) first.")

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    monitors = _make_node_monitors()
    stop_event = threading.Event()
    mode = "a" if append else "w"

    def loop() -> None:
        with open(log_path, mode, buffering=1) as f:
            while not stop_event.is_set():
                try:
                    snapshots = ray.get([m.snapshot.remote(topn=int(topn)) for m in monitors])

                    record = {
                        "driver_time": datetime.now().isoformat(timespec="seconds"),
                        "snapshots": snapshots,
                    }
                    f.write(json.dumps(record, sort_keys=True) + "\n")

                    print("\n[Ray memory monitor]", flush=True)
                    for s in snapshots:
                        hostname = s.get("hostname")
                        used = s.get("node_used_GB") or 0.0
                        total = s.get("node_total_GB") or 0.0
                        avail = s.get("node_available_GB") or 0.0
                        percent = s.get("node_percent") or 0.0

                        print(
                            f"  node={hostname} "
                            f"used={used:.1f}/{total:.1f} GB "
                            f"({percent:.1f}%), avail={avail:.1f} GB",
                            flush=True,
                        )

                        if percent >= warn_node_used_percent:
                            print(
                                f"  WARNING: node {hostname} memory usage "
                                f"{percent:.1f}% >= {warn_node_used_percent:.1f}%",
                                flush=True,
                            )

                        for p in s.get("top_processes", [])[: int(print_topn)]:
                            rss = p.get("rss_GB")
                            shr = p.get("shared_GB")
                            heap = p.get("heap_est_RSS_minus_SHR_GB")
                            cmd = p.get("cmd", "")[:90]
                            print(
                                f"    PID={p.get('pid')} "
                                f"RSS={rss if rss is not None else 0.0:.2f} GB "
                                f"SHR={shr if shr is not None else 0.0:.2f} GB "
                                f"heap~={heap if heap is not None else 0.0:.2f} GB "
                                f"cmd={cmd}",
                                flush=True,
                            )

                except Exception as err:
                    print(f"[Ray memory monitor] ERROR: {repr(err)}", flush=True)

                stop_event.wait(float(interval_sec))

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    return MemoryWatchHandle(stop_event=stop_event, thread=thread, log_file=str(log_path))


def summarize_memory_log(log_file: str, topn: int = 20) -> List[Dict[str, Any]]:
    """
    Read a ray_memory_watch.jsonl file and summarize peak process RSS per host/PID.

    Returns a list sorted by peak RSS descending.
    """
    peaks: Dict[str, Dict[str, Any]] = {}

    with open(log_file, "r") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            for snap in rec.get("snapshots", []):
                host = snap.get("hostname")
                for p in snap.get("top_processes", []):
                    pid = p.get("pid")
                    key = f"{host}:{pid}"
                    rss = p.get("rss_GB") or 0.0
                    heap = p.get("heap_est_RSS_minus_SHR_GB") or 0.0
                    old = peaks.get(key)
                    if old is None or rss > old.get("peak_rss_GB", 0.0):
                        peaks[key] = {
                            "host": host,
                            "pid": pid,
                            "peak_rss_GB": rss,
                            "peak_heap_est_GB": heap,
                            "cmd": p.get("cmd", ""),
                        }

    rows = sorted(peaks.values(), key=lambda x: x["peak_rss_GB"], reverse=True)
    return rows[: int(topn)]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Summarize a ray_memory_watch.jsonl file.")
    parser.add_argument("log_file", help="Path to ray_memory_watch.jsonl")
    parser.add_argument("--topn", type=int, default=20)
    args = parser.parse_args()

    rows = summarize_memory_log(args.log_file, topn=args.topn)
    print(json.dumps(rows, indent=2, default=str))

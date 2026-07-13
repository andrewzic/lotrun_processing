#!/usr/bin/env python3
"""Wait for at least one Dask worker to connect to a scheduler.

Usage:
    wait_for_workers.py <scheduler_address> [--timeout SECONDS] [--interval SECONDS]
"""

import argparse
import sys
import time

from distributed import Client


def main():
    parser = argparse.ArgumentParser(
        description="Poll a Dask scheduler until at least one worker connects."
    )
    parser.add_argument("scheduler", help="Scheduler address (e.g. tcp://host:port)")
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Max seconds to wait (default: 300)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Polling interval in seconds (default: 10)",
    )
    args = parser.parse_args()

    client = Client(args.scheduler)
    start = time.time()

    try:
        while time.time() - start < args.timeout:
            info = client.scheduler_info()
            n_workers = len(info.get("workers", {}))
            elapsed = int(time.time() - start)
            print(
                f"[DASK] {n_workers} worker(s) connected ({elapsed}s elapsed)",
                flush=True,
            )
            if n_workers > 0:
                print("[DASK] Workers ready, proceeding.", flush=True)
                return 0
            time.sleep(args.interval)

        print(
            f"ERROR: No workers connected after {args.timeout}s",
            flush=True,
        )
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())

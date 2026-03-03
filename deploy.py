#!/usr/bin/env python3
"""
Create Prefect deployment for orchestration workflow.
"""

from orchestrate import main

if __name__ == "__main__":
    main.serve(
        name="denovo-benchmarks-runner",
        description="Orchestrate denovo benchmarks: build containers and run benchmarks",
        tags=["hpc", "benchmarks", "denovo"],
        version="1.0",
    )

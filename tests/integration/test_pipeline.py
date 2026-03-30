"""
Integration tests for the full pipeline orchestration.

Mocking strategy — only mock the raw I/O primitives, never high-level logic:
  - runner.remote_fs.*   — SSH / rsync calls → replaced with local-filesystem
                           equivalents that operate on a mutable copy of the
                           static alexandria fixture directory.
  - runner.git_ops.git_fetch / git_count_behind — network git operations;
    the rest of check_or_clone_repo() runs for real against a git-init'd dir.
  - runner.algorithm_runner.get_runner_dir — returns the mutable asimov dir.
  - Slurm: submit_and_wait_for_* → raise AssertionError on any call.

What runs for REAL (no mocking):
  - check_or_clone_repo()              full branch-check + update logic
  - check_outputs()                    directory traversal + path parsing
  - check_containers()                 per-algo file-existence checks
  - check_evaluation_container()       file-existence check
  - get_outputs_needing_augmentation() CSV header sniffing
  - pull_evaluation_container()        file-existence → early-return (file present)
  - check_and_display_builds()         full BuildState logic (empty state)
  - find_datasets_needing_evaluation() real result-CSV checks on disk
  - analyze_missing()                  pure dict/set logic

Static fixture layout (tests/test_datasets/<scenario>/):
  git/                 denovo_benchmarks git checkout content (algorithms/, results/, …)
                       NOTE: only repo-tracked files — no generated artefacts.
  asimov/              Asimov-local files outside the git checkout (pulled algo
                       containers, state files, …).  Empty for scenarios where
                       nothing has been pre-fetched.
  alexandria/          Alexandria storage before test.
  expected_git/        Expected state of denovo_benchmarks checkout after test
                       (includes pipeline artefacts such as evaluation.sif).
  expected_asimov/     Expected Asimov state after test — compared at teardown.
  expected_alexandria/ Expected Alexandria state after test — compared at teardown.
"""

import asyncio
import shutil
import subprocess
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterParentFlowRunId,
    TaskRunFilter,
    TaskRunFilterFlowRunId,
)
from prefect.testing.utilities import prefect_test_harness

from runner.build_state import BuildState

# ---------------------------------------------------------------------------
# Scenario directories (static, read-only, committed to the repo)
# ---------------------------------------------------------------------------

SCENARIOS_DIR = Path(__file__).parent.parent / "test_datasets"
ALL_RESULTS_READY = SCENARIOS_DIR / "all_results_ready"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _setup_mutable_asimov(scenario: Path, tmp_path: Path) -> Path:
    """
    Build a mutable working copy of the Asimov filesystem for one test.

    1. Copy ``scenario/git/`` into ``tmp_path/asimov/denovo_benchmarks/``.
    2. Overlay ``scenario/asimov/`` on top (dirs_exist_ok), so any extra
       files that should already be on Asimov (e.g. evaluation.sif) end up
       in the right places.
    3. ``git init`` the checkout so check_or_clone_repo() can inspect branch/log.
    """
    asimov = tmp_path / "asimov"
    bm = asimov / "denovo_benchmarks"

    # Step 1 — git checkout content
    shutil.copytree(scenario / "git", bm)

    # Step 2 — asimov overlay (evaluation.sif etc.)
    shutil.copytree(scenario / "asimov", asimov, dirs_exist_ok=True)

    # Step 3 — initialise as a real git repo
    for cmd in [
        ["git", "-C", str(bm), "init", "-b", "main"],
        ["git", "-C", str(bm), "config", "user.email", "test@test.com"],
        ["git", "-C", str(bm), "config", "user.name", "Test"],
        ["git", "-C", str(bm), "add", "."],
        ["git", "-C", str(bm), "commit", "-m", "init"],
    ]:
        subprocess.run(cmd, capture_output=True, check=False)

    return asimov


def _setup_mutable_alexandria(scenario: Path, tmp_path: Path) -> Path:
    """Return a mutable copy of ``scenario/alexandria/`` inside ``tmp_path``."""
    alex = tmp_path / "alexandria"
    shutil.copytree(scenario / "alexandria", alex)
    return alex


def _assert_matches_expected(actual: Path, expected: Path, label: str) -> None:
    """
    Assert that every file listed in *expected/* exists in *actual/* and has
    identical content.  ``.git/`` sub-trees are excluded from *actual*.

    This is an "at-least" check: the expected directory documents the files
    the pipeline is supposed to create or leave intact; any extra files the
    pipeline might write are not flagged as errors here (Slurm guards prevent
    unexpected mutations in test scenarios anyway).
    """
    for exp_file in sorted(expected.rglob("*")):
        if not exp_file.is_file():
            continue
        rel = exp_file.relative_to(expected)
        act_file = actual / rel
        assert act_file.exists(), f"[{label}] Expected file missing after test: {rel}"
        assert act_file.read_bytes() == exp_file.read_bytes(), (
            f"[{label}] File content mismatch after test: {rel}"
        )


# ---------------------------------------------------------------------------
# Remote-filesystem primitives replaced by local-filesystem equivalents
# ---------------------------------------------------------------------------


def _local_remote_file_exists(alex: Path):
    def _impl(host: str, path: str) -> bool:
        return (alex / path.lstrip("/")).is_file()

    return _impl


def _local_remote_dir_exists(alex: Path):
    def _impl(host: str, path: str) -> bool:
        return (alex / path.lstrip("/")).is_dir()

    return _impl


def _local_remote_mkdir(alex: Path):
    def _impl(host: str, path: str) -> None:
        (alex / path.lstrip("/")).mkdir(parents=True, exist_ok=True)

    return _impl


def _local_remote_find(alex: Path):
    def _impl(host: str, path: str, mindepth: int, maxdepth: int, type_: str = "d") -> list[str]:
        local_root = alex / path.lstrip("/")
        if not local_root.exists():
            return []
        results = []
        for p in local_root.rglob("*"):
            if type_ == "d" and not p.is_dir():
                continue
            if type_ == "f" and not p.is_file():
                continue
            depth = len(p.relative_to(local_root).parts)
            if mindepth <= depth <= maxdepth:
                results.append("/" + str(p.relative_to(alex)))
        return results

    return _impl


def _local_remote_read_first_line(alex: Path):
    def _impl(host: str, path: str) -> str | None:
        local_path = alex / path.lstrip("/")
        if not local_path.is_file():
            return None
        with open(local_path) as f:
            return f.readline().strip()

    return _impl


def _local_rsync_pull(alex: Path):
    def _impl(host: str, src: str, dst: Path) -> bool:
        local_src = alex / src.lstrip("/")
        if not local_src.is_file():
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_src, dst)
        return True

    return _impl


def _slurm_must_not_run(name: str) -> MagicMock:
    """Raises AssertionError if called — ensures no Slurm job is submitted."""
    return MagicMock(side_effect=AssertionError(f"Slurm job must not be submitted: {name}"))


async def _fetch_all_run_states(flow_run_id: str) -> tuple[list, list]:
    """
    Query the Prefect backend (active inside ``prefect_test_harness``) for:
      - every task run that belongs to *flow_run_id*
      - every subflow run whose parent is *flow_run_id*

    Must be called **inside** the ``prefect_test_harness`` context;
    the ephemeral SQLite backend is torn down on exit.
    """
    async with get_client() as client:
        task_runs = await client.read_task_runs(
            task_run_filter=TaskRunFilter(flow_run_id=TaskRunFilterFlowRunId(any_=[flow_run_id]))
        )
        subflow_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                parent_flow_run_id=FlowRunFilterParentFlowRunId(any_=[flow_run_id])
            )
        )
    return task_runs, subflow_runs


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def clean_build_state(tmp_path: Path):
    """
    Redirect BuildState to a fresh tmp file so check_and_display_builds runs
    with an empty state (→ zero squeue calls) and never touches the production
    build_state.json.
    """
    original_init = BuildState.__init__

    def _patched_init(self, state_file=None):
        original_init(self, state_file=state_file or (tmp_path / "build_state.json"))

    with patch.object(BuildState, "__init__", _patched_init):
        yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_all_complete(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: every container and output already exist on Alexandria, and every
    dataset already has all result CSVs on disk (in the git checkout / Asimov).

    Static fixtures: tests/test_datasets/all_results_ready/
      git/                 → denovo_benchmarks algorithms + results (git-tracked only)
      asimov/              → empty (no pre-fetched algorithm containers)
      alexandria/          → all containers + augmented outputs present
      expected_git/        → git/ content + evaluation.sif pulled from Alexandria
      expected_asimov/     → empty (unchanged after test)
      expected_alexandria/ → unchanged after test

    Expected pipeline behaviour:
      - Flow completes with Prefect Completed state (no crashes, no cancellations).
      - check_or_clone_repo() sees repo exists, correct branch, 0 commits behind.
      - check_outputs() traverses alexandria and finds all (algo, dataset) pairs.
      - check_containers() finds all containers present.
      - check_evaluation_container() finds eval container present.
      - pull_evaluation_container() pulls evaluation.sif from Alexandria into
        benchmarks_dir (it does NOT pre-exist in git/ or asimov/).
      - get_outputs_needing_augmentation() reads CSV headers → all already augmented.
      - No Slurm jobs submitted (guards raise AssertionError on any call).
    """
    scenario = ALL_RESULTS_READY

    asimov = _setup_mutable_asimov(scenario, tmp_path)
    alex = _setup_mutable_alexandria(scenario, tmp_path)
    benchmarks_dir = asimov / "denovo_benchmarks"

    config = {
        "denovo_benchmarks": {
            "local_path": str(benchmarks_dir),
            "repo_url": "git@github.com:test/denovo_benchmarks.git",
            "branch": "main",
        },
        "alexandria": {
            "host": "mock@alexandria",
            "outputs_path": "/mock/outputs",
            "containers_path": "/mock/containers",
        },
        "datasets": ["test_dataset_human"],
        "slurm": {
            "partition": "one_hour",
            "cpus": 4,
            "memory": "16G",
            "gpus": 1,
            "time": "01:00:00",
        },
        "excluded_algorithms": [],
        "version_strategy": "latest_only",
    }

    with ExitStack() as stack:
        # ── Local-filesystem SSH / rsync primitives ──────────────────────────
        stack.enter_context(
            patch(
                "runner.alexandria.remote_file_exists",
                side_effect=_local_remote_file_exists(alex),
            )
        )
        stack.enter_context(
            patch(
                "runner.alexandria.remote_dir_exists",
                side_effect=_local_remote_dir_exists(alex),
            )
        )
        stack.enter_context(
            patch("runner.alexandria.remote_mkdir", side_effect=_local_remote_mkdir(alex))
        )
        stack.enter_context(
            patch("runner.alexandria.remote_find", side_effect=_local_remote_find(alex))
        )
        stack.enter_context(
            patch(
                "runner.alexandria.remote_read_first_line",
                side_effect=_local_remote_read_first_line(alex),
            )
        )
        stack.enter_context(
            patch(
                "runner.algorithm_runner.remote_file_exists",
                side_effect=_local_remote_file_exists(alex),
            )
        )
        stack.enter_context(
            patch("runner.algorithm_runner.rsync_pull", side_effect=_local_rsync_pull(alex))
        )
        # ── Git network primitives (no real remote needed) ───────────────────
        stack.enter_context(patch("runner.git_ops.git_fetch"))
        stack.enter_context(patch("runner.git_ops.git_count_behind", return_value=0))
        # ── Point get_runner_dir() at our mutable asimov dir ─────────────────
        stack.enter_context(patch("runner.algorithm_runner.get_runner_dir", return_value=asimov))
        # ── Scaffolding ───────────────────────────────────────────────────────
        stack.enter_context(patch("orchestrate.load_config", return_value=config))
        stack.enter_context(patch("orchestrate.cleanup_workspace", new=MagicMock()))
        # ── Slurm guards (raise AssertionError if called) ─────────────────────
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_build", _slurm_must_not_run("build"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_run", _slurm_must_not_run("run"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_evaluation", _slurm_must_not_run("evaluate"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_pull", _slurm_must_not_run("pull"))
        )

        with prefect_test_harness():
            from orchestrate import main

            state = main(return_state=True)

            # Collect task / subflow states while the Prefect backend is still live
            flow_run_id = str(state.state_details.flow_run_id)
            task_runs, subflow_runs = asyncio.run(_fetch_all_run_states(flow_run_id))

    # ── Prefect state assertions ──────────────────────────────────────────────
    assert state.is_completed(), f"Top-level flow did not complete. State: {state.type}"

    assert task_runs, "No task runs recorded — Prefect state query returned nothing"
    failed_tasks = [tr for tr in task_runs if not tr.state.is_completed()]
    assert not failed_tasks, "Tasks did not complete:\n" + "\n".join(
        f"  {tr.name!r}: {tr.state.type}" for tr in failed_tasks
    )

    failed_subflows = [fr for fr in subflow_runs if not fr.state.is_completed()]
    assert not failed_subflows, "Subflows did not complete:\n" + "\n".join(
        f"  {fr.name!r}: {fr.state.type}" for fr in failed_subflows
    )

    # ── Exact task / subflow counts ───────────────────────────────────────────
    # These numbers are the source-of-truth for pipeline structure.
    # If you add or remove a @task / @flow, update the counts and the
    # task-name dict below to document the intended change.
    #
    # Task name format from Prefect: "<Task Name>-<hash>".
    # We strip the trailing "-<hash>" to get the canonical name.
    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)

    expected_task_name_counts = Counter(
        {
            # Appears twice: once pre-loop (initial fetch) and once inside
            # the first loop iteration before the "all complete" break.
            "Check Alexandria Outputs": 2,
            # Each of the following runs exactly once (pre-loop only,
            # because the loop breaks before any re-checks are needed).
            "Analyze Container Build Status": 1,
            "Analyze Missing Combinations": 1,
            "Check Containers existance on Alexandria": 1,
            "Check Evaluation Container on Alexandria": 1,
            "Check Outputs Needing Augmentation": 1,
            "Discover available algorithms": 1,
            "Ensure denovo-benchmarks Repository": 1,
            "Evaluate Datasets": 1,
            "Find Datasets Needing Evaluation": 1,
            "Pull Evaluation Container": 1,
        }
    )

    assert task_name_counts == expected_task_name_counts, (
        "Pipeline task structure has changed!\n"
        "  Actual:   " + str(dict(task_name_counts.most_common())) + "\n"
        "  Expected: " + str(dict(expected_task_name_counts.most_common())) + "\n"
        "Update the expected_task_name_counts dict if the change is intentional."
    )

    # 1 subflow = evaluate_datasets_flow (called by the "Evaluate Datasets" task)
    assert len(subflow_runs) == 1, (
        f"Expected 1 subflow run (evaluate_datasets_flow), got {len(subflow_runs)}.\n"
        "Update if a new sub-flow was added or removed."
    )

    # ── Filesystem state assertions ───────────────────────────────────────────
    _assert_matches_expected(benchmarks_dir, scenario / "expected_git", "git")
    _assert_matches_expected(asimov, scenario / "expected_asimov", "asimov")
    _assert_matches_expected(alex, scenario / "expected_alexandria", "alexandria")

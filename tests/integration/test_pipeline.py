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
FRESH_START = SCENARIOS_DIR / "fresh_start"


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


def _build_creates_container(alex: Path):
    """
    Side-effect for ``submit_and_wait_for_build``.

    Creates the container .sif on the local Alexandria directory to simulate
    a successful Apptainer build that pushed the image to Alexandria.
    Returns ``(True, "job_build_<algo>")``.
    """

    def _impl(config, algo_name, version, build_state):
        containers_base = config["alexandria"]["containers_path"]
        if algo_name == "evaluation":
            sif = alex / (containers_base + "/evaluation/evaluation.sif").lstrip("/")
        else:
            sif = alex / (containers_base + f"/{algo_name}/{version}/container.sif").lstrip("/")
        sif.parent.mkdir(parents=True, exist_ok=True)
        sif.touch()
        return True, f"job_build_{algo_name}"

    return _impl


def _run_creates_output(alex: Path, algorithms: list[dict]):
    """
    Side-effect for ``submit_and_wait_for_run``.

    Creates an augmented ``output.csv`` on the local Alexandria directory to
    simulate a completed algorithm run that uploaded results to Alexandria.
    Returns ``(True, "job_run_<algo>_<dataset>")``.
    """

    def _impl(config, algo_name, version, dataset):
        outputs_base = config["alexandria"]["outputs_path"]
        csv = alex / (outputs_base + f"/{algo_name}/{version}/{dataset}/output.csv").lstrip("/")
        csv.parent.mkdir(parents=True, exist_ok=True)
        csv.write_text("peptide,score,SA,pred_RT\nGELVKGA,0.99,0.85,102.3\n")
        return True, f"job_run_{algo_name}_{dataset}"

    return _impl


def _pull_marks_available():
    """
    Side-effect for ``submit_and_wait_for_pull``.

    Marks the dataset as available in the DatasetManager state so subsequent
    ``run_single_combination`` calls see it as ready.  Returns ``True``.
    """

    def _impl(config, dataset_name, dataset_manager):
        # mark_available only updates existing entries — must create the entry first
        # (the real code calls mark_pulling when the job is submitted, then mark_available)
        dataset_manager.mark_pulling(dataset_name, "mock_job_pull", 0)
        dataset_manager.mark_available(dataset_name)
        return True

    return _impl


def _evaluate_creates_results(benchmarks_dir: Path):
    """
    Side-effect for ``submit_and_wait_for_evaluation``.

    Creates all expected result CSVs in benchmarks_dir/results/<dataset>/ to
    simulate a completed evaluation container run.
    Returns ``(True, "job_eval_<dataset>")``.
    """
    result_files = [
        "metrics.csv",
        "peptide_precision_plot_data.csv",
        "AA_precision_plot_data.csv",
        "RT_difference_plot_data.csv",
        "SA_plot_data.csv",
        "number_of_proteome_matches_plot_data.csv",
    ]

    def _impl(config, dataset_name):
        results_dir = benchmarks_dir / "results" / dataset_name
        results_dir.mkdir(parents=True, exist_ok=True)
        for fname in result_files:
            (results_dir / fname).write_text("header\nresult\n")
        return True, f"job_eval_{dataset_name}"

    return _impl


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
# Shared helpers — extracted from the two happy-path tests above
# ---------------------------------------------------------------------------


def _make_config(benchmarks_dir: Path) -> dict:
    """Standard pipeline config pointing at a mutable *benchmarks_dir*."""
    return {
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


def _enter_infra_patches(
    stack: ExitStack,
    asimov: Path,
    alex: Path,
    config: dict,
    *,
    git_count_behind: int = 0,
) -> None:
    """
    Enter all non-Slurm infrastructure patches into *stack*:
      - remote_fs primitives (SSH / rsync) → local-filesystem equivalents
      - git network primitives → no-ops (git_count_behind configurable)
      - get_runner_dir → *asimov*
      - load_config → *config*
      - cleanup_workspace → MagicMock
    """
    for target, side_effect in [
        ("runner.alexandria.remote_file_exists", _local_remote_file_exists(alex)),
        ("runner.alexandria.remote_dir_exists", _local_remote_dir_exists(alex)),
        ("runner.alexandria.remote_mkdir", _local_remote_mkdir(alex)),
        ("runner.alexandria.remote_find", _local_remote_find(alex)),
        ("runner.alexandria.remote_read_first_line", _local_remote_read_first_line(alex)),
        ("runner.algorithm_runner.remote_file_exists", _local_remote_file_exists(alex)),
        ("runner.algorithm_runner.rsync_pull", _local_rsync_pull(alex)),
    ]:
        stack.enter_context(patch(target, side_effect=side_effect))

    stack.enter_context(patch("runner.git_ops.git_fetch"))
    stack.enter_context(patch("runner.git_ops.git_count_behind", return_value=git_count_behind))
    stack.enter_context(patch("runner.algorithm_runner.get_runner_dir", return_value=asimov))
    stack.enter_context(patch("runner.dataset_manager.get_runner_dir", return_value=asimov))
    stack.enter_context(patch("orchestrate.load_config", return_value=config))
    stack.enter_context(patch("orchestrate.cleanup_workspace", new=MagicMock()))


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


# ---------------------------------------------------------------------------
# Error-scenario tests
# ---------------------------------------------------------------------------
# These tests verify graceful-degradation behaviour: the top-level flow must
# always complete (never crash), pipeline logic must stop correctly at the
# right point, and the filesystem must show only the side-effects that are
# expected given the error.
#
# Mocking strategy: same infra patches as the happy-path tests via
# _enter_infra_patches(); Slurm mocks are replaced with failure-producing
# equivalents.
#
# Exact Prefect task counts are asserted with the same rigour as the
# happy-path tests.  Tasks that raise an Exception (e.g. Build Container)
# appear in task_runs with a non-Completed state; the counts still include
# them so the Counter assertion remains a full structural check.
# ---------------------------------------------------------------------------


def test_pipeline_build_fails(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: all three Apptainer builds fail (casanovo, adanovo, evaluation).

    Expected pipeline behaviour:
      - All Build Container tasks raise an Exception → CRASHED state in Prefect.
      - Post-build re-check shows containers still absent.
      - evaluation_exists = False → augmentation / evaluate_datasets_flow skipped.
      - Main-loop iter 1: missing_with_container = [] (no containers) → break.
      - No Slurm run / pull / evaluation jobs submitted.
      - Alexandria: no .sif or output files created.
      - Asimov: evaluation.sif absent, dataset_state.json unchanged ({}).
    """
    scenario = FRESH_START
    asimov = _setup_mutable_asimov(scenario, tmp_path)
    alex = _setup_mutable_alexandria(scenario, tmp_path)
    benchmarks_dir = asimov / "denovo_benchmarks"
    config = _make_config(benchmarks_dir)

    with ExitStack() as stack:
        _enter_infra_patches(stack, asimov, alex, config)
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_build",
                return_value=(False, "mock_job_build"),
            )
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_run", _slurm_must_not_run("run"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_pull", _slurm_must_not_run("pull"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_evaluation", _slurm_must_not_run("evaluate"))
        )

        with prefect_test_harness():
            from orchestrate import main

            state = main(return_state=True)
            flow_run_id = str(state.state_details.flow_run_id)
            task_runs, subflow_runs = asyncio.run(_fetch_all_run_states(flow_run_id))

    # ── Prefect state assertions ──────────────────────────────────────────────
    assert state.is_completed(), f"Top-level flow did not complete. State: {state.type}"

    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)
    expected_task_name_counts = Counter(
        {
            # Pre-loop parallel + loop iter 1 = 2
            "Check Alexandria Outputs": 2,
            # Pre-build only; post-build re-check calls raw check_containers() not the task = 1
            # Post-build re-check does call check_evaluation_container_task (task wrapper) = 2
            "Check Containers existance on Alexandria": 1,
            "Check Evaluation Container on Alexandria": 2,
            # Once each
            "Analyze Container Build Status": 1,
            "Discover available algorithms": 1,
            "Ensure denovo-benchmarks Repository": 1,
            # All three fail (CRASHED state but still counted)
            "Build Container: casanovo": 1,
            "Build Container: adanovo": 1,
            "Build Container: evaluation": 1,
            # Loop iter 1 only — missing_with_container=[] → break immediately
            "Analyze Missing Combinations": 1,
            # NOTE: Pull Evaluation Container, Check Outputs Needing Augmentation,
            # Find Datasets Needing Evaluation, Evaluate Datasets, Scan Existing Datasets,
            # Pull Datasets, Run Algorithm Benchmarks — all skipped.
        }
    )
    assert task_name_counts == expected_task_name_counts, (
        "Pipeline task structure has changed!\n"
        "  Actual:   " + str(dict(task_name_counts.most_common())) + "\n"
        "  Expected: " + str(dict(expected_task_name_counts.most_common()))
    )
    assert len(subflow_runs) == 0, (
        f"Expected 0 subflow runs (all skipped after build failure), got {len(subflow_runs)}."
    )

    # ── Filesystem assertions ─────────────────────────────────────────────────
    # No containers or outputs created on Alexandria.
    assert not list((alex / "mock" / "containers").rglob("*.sif")), (
        "No .sif files should exist on Alexandria after build failure"
    )
    assert not list((alex / "mock" / "outputs").rglob("*.csv")), (
        "No output.csv files should exist on Alexandria after build failure"
    )
    assert not (asimov / "evaluation.sif").exists(), (
        "evaluation.sif must not be pulled when evaluation container was not built"
    )


def test_pipeline_dataset_pull_fails(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: builds succeed, but the dataset pull job returns failure.

    Expected pipeline behaviour:
      - Containers created on Alexandria; evaluation.sif pulled to Asimov.
      - Main-loop iter 1: scan finds no datasets; pull_datasets_flow called;
        pull_single_dataset_task returns False → pulled_count = 0 → break.
      - No run / evaluation Slurm jobs submitted.
      - Alexandria: containers present, no outputs.
      - Asimov: evaluation.sif present, dataset_state.json stays {}.
    """
    scenario = FRESH_START
    asimov = _setup_mutable_asimov(scenario, tmp_path)
    alex = _setup_mutable_alexandria(scenario, tmp_path)
    benchmarks_dir = asimov / "denovo_benchmarks"
    config = _make_config(benchmarks_dir)

    with ExitStack() as stack:
        _enter_infra_patches(stack, asimov, alex, config)
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_build",
                side_effect=_build_creates_container(alex),
            )
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_run", _slurm_must_not_run("run"))
        )
        # Pull job fails: returns False without marking dataset available.
        stack.enter_context(patch("orchestrate.submit_and_wait_for_pull", return_value=False))
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_evaluation", _slurm_must_not_run("evaluate"))
        )

        with prefect_test_harness():
            from orchestrate import main

            state = main(return_state=True)
            flow_run_id = str(state.state_details.flow_run_id)
            task_runs, subflow_runs = asyncio.run(_fetch_all_run_states(flow_run_id))

    # ── Prefect state assertions ──────────────────────────────────────────────
    assert state.is_completed(), f"Top-level flow did not complete. State: {state.type}"

    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)
    expected_task_name_counts = Counter(
        {
            # Pre-loop + loop iter 1 = 2
            "Check Alexandria Outputs": 2,
            # Pre-build only; post-build re-check calls raw check_containers() not the task = 1
            # Post-build re-check does call check_evaluation_container_task (task wrapper) = 2
            "Check Containers existance on Alexandria": 1,
            "Check Evaluation Container on Alexandria": 2,
            "Analyze Container Build Status": 1,
            "Discover available algorithms": 1,
            "Ensure denovo-benchmarks Repository": 1,
            "Build Container: casanovo": 1,
            "Build Container: adanovo": 1,
            "Build Container: evaluation": 1,
            # Post-build, pre-loop
            "Pull Evaluation Container": 1,
            "Check Outputs Needing Augmentation": 1,
            "Find Datasets Needing Evaluation": 1,
            "Evaluate Datasets": 1,  # subflow proxy, empty list
            # Loop iter 1: scan → pull → pulled_count=0 → break
            "Scan Existing Datasets on Asimov": 1,
            "Pull Datasets": 1,  # subflow proxy
            "Analyze Missing Combinations": 1,
        }
    )
    assert task_name_counts == expected_task_name_counts, (
        "Pipeline task structure has changed!\n"
        "  Actual:   " + str(dict(task_name_counts.most_common())) + "\n"
        "  Expected: " + str(dict(expected_task_name_counts.most_common()))
    )
    # 2 subflows: evaluate_datasets_flow (empty, pre-loop) + pull_datasets_flow (failed)
    assert len(subflow_runs) == 2, (
        f"Expected 2 subflow runs, got {len(subflow_runs)}.\n"
        + "\n".join(f"  {fr.name!r}" for fr in subflow_runs)
    )

    # ── Filesystem assertions ─────────────────────────────────────────────────
    # Containers built on Alexandria.
    assert (alex / "mock" / "containers" / "casanovo" / "v4.2.1" / "container.sif").exists()
    assert (alex / "mock" / "containers" / "adanovo" / "bm-1.0.0" / "container.sif").exists()
    assert (alex / "mock" / "containers" / "evaluation" / "evaluation.sif").exists()
    # Eval container pulled to Asimov.
    assert (asimov / "evaluation.sif").exists()
    # No outputs (pull failed, nothing ran).
    assert not list((alex / "mock" / "outputs").rglob("*.csv")), (
        "No output files should exist — run never started"
    )


def test_pipeline_algorithm_run_fails(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: builds succeed, dataset pull succeeds, but run jobs fail.

    Expected pipeline behaviour:
      - Containers created; dataset pulled (marked available).
      - run_single_combination raises Exception for every combination.
      - run_algorithm_benchmarks_flow returns (2, 0) → failed_runs=2 → break.
      - No outputs created on Alexandria.
      - dataset_state.json left with test_dataset_human marked available
        (cleanup_dataset_task is only called when ALL algos succeed).
    """
    scenario = FRESH_START
    asimov = _setup_mutable_asimov(scenario, tmp_path)
    alex = _setup_mutable_alexandria(scenario, tmp_path)
    benchmarks_dir = asimov / "denovo_benchmarks"
    config = _make_config(benchmarks_dir)

    with ExitStack() as stack:
        _enter_infra_patches(stack, asimov, alex, config)
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_build",
                side_effect=_build_creates_container(alex),
            )
        )
        # Run job fails: returns (False, job_id) → run_single_combination raises.
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_run",
                return_value=(False, "mock_job_run"),
            )
        )
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_pull",
                side_effect=_pull_marks_available(),
            )
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_evaluation", _slurm_must_not_run("evaluate"))
        )

        with prefect_test_harness():
            from orchestrate import main

            state = main(return_state=True)
            flow_run_id = str(state.state_details.flow_run_id)
            task_runs, subflow_runs = asyncio.run(_fetch_all_run_states(flow_run_id))

    # ── Prefect state assertions ──────────────────────────────────────────────
    assert state.is_completed(), f"Top-level flow did not complete. State: {state.type}"

    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)
    expected_task_name_counts = Counter(
        {
            # Pre-loop + loop iter 1 = 2
            "Check Alexandria Outputs": 2,
            # Pre-build only; post-build re-check calls raw check_containers() not the task = 1
            # Post-build re-check does call check_evaluation_container_task (task wrapper) = 2
            "Check Containers existance on Alexandria": 1,
            "Check Evaluation Container on Alexandria": 2,
            "Analyze Container Build Status": 1,
            "Discover available algorithms": 1,
            "Ensure denovo-benchmarks Repository": 1,
            "Build Container: casanovo": 1,
            "Build Container: adanovo": 1,
            "Build Container: evaluation": 1,
            "Pull Evaluation Container": 1,
            "Check Outputs Needing Augmentation": 1,
            "Find Datasets Needing Evaluation": 1,
            "Evaluate Datasets": 1,
            # Loop iter 1: scan → pull (ok) → run (fails) → failed_runs>0 → break
            "Scan Existing Datasets on Asimov": 1,
            "Pull Datasets": 1,
            "Run Algorithm Benchmarks": 1,
            "Analyze Missing Combinations": 1,
            # run_single_combination tasks live inside run_algorithm_benchmarks subflow.
        }
    )
    assert task_name_counts == expected_task_name_counts, (
        "Pipeline task structure has changed!\n"
        "  Actual:   " + str(dict(task_name_counts.most_common())) + "\n"
        "  Expected: " + str(dict(expected_task_name_counts.most_common()))
    )
    # 3 subflows: evaluate_datasets_flow + pull_datasets_flow + run_algorithm_benchmarks_flow
    assert len(subflow_runs) == 3, (
        f"Expected 3 subflow runs, got {len(subflow_runs)}.\n"
        + "\n".join(f"  {fr.name!r}" for fr in subflow_runs)
    )

    # ── Filesystem assertions ─────────────────────────────────────────────────
    # Containers present on Alexandria.
    assert (alex / "mock" / "containers" / "casanovo" / "v4.2.1" / "container.sif").exists()
    assert (alex / "mock" / "containers" / "adanovo" / "bm-1.0.0" / "container.sif").exists()
    assert (asimov / "evaluation.sif").exists()
    # No outputs (runs failed).
    assert not list((alex / "mock" / "outputs").rglob("*.csv")), (
        "No output files should exist — all runs failed"
    )
    # Dataset left marked available (cleanup only happens after successful runs).
    import json

    state_data = json.loads((asimov / "dataset_state.json").read_text())
    assert state_data.get("test_dataset_human", {}).get("status") == "available", (
        "Dataset should remain marked available when runs fail (no cleanup)"
    )


def test_pipeline_evaluation_fails(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: builds, pull, and algorithm runs succeed, but the evaluation job fails.

    Expected pipeline behaviour:
      - evaluate_dataset_task raises Exception inside run_algorithm_benchmarks_flow;
        the subflow catches it and continues (graceful degradation).
      - cleanup_dataset_task still runs (it runs whenever all ALGO runs succeed,
        regardless of evaluation result).
      - run_algorithm_benchmarks_flow returns (2, 2) → failed_runs=0 → continue.
      - Loop iter 2: all outputs present → break.
      - Result CSVs are NOT created (evaluation failed).
      - dataset_state.json = {} (cleanup removed the dataset entry).
    """
    scenario = FRESH_START
    algorithms = [
        {"name": "casanovo", "version": "v4.2.1"},
        {"name": "adanovo", "version": "bm-1.0.0"},
    ]
    asimov = _setup_mutable_asimov(scenario, tmp_path)
    alex = _setup_mutable_alexandria(scenario, tmp_path)
    benchmarks_dir = asimov / "denovo_benchmarks"
    config = _make_config(benchmarks_dir)

    with ExitStack() as stack:
        _enter_infra_patches(stack, asimov, alex, config)
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_build",
                side_effect=_build_creates_container(alex),
            )
        )
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_run",
                side_effect=_run_creates_output(alex, algorithms),
            )
        )
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_pull",
                side_effect=_pull_marks_available(),
            )
        )
        # Evaluation job fails.
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_evaluation",
                return_value=(False, "mock_job_eval"),
            )
        )

        with prefect_test_harness():
            from orchestrate import main

            state = main(return_state=True)
            flow_run_id = str(state.state_details.flow_run_id)
            task_runs, subflow_runs = asyncio.run(_fetch_all_run_states(flow_run_id))

    # ── Prefect state assertions ──────────────────────────────────────────────
    assert state.is_completed(), f"Top-level flow did not complete. State: {state.type}"

    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)
    expected_task_name_counts = Counter(
        {
            # Pre-loop + iter 1 + iter 2 = 3
            "Check Alexandria Outputs": 3,
            # iter 1 (missing) + iter 2 (all present → break) = 2
            "Analyze Missing Combinations": 2,
            # Pre-build only; post-build re-check calls raw check_containers() not the task = 1
            # Post-build re-check does call check_evaluation_container_task (task wrapper) = 2
            "Check Containers existance on Alexandria": 1,
            "Check Evaluation Container on Alexandria": 2,
            "Analyze Container Build Status": 1,
            "Discover available algorithms": 1,
            "Ensure denovo-benchmarks Repository": 1,
            "Build Container: casanovo": 1,
            "Build Container: adanovo": 1,
            "Build Container: evaluation": 1,
            "Pull Evaluation Container": 1,
            "Check Outputs Needing Augmentation": 1,
            "Find Datasets Needing Evaluation": 1,
            "Evaluate Datasets": 1,
            # Loop iter 1
            "Scan Existing Datasets on Asimov": 1,
            "Pull Datasets": 1,
            "Run Algorithm Benchmarks": 1,
            # iter 2 breaks at Analyze Missing Combinations before scan/pull/run
        }
    )
    assert task_name_counts == expected_task_name_counts, (
        "Pipeline task structure has changed!\n"
        "  Actual:   " + str(dict(task_name_counts.most_common())) + "\n"
        "  Expected: " + str(dict(expected_task_name_counts.most_common()))
    )
    assert len(subflow_runs) == 3, (
        f"Expected 3 subflow runs, got {len(subflow_runs)}.\n"
        + "\n".join(f"  {fr.name!r}" for fr in subflow_runs)
    )

    # ── Filesystem assertions ─────────────────────────────────────────────────
    # Outputs exist on Alexandria (runs succeeded).
    assert (
        alex / "mock" / "outputs" / "casanovo" / "v4.2.1" / "test_dataset_human" / "output.csv"
    ).exists()
    assert (
        alex / "mock" / "outputs" / "adanovo" / "bm-1.0.0" / "test_dataset_human" / "output.csv"
    ).exists()
    # No result CSVs (evaluation failed).
    assert not (benchmarks_dir / "results" / "test_dataset_human").exists(), (
        "results/ directory must not exist — evaluation never completed"
    )
    # Dataset cleaned up despite eval failure (cleanup runs whenever algo runs succeed).

    assert (asimov / "dataset_state.json").read_bytes() == b"{}", (
        "cleanup_dataset_task runs even when evaluation fails"
    )


def test_pipeline_git_pull_fails(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: git reports the repo is 3 commits behind but the pull fails
    (no real remote on the tmp git repo).

    Expected pipeline behaviour:
      - check_or_clone_repo() calls git_pull_rebase(); it fails (no configured
        remote) and returns False.  The task logs a warning and returns False
        — no crash, no raised exception.
      - Pipeline continues normally: all outputs already present on Alexandria
        (all_results_ready scenario), so the loop breaks on the first iteration.
      - Task counts are identical to test_pipeline_all_complete.
    """
    scenario = ALL_RESULTS_READY
    asimov = _setup_mutable_asimov(scenario, tmp_path)
    alex = _setup_mutable_alexandria(scenario, tmp_path)
    benchmarks_dir = asimov / "denovo_benchmarks"
    config = _make_config(benchmarks_dir)

    with ExitStack() as stack:
        # git_count_behind=3 triggers the pull path; git_pull_rebase runs for real
        # on the tmp repo (no remote → fails gracefully, returns False).
        _enter_infra_patches(stack, asimov, alex, config, git_count_behind=3)
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_build", _slurm_must_not_run("build"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_run", _slurm_must_not_run("run"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_pull", _slurm_must_not_run("pull"))
        )
        stack.enter_context(
            patch("orchestrate.submit_and_wait_for_evaluation", _slurm_must_not_run("evaluate"))
        )

        with prefect_test_harness():
            from orchestrate import main

            state = main(return_state=True)
            flow_run_id = str(state.state_details.flow_run_id)
            task_runs, subflow_runs = asyncio.run(_fetch_all_run_states(flow_run_id))

    # ── Prefect state assertions ──────────────────────────────────────────────
    assert state.is_completed(), f"Top-level flow did not complete. State: {state.type}"

    # All tasks completed (git pull failure is logged, not raised as exception).
    failed_tasks = [tr for tr in task_runs if not tr.state.is_completed()]
    assert not failed_tasks, "All tasks must complete despite git pull failure:\n" + "\n".join(
        f"  {tr.name!r}: {tr.state.type}" for tr in failed_tasks
    )

    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)
    # Identical to test_pipeline_all_complete — pipeline proceeds normally
    # after the failed pull.
    expected_task_name_counts = Counter(
        {
            "Check Alexandria Outputs": 2,
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
        "  Expected: " + str(dict(expected_task_name_counts.most_common()))
    )
    assert len(subflow_runs) == 1, (
        f"Expected 1 subflow run (evaluate_datasets_flow), got {len(subflow_runs)}."
    )

    # ── Filesystem assertions ─────────────────────────────────────────────────
    # evaluation.sif pulled from Alexandria despite failed git pull.
    assert (asimov / "evaluation.sif").exists()


def test_pipeline_fresh_start(
    tmp_path: Path,
    clean_build_state,
) -> None:
    """
    Scenario: nothing exists anywhere — Alexandria is empty, no containers, no
    outputs, no result CSVs.  dataset_state.json is empty; the dataset is NOT
    pre-available on Asimov so the pipeline pulls it before running algorithms.

    Static fixtures: tests/test_datasets/fresh_start/
      git/                 → algorithms only (no results — none exist yet)
      asimov/              → dataset_state.json = {} (no datasets present)
      alexandria/          → empty
      expected_git/        → algorithms only (evaluation doesn't run this invocation;
                             results are created in a subsequent pipeline run after
                             outputs exist on Alexandria)
      expected_asimov/     → evaluation.sif pulled from Alexandria
                             + dataset_state.json = {} (cleaned up after processing)
      expected_alexandria/ → all containers built + all algo outputs created

    Expected pipeline behaviour:
      - check_outputs() → empty.
      - check_containers() → all missing.
      - check_evaluation_container() → missing.
      - build_single_container submitted for every algo + evaluation  (3 total).
      - After builds: re-check → all containers present; eval exists.
      - pull_evaluation_container() → rsync from Alexandria to asimov/evaluation.sif.
      - get_outputs_needing_augmentation() → nothing (no existing outputs).
      - find_datasets_needing_evaluation() → nothing (no existing outputs).
      - Main loop iter 1: scan finds no datasets → pull_datasets_flow called →
        pull_single_dataset_task marks test_dataset_human as available.
      - Run all algo×dataset combos; submit_and_wait_for_run creates output.csv.
      - cleanup_dataset_task removes dataset from state → dataset_state.json = {}.
      - Main loop iter 2: all outputs present → nothing missing → break.
      - No evaluation happens this run (outputs existed only after the loop ran).
    """
    scenario = FRESH_START
    algorithms = [
        {"name": "casanovo", "version": "v4.2.1"},
        {"name": "adanovo", "version": "bm-1.0.0"},
    ]

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
        # ── Point get_runner_dir() → asimov (for eval container path + DatasetManager) ─
        stack.enter_context(patch("runner.algorithm_runner.get_runner_dir", return_value=asimov))
        stack.enter_context(patch("runner.dataset_manager.get_runner_dir", return_value=asimov))
        # ── Scaffolding ───────────────────────────────────────────────────────
        stack.enter_context(patch("orchestrate.load_config", return_value=config))
        stack.enter_context(patch("orchestrate.cleanup_workspace", new=MagicMock()))
        # ── Slurm: build creates containers on Alexandria ─────────────────────
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_build",
                side_effect=_build_creates_container(alex),
            )
        )
        # ── Slurm: run creates augmented output.csv on Alexandria ─────────────
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_run",
                side_effect=_run_creates_output(alex, algorithms),
            )
        )
        # ── Slurm: evaluate creates result CSVs in benchmarks_dir/results/ ───
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_evaluation",
                side_effect=_evaluate_creates_results(benchmarks_dir),
            )
        )
        # ── Slurm: pull marks dataset as available in DatasetManager ────────
        stack.enter_context(
            patch(
                "orchestrate.submit_and_wait_for_pull",
                side_effect=_pull_marks_available(),
            )
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
    task_name_counts = Counter(tr.name.rsplit("-", 1)[0] for tr in task_runs)

    expected_task_name_counts = Counter(
        {
            # Pre-loop once; loop iter 1 (missing outputs); loop iter 2 (all done) = 3
            "Check Alexandria Outputs": 3,
            # Loop iter 1 (missing → runs algo) + iter 2 (nothing missing → break) = 2
            "Analyze Missing Combinations": 2,
            # Initial check + re-check after builds complete = 2
            "Check Evaluation Container on Alexandria": 2,
            # Proxy task for run_algorithm_benchmarks_flow subflow = 1
            "Run Algorithm Benchmarks": 1,
            # Proxy task for evaluate_datasets_flow subflow (empty list → no-op) = 1
            "Evaluate Datasets": 1,
            # Loop iter 1 only (iter 2 breaks before scan) = 1
            "Scan Existing Datasets on Asimov": 1,
            # Proxy task for pull_datasets_flow subflow = 1
            # (pull_single_dataset_task runs inside the subflow, not tracked here)
            "Pull Datasets": 1,
            # Pre-loop (post-build) — no existing outputs → not needed  = 1
            "Find Datasets Needing Evaluation": 1,
            # Pre-loop (post-build) — no existing outputs → nothing to augment = 1
            "Check Outputs Needing Augmentation": 1,
            # Post-build once = 1
            "Pull Evaluation Container": 1,
            # build_single_container for each algo + evaluation = 3
            "Build Container: evaluation": 1,
            "Build Container: casanovo": 1,
            "Build Container: adanovo": 1,
            # Pre-loop once each = 1
            "Analyze Container Build Status": 1,
            "Check Containers existance on Alexandria": 1,
            "Discover available algorithms": 1,
            "Ensure denovo-benchmarks Repository": 1,
        }
    )

    assert task_name_counts == expected_task_name_counts, (
        "Pipeline task structure has changed!\n"
        "  Actual:   " + str(dict(task_name_counts.most_common())) + "\n"
        "  Expected: " + str(dict(expected_task_name_counts.most_common())) + "\n"
        "Update the expected_task_name_counts dict if the change is intentional."
    )

    # 3 subflows: evaluate_datasets_flow (pre-loop, empty) +
    #             pull_datasets_flow (iter 1, pulls test_dataset_human) +
    #             run_algorithm_benchmarks_flow (iter 1, runs all combos)
    # Note: pull_single_dataset_task, run_single_combination and evaluate_dataset_task
    # run inside subflows — they appear under those subflows' flow_run_ids, not the
    # main flow's, so they are not counted here.
    assert len(subflow_runs) == 3, (
        f"Expected 3 subflow runs, got {len(subflow_runs)}.\n"
        + "\n".join(f"  {fr.name!r}" for fr in subflow_runs)
    )

    # ── Filesystem state assertions ───────────────────────────────────────────
    _assert_matches_expected(benchmarks_dir, scenario / "expected_git", "git")
    _assert_matches_expected(asimov, scenario / "expected_asimov", "asimov")
    _assert_matches_expected(alex, scenario / "expected_alexandria", "alexandria")

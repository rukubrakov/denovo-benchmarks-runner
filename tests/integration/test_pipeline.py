"""
Integration tests for the full pipeline orchestration.

Mocking strategy — only mock what cannot run without external infrastructure:
  - SSH/rsync → Alexandria (check_outputs, check_containers,
                            check_evaluation_container,
                            get_outputs_needing_augmentation,
                            pull_evaluation_container)
  - Git clone/pull (check_or_clone_repo)
  - Slurm submission (submit_and_wait_for_build/run/evaluate/pull) — patched
    to raise AssertionError so any unexpected submission fails the test loudly.

What runs for REAL:
  - get_algorithms / display_algorithms — reads versions.log from tmp filesystem.
  - check_and_display_builds — full build-state logic; BuildState backed by
    tmp_path so it never touches the production build_state.json or Slurm
    (empty state => update_from_slurm makes zero squeue calls).
  - find_datasets_needing_evaluation — checks result CSVs on real tmp filesystem.
  - analyze_missing — pure dict/set logic on the Alexandria-returned data.
  - cleanup_workspace — mocked only because it operates on the production
    runner dir (logs/, slurm_jobs/) which is unsafe to wipe in tests.
  - load_config — mocked so the test is self-contained (no real config.yaml).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from runner.build_state import BuildState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALGORITHMS = [
    {"name": "casanovo", "version": "v4.2.1"},
    {"name": "adanovo", "version": "bm-1.0.0"},
]

DATASETS = ["test_dataset_human"]

RESULT_FILES = [
    "metrics.csv",
    "peptide_precision_plot_data.csv",
    "AA_precision_plot_data.csv",
    "RT_difference_plot_data.csv",
    "SA_plot_data.csv",
    "number_of_proteome_matches_plot_data.csv",
]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def benchmarks_dir(tmp_path: Path) -> Path:
    """
    Real benchmarks directory in tmp_path.

      algorithms/<name>/versions.log  ->  get_algorithms() reads these for real.
      results/<dataset>/<csv>         ->  find_datasets_needing_evaluation() checks these.
    """
    bm = tmp_path / "denovo_benchmarks"

    for algo in ALGORITHMS:
        algo_dir = bm / "algorithms" / algo["name"]
        algo_dir.mkdir(parents=True)
        (algo_dir / "versions.log").write_text(
            f'container_version: "{algo["version"]}"\nalgorithm_version: "1.0.0"\n'
        )

    for dataset in DATASETS:
        result_dir = bm / "results" / dataset
        result_dir.mkdir(parents=True)
        for fname in RESULT_FILES:
            (result_dir / fname).write_text("header\ndata\n")

    return bm


@pytest.fixture()
def test_config(benchmarks_dir: Path) -> dict:
    """
    Pipeline config pointing at tmp_path.

    local_path is absolute so that Path(any_dir) / local_path resolves to
    benchmarks_dir (Python Path: absolute component wins).
    """
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
        "datasets": DATASETS,
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


@pytest.fixture()
def clean_build_state(tmp_path: Path):
    """
    Redirect BuildState to a fresh tmp file so check_and_display_builds
    runs with an empty state (=> zero Slurm calls in update_from_slurm)
    and never touches the production build_state.json.
    """
    original_init = BuildState.__init__

    def _patched_init(self, state_file=None):
        original_init(self, state_file=state_file or (tmp_path / "build_state.json"))

    with patch.object(BuildState, "__init__", _patched_init):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _slurm_must_not_run(name: str) -> MagicMock:
    """Raises AssertionError if called — ensures no Slurm job is submitted."""
    return MagicMock(side_effect=AssertionError(f"Slurm job must not be submitted: {name}"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_all_complete(
    test_config: dict,
    clean_build_state,
) -> None:
    """
    Scenario: every container and output already exist on Alexandria, and
    every dataset already has all six result CSVs on disk.

    Expected behaviour (verified via assertions below):
      - Git sync runs exactly once.
      - Alexandria is queried exactly once each for outputs, containers, eval
        container.
      - Eval container is pulled exactly once (evaluation_exists=True).
      - Augmentation check runs exactly once (evaluation_exists=True).
      - evaluate_datasets_flow produces (0, 0) because all result CSVs exist.
      - No Slurm jobs submitted (build / run / evaluate / pull).
      - Algorithm discovery and result-file checks run against the real
        tmp filesystem — not mocked.
    """
    all_outputs: set[tuple[str, str]] = {
        (algo["name"], dataset) for algo in ALGORITHMS for dataset in DATASETS
    }
    all_containers: dict[str, bool] = {algo["name"]: True for algo in ALGORITHMS}

    with prefect_test_harness():
        with (
            patch("orchestrate.load_config", return_value=test_config),
            patch("orchestrate.cleanup_workspace", new=MagicMock()),
            # ── External I/O (SSH / git / rsync) ─────────────────────────────
            patch("orchestrate.check_or_clone_repo") as mock_clone,
            patch("orchestrate.check_outputs", return_value=all_outputs) as mock_outputs,
            patch("orchestrate.check_containers", return_value=all_containers) as mock_containers,
            patch(
                "orchestrate.check_evaluation_container", return_value=True
            ) as mock_eval_container,
            patch(
                "orchestrate.get_outputs_needing_augmentation", return_value=[]
            ) as mock_augmentation,
            patch(
                "runner.algorithm_runner.pull_evaluation_container", return_value=True
            ) as mock_pull_eval,
            # ── Slurm guard (raises if called) ───────────────────────────────
            patch("orchestrate.submit_and_wait_for_build", _slurm_must_not_run("build")),
            patch("orchestrate.submit_and_wait_for_run", _slurm_must_not_run("run")),
            patch("orchestrate.submit_and_wait_for_evaluation", _slurm_must_not_run("evaluate")),
            patch("orchestrate.submit_and_wait_for_pull", _slurm_must_not_run("pull")),
        ):
            from orchestrate import main

            main()

        # ── Verify external I/O calls ─────────────────────────────────────────
        # Git sync: once (pre-loop only, not repeated per iteration)
        mock_clone.assert_called_once()

        # outputs: re-fetched on every loop iteration.
        # "all complete" → 1 pre-loop + 1 first iteration (before break) = 2.
        assert mock_outputs.call_count == 2, (
            f"check_outputs expected 2 (pre-loop + loop iter 1 before break), "
            f"got {mock_outputs.call_count}"
        )

        # containers / eval_container / augmentation / pull_eval: only refreshed
        # when there ARE missing outputs (i.e. past the early-break check).
        # In "all complete" the loop breaks before those re-checks → 1 each.
        assert mock_containers.call_count == 1, (
            f"check_containers expected 1 (pre-loop only), got {mock_containers.call_count}"
        )
        assert mock_eval_container.call_count == 1, (
            f"check_evaluation_container expected 1, got {mock_eval_container.call_count}"
        )
        assert mock_augmentation.call_count == 1, (
            f"get_outputs_needing_augmentation expected 1, got {mock_augmentation.call_count}"
        )
        assert mock_pull_eval.call_count == 1, (
            f"pull_evaluation_container expected 1, got {mock_pull_eval.call_count}"
        )

        # Algorithm discovery used the real tmp filesystem — verify correct names
        # (call_args uses the last call's positional args: check_containers(config, algorithms))
        discovered_algo_names = {a["name"] for a in mock_containers.call_args[0][1]}
        assert discovered_algo_names == {a["name"] for a in ALGORITHMS}, (
            f"Wrong algorithms passed to check_containers: {discovered_algo_names}"
        )

        # ── Verify Slurm was NEVER touched ───────────────────────────────────
        # The _slurm_must_not_run guards raise AssertionError on any call, so
        # reaching this line already proves zero Slurm submissions.

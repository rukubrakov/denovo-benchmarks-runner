"""Unit tests for algorithm_runner utilities."""

from runner.algorithm_runner import get_slurm_resources_for_run


class TestGetSlurmResourcesForRun:
    """Tests for get_slurm_resources_for_run."""

    def test_returns_defaults_when_slurm_key_missing(self):
        result = get_slurm_resources_for_run({})
        assert result == {
            "partition": "one_hour",
            "cpus": 4,
            "memory": "16G",
            "gpus": 1,
            "time": "01:00:00",
        }

    def test_returns_defaults_when_slurm_section_empty(self):
        result = get_slurm_resources_for_run({"slurm": {}})
        assert result["partition"] == "one_hour"
        assert result["cpus"] == 4
        assert result["memory"] == "16G"
        assert result["gpus"] == 1
        assert result["time"] == "01:00:00"

    def test_overrides_all_values_from_config(self):
        config = {
            "slurm": {
                "partition": "gpu_long",
                "cpus": 8,
                "memory": "32G",
                "gpus": 2,
                "time": "04:00:00",
            }
        }
        result = get_slurm_resources_for_run(config)
        assert result == {
            "partition": "gpu_long",
            "cpus": 8,
            "memory": "32G",
            "gpus": 2,
            "time": "04:00:00",
        }

    def test_partial_override_uses_defaults_for_missing_keys(self):
        config = {"slurm": {"partition": "long", "memory": "64G"}}
        result = get_slurm_resources_for_run(config)
        assert result["partition"] == "long"
        assert result["memory"] == "64G"
        assert result["cpus"] == 4
        assert result["gpus"] == 1
        assert result["time"] == "01:00:00"

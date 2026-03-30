# Test Datasets

Static, committed fixture directories for integration tests.

Each subdirectory represents one test **scenario** and contains up to five
sub-folders that mirror the real system layout:

| Folder | Description |
|---|---|
| `git/` | Contents of the `denovo_benchmarks` git checkout (algorithms, results, …). Read-only; copied + git-initialised into a temp dir at test time. |
| `asimov/` | Asimov (compute server) filesystem state *before* the test runs. Merged on top of the `git/` copy inside `tmp_path`. `.sif` files that should already be present locally live here. |
| `alexandria/` | Alexandria (storage server) filesystem state *before* the test runs. Remote SSH/rsync primitives are replaced with local-filesystem equivalents pointing at this directory. |
| `expected_asimov/` | Expected Asimov state *after* running the scenario. Compared against the mutable copy at test teardown. |
| `expected_alexandria/` | Expected Alexandria state *after* running the scenario. Compared against the mutable copy at test teardown. |

## Scenarios

### `all_results_ready/`

Every container and output already exists on Alexandria, and every dataset
already has all result CSVs on disk (in the git checkout / Asimov).

- **Expected pipeline behaviour**: no Slurm jobs are submitted.
- **Expected filesystem changes**: none — `expected_asimov` and
  `expected_alexandria` are identical to the initial state.

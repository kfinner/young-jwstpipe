# Reduction Wrapper Pipeline

This document describes how the wrapped YOUNG JWST reduction pipeline currently works in `/Users/kfinner/Documents/GitHub/young-jwstpipe-wrapped`.

## Main entry points

- `download_and_reduce.py`
- `young_pipeline.sh`
- `utils/pipeline_stage1.py`
- `utils/pipeline_stage2.py`
- `utils/pipeline_stage3.py`

## High-level flow

1. Parse target names and load the base YAML config.
2. Resolve the output directory for this run.
3. Download or reuse cached `*_uncal.fits` files.
4. Stage the files into a temporary per-run directory.
5. Generate a temporary run-specific config.
6. Launch `young_pipeline.sh` with the active Python environment on `PATH`.
7. Archive logs and clean up temporary data after a successful run.

## Input data

The wrapper queries MAST for public JWST NIRCam imaging observations and selects `*_uncal.fits` science products.

## Cache layout

The wrapper uses:
- `download_cache/targets/` for reusable cached target files
- `download_cache/_mast_downloads/` for the MAST download cache
- a temporary `download_cache/young_pipeline_targets_*` directory for the per-run staging area

Default behavior:
- cached raw files are kept after success
- staging data is removed after success
- generated configs are removed after success

Optional behavior:
- `--delete-cached-files` deletes cached raw files for the targets in that run after a successful reduction
- `--keep-staging-data` preserves staging data
- `--keep-generated-config` preserves the generated YAML config

## Logging and provenance

The wrapper writes:
- `download_cache/last_run_command.txt`
- a command-history entry into `<output-dir>/ANALYSIS_SUMMARY.md`

Each summary entry includes:
- timestamp
- working directory
- exact command

## Runtime environment behavior

The wrapper now explicitly:
- uses the active interpreter's `bin` directory in `PATH`
- clears stale `CRDS_CONTEXT`
- checks for required runtime tools such as `python`, `crds`, and `yq`

## Pipeline stages

### Stage 1
Handled by `utils/pipeline_stage1.py`.

Current compatibility behavior for modern JWST releases includes:
- skip `clean_flicker_noise`
- use median `refpix`
- skip `dark_current`
- cap worker count internally for stability on this machine

### Stage 2
Handled by `utils/pipeline_stage2.py`.

Current wrapper behavior:
- caps worker count internally
- exits cleanly if no `rate.fits` files are present

### Stage 3
Handled by `utils/pipeline_stage3.py`.

Current wrapper behavior:
- archives older Stage 3 logs on rerun
- keeps per-filter Stage 3 logs
- deletes `*_asn_crf.fits` intermediates after successful reduction

## Output cleanup after success

After a successful reduction, `young_pipeline.sh` currently deletes:
- `stage1_output/`
- `stage2_output/`
- Stage 3 `*_asn_crf.fits`

It preserves:
- final Stage 3 products
- logs
- archived logs from earlier reruns

## Typical command

```bash
python /Users/kfinner/Documents/GitHub/young-jwstpipe-wrapped/download_and_reduce.py "PLCKG165+67.0" --config ./config.yaml --skip-download --output-dir "$PWD/PLCKG165+67.0"
```

## Known operational notes

- Overly high parallel settings can destabilize the run on this machine.
- The wrapper now records provenance automatically, but the global `download_cache/last_run_command.txt` is still not cluster-specific.
- The cluster-specific provenance file should be treated as the authoritative record.

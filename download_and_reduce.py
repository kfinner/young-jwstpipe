#!/usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = REPO_ROOT / "config.yaml"
DEFAULT_CACHE_ROOT = REPO_ROOT / "download_cache"
RAW_DOWNLOAD_DIRNAME = "_mast_downloads"
TARGET_CACHE_DIRNAME = "targets"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Download public JWST NIRCam imaging UNCAL products for one or more "
            "targets, then run the YOUNG JWST reduction pipeline."
        )
    )
    parser.add_argument(
        "targets",
        nargs="*",
        help="Target names. You can pass multiple values or comma-separated values.",
    )
    parser.add_argument(
        "--targets-file",
        type=Path,
        help="Optional file with one target per line.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Base pipeline config to clone for this run.",
    )
    parser.add_argument(
        "--download-root",
        type=Path,
        default=DEFAULT_CACHE_ROOT,
        help="Directory used for the download cache and per-run staging data.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Override output_directory for this run.",
    )
    parser.add_argument(
        "--query-radius-arcsec",
        type=float,
        default=30.0,
        help="Fallback cone-search radius in arcseconds if an exact target-name query finds nothing.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Reuse cached target directories and fail if a requested target has not been downloaded yet.",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Download or stage the input data but do not launch the reduction pipeline.",
    )
    parser.add_argument(
        "--keep-staging-data",
        action="store_true",
        help="Keep the temporary per-run staging directory after a successful run.",
    )
    parser.add_argument(
        "--keep-generated-config",
        action="store_true",
        help="Keep the generated config file after a successful run.",
    )
    return parser.parse_args()


def load_target_list(args):
    targets = []

    for raw_target in args.targets:
        targets.extend(part.strip() for part in raw_target.split(","))

    if args.targets_file:
        for line in args.targets_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                targets.append(stripped)

    deduped_targets = []
    seen = set()
    for target in targets:
        if target and target not in seen:
            deduped_targets.append(target)
            seen.add(target)

    if not deduped_targets:
        raise SystemExit("No targets were provided. Pass at least one target name or use --targets-file.")

    return deduped_targets


def sanitize_target_name(target):
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", target.strip())
    slug = slug.strip("._-")
    return slug or "target"


def ensure_astroquery():
    try:
        from astroquery.mast import Observations
    except ImportError as exc:
        raise SystemExit(
            "astroquery is required for downloads. Install it with `pip install astroquery` "
            "in the pipeline environment."
        ) from exc

    return Observations


def ensure_yaml():
    try:
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "PyYAML is required to read and write pipeline configs. Install it with `pip install pyyaml` "
            "in the pipeline environment."
        ) from exc

    return yaml


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip().upper()


def row_value(row, column_name):
    if column_name in row.colnames:
        return row[column_name]
    return None


def filter_nircam_imaging_observations(observations):
    if len(observations) == 0:
        return observations

    keep_indices = []
    for index, row in enumerate(observations):
        obs_collection = normalize_text(row_value(row, "obs_collection"))
        dataproduct_type = normalize_text(row_value(row, "dataproduct_type"))
        instrument_name = normalize_text(row_value(row, "instrument_name"))
        intent_type = normalize_text(row_value(row, "intentType"))

        if obs_collection != "JWST":
            continue
        if dataproduct_type != "IMAGE":
            continue
        if "NIRCAM" not in instrument_name:
            continue
        if intent_type and intent_type != "SCIENCE":
            continue

        keep_indices.append(index)

    return observations[keep_indices]


def query_target_observations(Observations, target, radius_arcsec):
    observations = Observations.query_criteria(obs_collection="JWST", target_name=target)
    filtered = filter_nircam_imaging_observations(observations)
    if len(filtered) > 0:
        return filtered, "target_name"

    fallback_radius = f"{radius_arcsec} arcsec"
    observations = Observations.query_object(target, radius=fallback_radius)
    filtered = filter_nircam_imaging_observations(observations)
    return filtered, f"cone search ({fallback_radius})"


def dedupe_products(products):
    seen = set()
    keep_indices = []
    for index, row in enumerate(products):
        key = row_value(row, "dataURI") or row_value(row, "productFilename") or row_value(row, "obsID")
        if key in seen:
            continue
        seen.add(key)
        keep_indices.append(index)
    return products[keep_indices]


def query_target_products(Observations, target, radius_arcsec):
    observations, query_mode = query_target_observations(Observations, target, radius_arcsec)
    if len(observations) == 0:
        raise RuntimeError(
            f"No public JWST NIRCam imaging observations were found for target '{target}'."
        )

    products = Observations.get_product_list(observations)
    filtered_products = Observations.filter_products(
        products,
        productType="SCIENCE",
        productSubGroupDescription="UNCAL",
        extension="fits",
    )

    keep_indices = []
    for index, row in enumerate(filtered_products):
        filename = str(row_value(row, "productFilename") or "")
        if filename.endswith("_uncal.fits"):
            keep_indices.append(index)

    uncal_products = filtered_products[keep_indices]

    unique_products = dedupe_products(uncal_products)
    if len(unique_products) == 0:
        raise RuntimeError(
            f"JWST observations were found for '{target}', but no NIRCam *_uncal.fits products were available."
        )

    return unique_products, query_mode


def ensure_directory(path):
    path.mkdir(parents=True, exist_ok=True)
    return path


def link_or_copy_file(source, destination):
    if destination.exists():
        return

    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


def download_target_data(Observations, target, cache_root, raw_download_root, radius_arcsec, skip_download):
    target_slug = sanitize_target_name(target)
    cache_dir = cache_root / f"MAST_{target_slug}" / target_slug
    ensure_directory(cache_dir)

    cached_uncal_files = sorted(cache_dir.glob("*_uncal.fits"))
    if cached_uncal_files:
        print(f"[cache] Reusing {len(cached_uncal_files)} cached files for {target}")
        return target_slug, cache_dir, cached_uncal_files

    if skip_download:
        raise RuntimeError(
            f"No cached files were found for '{target}' in {cache_dir}, and --skip-download was requested."
        )

    products, query_mode = query_target_products(Observations, target, radius_arcsec)
    print(f"[query] {target}: {len(products)} UNCAL products selected via {query_mode}")

    manifest = Observations.download_products(
        products,
        download_dir=str(raw_download_root),
        cache=True,
    )

    downloaded_files = []
    failed_rows = []
    for row in manifest:
        status = str(row_value(row, "Status") or "").upper()
        local_path = row_value(row, "Local Path")
        if status not in {"COMPLETE", "ALREADY_DOWNLOADED"}:
            failed_rows.append(row)
            continue
        if not local_path:
            failed_rows.append(row)
            continue

        source_path = Path(str(local_path)).expanduser().resolve()
        destination_path = cache_dir / source_path.name
        link_or_copy_file(source_path, destination_path)
        downloaded_files.append(destination_path)

    if failed_rows:
        details = ", ".join(
            f"{row_value(row, 'productFilename') or 'unknown'} [{row_value(row, 'Status') or 'UNKNOWN'}]"
            for row in failed_rows[:5]
        )
        raise RuntimeError(f"Some downloads failed for '{target}': {details}")

    if not downloaded_files:
        raise RuntimeError(f"No files were downloaded for '{target}'.")

    return target_slug, cache_dir, sorted(downloaded_files)


def stage_target_files(target_slug, cached_files, staging_root):
    staged_dir = ensure_directory(staging_root / f"MAST_{target_slug}" / target_slug)
    for cached_file in cached_files:
        link_or_copy_file(cached_file, staged_dir / cached_file.name)
    return staged_dir


def resolve_output_dir(base_config, base_config_path, explicit_output_dir):
    if explicit_output_dir:
        return explicit_output_dir.expanduser().resolve()

    configured_output = base_config.get("output_directory")
    if configured_output:
        output_path = Path(configured_output).expanduser()
        if not output_path.is_absolute():
            output_path = (base_config_path.parent / output_path).resolve()
        return output_path

    return REPO_ROOT


def build_generated_config(base_config, base_config_path, staging_root, output_dir):
    generated_config = dict(base_config)
    generated_config["pipeline_directory"] = str(REPO_ROOT)
    generated_config["data_directory"] = str(staging_root)
    generated_config["output_directory"] = str(output_dir)
    generated_config["combine_observations"] = False
    generated_config["group_by_directory"] = True
    generated_config["dir_prefix"] = "MAST_"
    return generated_config


def write_generated_config(config, directory):
    yaml = ensure_yaml()
    ensure_directory(directory)
    config_fd, config_path = tempfile.mkstemp(
        prefix="young_pipeline_",
        suffix=".yaml",
        dir=str(directory),
    )
    os.close(config_fd)
    config_file = Path(config_path)
    config_file.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_file


def run_pipeline(config_path):
    env = os.environ.copy()
    env["YOUNG_PIPELINE_CONFIG"] = str(config_path)

    subprocess.run(
        ["bash", str(REPO_ROOT / "young_pipeline.sh")],
        cwd=str(REPO_ROOT),
        env=env,
        check=True,
    )


def main():
    args = parse_args()
    targets = load_target_list(args)
    yaml = ensure_yaml()

    base_config_path = args.config.expanduser().resolve()
    if not base_config_path.exists():
        raise SystemExit(f"Base config file not found: {base_config_path}")

    base_config = yaml.safe_load(base_config_path.read_text(encoding="utf-8")) or {}
    output_dir = resolve_output_dir(base_config, base_config_path, args.output_dir)

    download_root = ensure_directory(args.download_root.expanduser().resolve())
    target_cache_root = ensure_directory(download_root / TARGET_CACHE_DIRNAME)
    raw_download_root = ensure_directory(download_root / RAW_DOWNLOAD_DIRNAME)
    staging_root = Path(
        tempfile.mkdtemp(prefix="young_pipeline_targets_", dir=str(download_root))
    ).resolve()

    generated_config_path = None
    success = False

    try:
        Observations = ensure_astroquery()

        staged_targets = []
        for target in targets:
            target_slug, _, cached_files = download_target_data(
                Observations,
                target,
                target_cache_root,
                raw_download_root,
                args.query_radius_arcsec,
                args.skip_download,
            )
            staged_dir = stage_target_files(target_slug, cached_files, staging_root)
            staged_targets.append((target, staged_dir, len(cached_files)))

        print("[stage] Targets prepared for this run:")
        for target, staged_dir, file_count in staged_targets:
            print(f"  - {target}: {file_count} files -> {staged_dir}")

        generated_config = build_generated_config(
            base_config,
            base_config_path,
            staging_root,
            output_dir,
        )
        generated_config_path = write_generated_config(generated_config, download_root)
        print(f"[config] Generated run config: {generated_config_path}")

        if not args.skip_pipeline:
            run_pipeline(generated_config_path)
            print("[pipeline] Reduction complete.")
        else:
            print("[pipeline] Skipped pipeline execution as requested.")

        success = True
    finally:
        if success:
            if not args.keep_generated_config and generated_config_path and generated_config_path.exists():
                generated_config_path.unlink()
            if not args.keep_staging_data and staging_root.exists():
                shutil.rmtree(staging_root)
        else:
            print(
                f"[debug] Preserving staging data in {staging_root}"
                + (
                    f" and generated config {generated_config_path}"
                    if generated_config_path
                    else ""
                ),
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()

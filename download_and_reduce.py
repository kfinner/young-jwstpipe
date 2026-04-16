#!/usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import shlex
from shutil import which

from astroquery.exceptions import ResolverError
from astropy.coordinates import SkyCoord


SUMMARY_FILENAME = "ANALYSIS_SUMMARY.md"

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
        "--ra",
        type=float,
        help="Optional right ascension in decimal degrees for cone searches.",
    )
    parser.add_argument(
        "--dec",
        type=float,
        help="Optional declination in decimal degrees for cone searches.",
    )
    parser.add_argument(
        "--query-radius-arcsec",
        type=float,
        default=30.0,
        help="Cone-search radius in arcseconds used to gather nearby observations.",
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
    parser.add_argument(
        "--delete-cached-files",
        action="store_true",
        help="Delete cached raw files for the requested targets after a successful run.",
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
            "Failed to import astroquery in the active interpreter "
            f"({sys.executable}). Original error: {exc}"
        ) from exc

    return Observations


def ensure_yaml():
    try:
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "Failed to import PyYAML in the active interpreter "
            f"({sys.executable}). Original error: {exc}"
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


def dedupe_observations(observations):
    seen = set()
    keep_indices = []
    for index, row in enumerate(observations):
        key = (
            row_value(row, "obsid")
            or row_value(row, "obs_id")
            or row_value(row, "obsID")
            or row_value(row, "observationid")
            or row_value(row, "target_name")
        )
        if key in seen:
            continue
        seen.add(key)
        keep_indices.append(index)
    return observations[keep_indices]


def resolve_target_coordinates(target):
    try:
        coordinates = SkyCoord.from_name(target)
    except Exception:
        return None, None, None
    return float(coordinates.ra.deg), float(coordinates.dec.deg), coordinates.to_string("hmsdms")


def query_target_observations(Observations, target, radius_arcsec, ra=None, dec=None):
    target_matches = Observations.query_criteria(obs_collection="JWST", target_name=target)
    target_filtered = filter_nircam_imaging_observations(target_matches)

    fallback_radius = f"{radius_arcsec} arcsec"
    cone_query_available = True
    resolved_description = None
    used_manual_coordinates = ra is not None and dec is not None
    if ra is None or dec is None:
        resolved_ra, resolved_dec, resolved_description = resolve_target_coordinates(target)
        if resolved_ra is not None and resolved_dec is not None:
            ra = resolved_ra if ra is None else ra
            dec = resolved_dec if dec is None else dec
        elif len(target_filtered) == 0:
            raise RuntimeError(
                f"Could not resolve '{target}' to coordinates and no exact JWST NIRCam target-name matches were found. "
                "Try another target name or pass --ra/--dec explicitly."
            )

    if ra is not None and dec is not None:
        coordinates = f"{ra} {dec}"
        cone_matches = Observations.query_region(coordinates, radius=fallback_radius)
    else:
        try:
            cone_matches = Observations.query_object(target, radius=fallback_radius)
        except ResolverError:
            cone_matches = target_matches[:0]
            cone_query_available = False
        else:
            cone_query_available = True
    cone_filtered = filter_nircam_imaging_observations(cone_matches)

    cone_mode = f"cone search ({fallback_radius})"
    if used_manual_coordinates:
        cone_mode = f"cone search ({fallback_radius}; manual coordinates {ra}, {dec})"
    elif resolved_description:
        cone_mode = f"cone search ({fallback_radius}; resolved to {resolved_description})"

    if len(target_filtered) == 0 and len(cone_filtered) == 0:
        mode = f"target_name + {cone_mode}" if cone_query_available else "target_name"
        return target_filtered, mode
    if len(target_filtered) == 0:
        return dedupe_observations(cone_filtered), cone_mode
    if len(cone_filtered) == 0:
        return dedupe_observations(target_filtered), "target_name"

    from astropy.table import vstack

    combined = vstack([target_filtered, cone_filtered], metadata_conflicts="silent")
    combined = dedupe_observations(combined)
    return combined, f"target_name + {cone_mode}"


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


def observation_key(row):
    return (
        row_value(row, "obsid")
        or row_value(row, "obs_id")
        or row_value(row, "obsID")
        or row_value(row, "observationid")
    )


def product_observation_key(row):
    return (
        row_value(row, "obsID")
        or row_value(row, "obs_id")
        or row_value(row, "obsid")
        or row_value(row, "parent_obsid")
    )


def summarize_selected_products(observations, products):
    observation_lookup = {}
    for row in observations:
        key = observation_key(row)
        if key is None:
            continue
        observation_lookup[str(key)] = row

    counts = {}
    total_frames = 0
    for row in products:
        obs_key = product_observation_key(row)
        observation = observation_lookup.get(str(obs_key)) if obs_key is not None else None
        program = row_value(row, "proposal_id")
        if observation is not None:
            program = program or row_value(observation, "proposal_id") or row_value(observation, "proposalid")
            filters = row_value(observation, "filters") or row_value(observation, "filter")
        else:
            filters = row_value(row, "filters") or row_value(row, "filter")

        program_label = str(program).strip() if program not in (None, "") else "unknown"
        filter_label = str(filters).strip() if filters not in (None, "") else "unknown filter"
        counts.setdefault(program_label, {})
        counts[program_label][filter_label] = counts[program_label].get(filter_label, 0) + 1
        total_frames += 1

    summary_rows = []
    for program in sorted(counts, key=lambda value: (value == "unknown", value)):
        filter_summary = []
        for filter_name in sorted(counts[program]):
            frame_count = counts[program][filter_name]
            frame_label = "frame" if frame_count == 1 else "frames"
            filter_summary.append(f"{frame_count} {filter_name} {frame_label}")
        summary_rows.append({
            "program": program,
            "filters": ", ".join(filter_summary),
            "frame_total": sum(counts[program].values()),
        })
    return summary_rows, total_frames


def print_terminal_banner(target):
    label = f" JWST Lensing Pipeline: {target} "
    width = max(72, len(label) + 8)
    border = "=" * width
    inner = label.center(width)
    print()
    print(border)
    print(inner)
    print(border)
    print("   .        *             .      .      *")
    print("        .        .  JWST NIRCam Observation Check   .")
    print("   *       .         .         *        .        .")
    print()


def print_observation_summary_table(target, query_mode, summary_rows, total_frames):
    program_width = max(len("Program"), *(len(row["program"]) for row in summary_rows)) if summary_rows else len("Program")
    frames_width = max(len("Frames"), *(len(str(row["frame_total"])) for row in summary_rows)) if summary_rows else len("Frames")
    filters_width = max(len("Matched Filters"), *(len(row["filters"]) for row in summary_rows)) if summary_rows else len("Matched Filters")

    border = "+-" + "-" * program_width + "-+-" + "-" * frames_width + "-+-" + "-" * filters_width + "-+"
    header = f"| {'Program'.ljust(program_width)} | {'Frames'.ljust(frames_width)} | {'Matched Filters'.ljust(filters_width)} |"

    print_terminal_banner(target)
    print(f" Query mode : {query_mode}")
    print(f" Total UNCAL: {total_frames} candidate frames")
    print()
    print(border)
    print(header)
    print(border)
    for row in summary_rows:
        print(
            f"| {row['program'].ljust(program_width)} | {str(row['frame_total']).ljust(frames_width)} | {row['filters'].ljust(filters_width)} |"
        )
    print(border)
    print()


def confirm_target_selection(target, query_mode, summary_rows, total_frames):
    print_observation_summary_table(target, query_mode, summary_rows, total_frames)

    while True:
        response = input(f"Continue with download and processing for {target}? [y/N]: ").strip().lower()
        if response in {"", "n", "no"}:
            raise SystemExit(f"Aborted before downloading '{target}'. Try another target name if needed.")
        if response in {"y", "yes"}:
            return
        print("Please answer 'y' or 'n'.")


def query_target_products(Observations, target, radius_arcsec, ra=None, dec=None):
    observations, query_mode = query_target_observations(Observations, target, radius_arcsec, ra=ra, dec=dec)
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

    return unique_products, query_mode, observations


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


def download_target_data(Observations, target, cache_root, raw_download_root, radius_arcsec, skip_download, ra=None, dec=None):
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

    products, query_mode, observations = query_target_products(Observations, target, radius_arcsec, ra=ra, dec=dec)
    summary_rows, total_frames = summarize_selected_products(observations, products)
    confirm_target_selection(target, query_mode, summary_rows, total_frames)
    print(f"[query] {target}: {len(products)} UNCAL products selected for download")

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


def prune_empty_parents(path, stop_at):
    current = path
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def delete_cached_files(cache_dirs, cached_files, raw_download_root):
    removed_target_files = 0
    removed_raw_files = 0

    for cached_file in cached_files:
        if cached_file.exists():
            cached_file.unlink()
            removed_target_files += 1

        if raw_download_root.exists():
            try:
                raw_matches = list(raw_download_root.rglob(cached_file.name))
            except FileNotFoundError:
                raw_matches = []
            for raw_path in raw_matches:
                if raw_path.is_file():
                    raw_path.unlink()
                    removed_raw_files += 1
                    prune_empty_parents(raw_path.parent, raw_download_root)

    for cache_dir in sorted(set(cache_dirs), reverse=True):
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            prune_empty_parents(cache_dir.parent, cache_dir.parents[1])

    return removed_target_files, removed_raw_files


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


def write_run_command_log(directory):
    log_path = directory / "last_run_command.txt"
    command = shlex.join([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]])
    log_contents = "\n".join(
        [
            f"timestamp: {datetime.now().astimezone().isoformat()}",
            f"working_directory: {Path.cwd()}",
            f"command: {command}",
            "",
        ]
    )
    log_path.write_text(log_contents, encoding="utf-8")
    return log_path


def append_command_history(summary_path, tool_name, command, working_directory):
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    entry = (
        f"### {timestamp} - {tool_name}\n\n"
        f"- Working directory: `{working_directory}`\n"
        f"- Command: `{command}`\n\n"
    )

    if summary_path.exists():
        existing = summary_path.read_text(encoding="utf-8")
    else:
        existing = "# Analysis Summary\n\n"

    if "## Command History" not in existing:
        existing = existing.rstrip() + "\n\n## Command History\n\n"
    elif not existing.endswith("\n"):
        existing += "\n"

    summary_path.write_text(existing.rstrip() + "\n\n" + entry, encoding="utf-8")


def update_analysis_summary(output_dir):
    summary_path = output_dir / SUMMARY_FILENAME
    command = shlex.join([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]])
    append_command_history(summary_path, Path(__file__).name, command, Path.cwd())
    return summary_path


def build_pipeline_env(config_path):
    env = os.environ.copy()
    env["YOUNG_PIPELINE_CONFIG"] = str(config_path)
    env["PATH"] = f"{Path(sys.executable).resolve().parent}{os.pathsep}{env.get('PATH', '')}"
    env.pop("CRDS_CONTEXT", None)
    return env


def ensure_runtime_tools(env):
    missing = [
        tool for tool in ("python", "crds", "yq")
        if which(tool, path=env.get("PATH")) is None
    ]
    if missing:
        raise SystemExit(
            "Missing required runtime tools in the active environment "
            f"({sys.executable}): {', '.join(missing)}"
        )


def run_pipeline(config_path):
    env = build_pipeline_env(config_path)
    ensure_runtime_tools(env)

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
    ensure_directory(output_dir)

    download_root = ensure_directory(args.download_root.expanduser().resolve())
    target_cache_root = ensure_directory(download_root / TARGET_CACHE_DIRNAME)
    raw_download_root = ensure_directory(download_root / RAW_DOWNLOAD_DIRNAME)
    run_log_path = write_run_command_log(download_root)
    print(f"[log] Saved launch command: {run_log_path}")
    summary_path = update_analysis_summary(output_dir)
    print(f"[log] Updated analysis summary: {summary_path}")
    staging_root = Path(
        tempfile.mkdtemp(prefix="young_pipeline_targets_", dir=str(download_root))
    ).resolve()

    generated_config_path = None
    success = False
    cached_target_dirs = []
    cached_target_files = []

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
                ra=args.ra,
                dec=args.dec,
            )
            staged_dir = stage_target_files(target_slug, cached_files, staging_root)
            staged_targets.append((target, staged_dir, len(cached_files)))
            cached_target_dirs.append(target_cache_root / f"MAST_{target_slug}" / target_slug)
            cached_target_files.extend(cached_files)

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
            if args.delete_cached_files:
                removed_target_files, removed_raw_files = delete_cached_files(
                    cached_target_dirs,
                    cached_target_files,
                    raw_download_root,
                )
                print(
                    f"[cache] Deleted {removed_target_files} target-cache files and "
                    f"{removed_raw_files} raw-download files."
                )
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

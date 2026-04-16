#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import shlex
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
from astropy.io import fits
from PIL import Image


FILTER_WAVELENGTHS = {
    "F070W": 0.70,
    "F090W": 0.90,
    "F115W": 1.15,
    "F140M": 1.40,
    "F150W": 1.50,
    "F150W2": 1.50,
    "F162M": 1.62,
    "F182M": 1.82,
    "F200W": 2.00,
    "F210M": 2.10,
    "F250M": 2.50,
    "F277W": 2.77,
    "F300M": 3.00,
    "F335M": 3.35,
    "F356W": 3.56,
    "F410M": 4.10,
    "F444W": 4.44,
    "F460M": 4.60,
    "F480M": 4.80,
}


class ColorImageError(Exception):
    pass


SUMMARY_FILENAME = "ANALYSIS_SUMMARY.md"


def append_command_history(summary_path: Path, tool_name: str, command: str, working_directory: Path) -> None:
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


def resolve_cluster_root(input_dir: Path, output_path: Path) -> Path:
    if input_dir.name == "stage3_output" and input_dir.parent.name.startswith("Output_"):
        return input_dir.parent.parent
    return output_path.parent


def update_analysis_summary(input_dir: Path, output_path: Path) -> Path:
    cluster_root = resolve_cluster_root(input_dir, output_path)
    summary_path = cluster_root / SUMMARY_FILENAME
    command = shlex.join([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]])
    append_command_history(summary_path, Path(__file__).name, command, Path.cwd())
    return summary_path


PRESETS = {
    "original": {
        "lower_percentile": 1.0,
        "upper_percentile": 99.7,
        "clip_max_percentile": 100.0,
        "stretch_mode": "arcsinh",
        "stretch": 6.0,
        "power": 0.5,
        "gamma": 1.0,
        "brightness": 1.0,
        "highlight_rolloff": 0.85,
        "highlight_strength": 0.6,
    },
    "core": {
        "lower_percentile": 1.0,
        "upper_percentile": 99.995,
        "clip_max_percentile": 100.0,
        "stretch_mode": "power",
        "stretch": 6.0,
        "power": 0.4,
        "gamma": 1.0,
        "brightness": 1.2,
        "highlight_rolloff": 1.0,
        "highlight_strength": 0.0,
    },
    "core_color": {
        "lower_percentile": 1.0,
        "upper_percentile": 99.995,
        "clip_max_percentile": 100.0,
        "stretch_mode": "power",
        "stretch": 6.0,
        "power": 0.4,
        "gamma": 1.0,
        "brightness": 1.15,
        "highlight_rolloff": 1.0,
        "highlight_strength": 0.0,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a JWST color image from Stage 3 mosaics."
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--no-flip-y",
        action="store_true",
        help="Keep the native FITS row order instead of flipping the PNG vertically.",
    )
    parser.add_argument(
        "--preset",
        type=str,
        choices=tuple(PRESETS.keys()),
        default="original",
        help="Named display preset.",
    )
    parser.add_argument("--red-filter", type=str, default=None)
    parser.add_argument("--green-filter", type=str, default=None)
    parser.add_argument("--blue-filter", type=str, default=None)
    parser.add_argument("--lower-percentile", type=float, default=1.0)
    parser.add_argument("--upper-percentile", type=float, default=99.7)
    parser.add_argument(
        "--clip-max-percentile",
        type=float,
        default=100.0,
        help="Cap each channel at this percentile before normalization. Use <100 to suppress extreme cores.",
    )
    parser.add_argument("--stretch", type=float, default=6.0)
    parser.add_argument(
        "--stretch-mode",
        type=str,
        choices=("arcsinh", "power", "log"),
        default="arcsinh",
        help="Display stretch to apply after percentile normalization.",
    )
    parser.add_argument(
        "--power",
        type=float,
        default=0.5,
        help="Exponent used when --stretch-mode power is selected.",
    )
    parser.add_argument("--gamma", type=float, default=1.0)
    parser.add_argument(
        "--brightness",
        type=float,
        default=1.0,
        help="Post-stretch brightness multiplier. Values >1 brighten the final image.",
    )
    parser.add_argument(
        "--highlight-rolloff",
        type=float,
        default=0.85,
        help="Start compressing highlights above this normalized level. Set >=1 to disable.",
    )
    parser.add_argument(
        "--highlight-strength",
        type=float,
        default=0.6,
        help="Strength of highlight compression. Higher values protect bright cores more.",
    )
    parser.add_argument(
        "--weights",
        type=float,
        nargs=3,
        default=(1.0, 1.0, 1.0),
        metavar=("R", "G", "B"),
    )
    return parser.parse_args()


def apply_preset_defaults(args: argparse.Namespace) -> argparse.Namespace:
    preset = PRESETS[args.preset]
    for key, value in preset.items():
        if getattr(args, key) == parse_args_default(key):
            setattr(args, key, value)
    return args


def parse_args_default(name: str):
    defaults = {
        "lower_percentile": 1.0,
        "upper_percentile": 99.7,
        "clip_max_percentile": 100.0,
        "stretch": 6.0,
        "stretch_mode": "arcsinh",
        "power": 0.5,
        "gamma": 1.0,
        "brightness": 1.0,
        "highlight_rolloff": 0.85,
        "highlight_strength": 0.6,
    }
    return defaults[name]


def extract_filter_name(path: Path) -> str:
    match = re.search(r"_([A-Z0-9]+)_(?:sci|i2d)\.fits(?:\.fz)?$", path.name)
    if not match:
        raise ColorImageError(f"Could not determine filter name from {path}")
    return match.group(1)


def discover_input_files(input_dir: Path) -> dict[str, Path]:
    files = {}
    patterns = (
        "*/output_files/*_sci.fits",
        "*/output_files/*_sci.fits.fz",
        "*/output_files/*_i2d.fits",
        "*/output_files/*_i2d.fits.fz",
    )
    for pattern in patterns:
        for path in sorted(input_dir.glob(pattern)):
            files[extract_filter_name(path)] = path
        if files:
            return files

    raise ColorImageError(f"No *_sci.fits, *_sci.fits.fz, *_i2d.fits, or *_i2d.fits.fz files found under {input_dir}")


def get_filter_wavelength(filter_name: str) -> float:
    if filter_name in FILTER_WAVELENGTHS:
        return FILTER_WAVELENGTHS[filter_name]

    match = re.fullmatch(r"F(\d{3,4})([A-Z0-9]*)", filter_name)
    if match:
        return float(match.group(1)) / 100.0

    raise ColorImageError(f"Unknown filter wavelength for {filter_name}")


def sort_filters(filters: list[str]) -> list[str]:
    return sorted(filters, key=get_filter_wavelength)


def choose_channels(
    available_filters: list[str],
    red_filter: str | None,
    green_filter: str | None,
    blue_filter: str | None,
) -> tuple[str, str, str]:
    ordered = sort_filters(available_filters)
    blue = blue_filter or ordered[0]
    red = red_filter or ordered[-1]
    green = green_filter or ordered[len(ordered) // 2]
    for name in (red, green, blue):
        if name not in available_filters:
            raise ColorImageError(
                f"Requested filter {name} is not available. Found: {', '.join(ordered)}"
            )
    return red, green, blue


def get_rgb_channel_weights(ordered_filters: list[str]) -> dict[str, np.ndarray]:
    """Map all available filters smoothly into blue, green, and red channels."""
    if len(ordered_filters) == 1:
        return {ordered_filters[0]: np.array([1.0, 1.0, 1.0], dtype=np.float32)}

    wavelengths = np.asarray([get_filter_wavelength(name) for name in ordered_filters], dtype=np.float32)
    wave_min = float(np.min(wavelengths))
    wave_max = float(np.max(wavelengths))
    positions = (wavelengths - wave_min) / (wave_max - wave_min)

    centers = {
        "blue": 0.0,
        "green": 0.5,
        "red": 1.0,
    }
    weights = {}
    for filter_name, pos in zip(ordered_filters, positions):
        blue_weight = max(0.0, 1.0 - abs(float(pos) - centers["blue"]) / 0.5)
        green_weight = max(0.0, 1.0 - abs(float(pos) - centers["green"]) / 0.5)
        red_weight = max(0.0, 1.0 - abs(float(pos) - centers["red"]) / 0.5)
        vector = np.array([red_weight, green_weight, blue_weight], dtype=np.float32)
        if np.sum(vector) == 0:
            nearest = int(np.argmin(np.abs(np.array([1.0, 0.5, 0.0], dtype=np.float32) - pos)))
            vector[nearest] = 1.0
        weights[filter_name] = vector
    return weights


def read_image_data(path: Path) -> np.ndarray:
    with fits.open(path, memmap=True) as hdul:
        for hdu in hdul:
            data = getattr(hdu, "data", None)
            if data is None:
                continue
            array = np.asarray(data, dtype=np.float32)
            array = np.squeeze(array)
            if array.ndim == 2:
                return np.nan_to_num(array, nan=0.0, posinf=0.0, neginf=0.0)
    raise ColorImageError(f"No 2D image plane found in {path}")


def normalize_channel(
    data: np.ndarray,
    lower_percentile: float,
    upper_percentile: float,
    clip_max_percentile: float,
) -> np.ndarray:
    finite = np.isfinite(data)
    if not np.any(finite):
        return np.zeros_like(data, dtype=np.float32)
    sample = data[finite]
    if clip_max_percentile < 100.0:
        if not 0.0 < clip_max_percentile <= 100.0:
            raise ColorImageError("--clip-max-percentile must be between 0 and 100")
        clip_value = np.percentile(sample, clip_max_percentile)
        data = np.minimum(data, clip_value)
        sample = np.minimum(sample, clip_value)
    low = np.percentile(sample, lower_percentile)
    high = np.percentile(sample, upper_percentile)
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        return np.zeros_like(data, dtype=np.float32)
    scaled = (data - low) / (high - low)
    return np.clip(scaled, 0.0, 1.0).astype(np.float32)


def apply_arcsinh(rgb: np.ndarray, stretch: float) -> np.ndarray:
    if stretch <= 0:
        raise ColorImageError("--stretch must be positive")
    return np.arcsinh(stretch * rgb) / np.arcsinh(stretch)


def apply_power(rgb: np.ndarray, power: float) -> np.ndarray:
    if power <= 0:
        raise ColorImageError("--power must be positive")
    return np.power(np.clip(rgb, 0.0, 1.0), power, dtype=np.float32)


def apply_log(rgb: np.ndarray, stretch: float) -> np.ndarray:
    if stretch <= 0:
        raise ColorImageError("--stretch must be positive")
    return np.log1p(stretch * rgb) / np.log1p(stretch)


def apply_stretch(rgb: np.ndarray, stretch_mode: str, stretch: float, power: float) -> np.ndarray:
    if stretch_mode == "arcsinh":
        return apply_arcsinh(rgb, stretch)
    if stretch_mode == "power":
        return apply_power(rgb, power)
    if stretch_mode == "log":
        return apply_log(rgb, stretch)
    raise ColorImageError(f"Unsupported stretch mode: {stretch_mode}")


def apply_gamma(rgb: np.ndarray, gamma: float) -> np.ndarray:
    if gamma <= 0:
        raise ColorImageError("--gamma must be positive")
    if gamma == 1.0:
        return rgb
    return np.power(np.clip(rgb, 0.0, 1.0), 1.0 / gamma, dtype=np.float32)


def apply_brightness(rgb: np.ndarray, brightness: float) -> np.ndarray:
    if brightness <= 0:
        raise ColorImageError("--brightness must be positive")
    if brightness == 1.0:
        return rgb
    return np.clip(rgb * brightness, 0.0, 1.0)


def apply_highlight_rolloff(
    rgb: np.ndarray,
    rolloff: float,
    strength: float,
) -> np.ndarray:
    if rolloff >= 1.0:
        return rgb
    if not 0.0 < rolloff < 1.0:
        raise ColorImageError("--highlight-rolloff must be between 0 and 1, or >=1 to disable")
    if strength < 0:
        raise ColorImageError("--highlight-strength must be non-negative")
    if strength == 0:
        return rgb

    clipped = np.clip(rgb, 0.0, 1.0)
    low = np.minimum(clipped, rolloff)
    high = np.clip(clipped - rolloff, 0.0, None)
    compressed_high = np.log1p(strength * high / (1.0 - rolloff)) / np.log1p(strength)
    return np.clip(low + (1.0 - rolloff) * compressed_high, 0.0, 1.0)


def build_rgb(
    red: np.ndarray,
    green: np.ndarray,
    blue: np.ndarray,
    lower_percentile: float,
    upper_percentile: float,
    clip_max_percentile: float,
    stretch_mode: str,
    stretch: float,
    power: float,
    gamma: float,
    brightness: float,
    highlight_rolloff: float,
    highlight_strength: float,
    weights: tuple[float, float, float],
) -> np.ndarray:
    rgb = np.stack(
        [
            normalize_channel(red, lower_percentile, upper_percentile, clip_max_percentile),
            normalize_channel(green, lower_percentile, upper_percentile, clip_max_percentile),
            normalize_channel(blue, lower_percentile, upper_percentile, clip_max_percentile),
        ],
        axis=-1,
    )
    rgb *= np.asarray(weights, dtype=np.float32).reshape(1, 1, 3)
    rgb = np.clip(rgb, 0.0, 1.0)
    rgb = apply_highlight_rolloff(rgb, highlight_rolloff, highlight_strength)
    rgb = apply_stretch(rgb, stretch_mode, stretch, power)
    rgb = apply_gamma(rgb, gamma)
    rgb = apply_brightness(rgb, brightness)
    return np.clip(rgb, 0.0, 1.0)


def build_rgb_from_filters(
    filter_images: dict[str, np.ndarray],
    lower_percentile: float,
    upper_percentile: float,
    clip_max_percentile: float,
    stretch_mode: str,
    stretch: float,
    power: float,
    gamma: float,
    brightness: float,
    highlight_rolloff: float,
    highlight_strength: float,
    weights: tuple[float, float, float],
) -> np.ndarray:
    ordered_filters = sort_filters(list(filter_images.keys()))
    channel_weights = get_rgb_channel_weights(ordered_filters)

    normalized = {
        name: normalize_channel(
            filter_images[name],
            lower_percentile,
            upper_percentile,
            clip_max_percentile,
        )
        for name in ordered_filters
    }

    shape = next(iter(normalized.values())).shape
    rgb = np.zeros((*shape, 3), dtype=np.float32)
    channel_sums = np.zeros(3, dtype=np.float32)

    for name in ordered_filters:
        vector = channel_weights[name]
        rgb += normalized[name][..., None] * vector.reshape(1, 1, 3)
        channel_sums += vector

    channel_sums = np.where(channel_sums == 0, 1.0, channel_sums)
    rgb /= channel_sums.reshape(1, 1, 3)
    rgb *= np.asarray(weights, dtype=np.float32).reshape(1, 1, 3)
    rgb = np.clip(rgb, 0.0, 1.0)
    rgb = apply_highlight_rolloff(rgb, highlight_rolloff, highlight_strength)
    rgb = apply_stretch(rgb, stretch_mode, stretch, power)
    rgb = apply_gamma(rgb, gamma)
    rgb = apply_brightness(rgb, brightness)
    return np.clip(rgb, 0.0, 1.0)


def save_rgb(rgb: np.ndarray, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rgb = np.flipud(rgb)
    image = Image.fromarray(np.round(rgb * 255).astype(np.uint8), mode="RGB")
    image.save(output_path)


def blend_luminance(core_rgb: np.ndarray, color_rgb: np.ndarray) -> np.ndarray:
    eps = 1e-6
    core_luma = np.mean(np.clip(core_rgb, 0.0, 1.0), axis=-1, keepdims=True)
    color_luma = np.mean(np.clip(color_rgb, 0.0, 1.0), axis=-1, keepdims=True)
    blended = color_rgb * (core_luma / np.maximum(color_luma, eps))
    return np.clip(blended, 0.0, 1.0)


def main() -> None:
    args = apply_preset_defaults(parse_args())
    input_files = discover_input_files(args.input_dir.expanduser().resolve())

    if args.red_filter or args.green_filter or args.blue_filter:
        red_name, green_name, blue_name = choose_channels(
            sorted(input_files.keys()),
            args.red_filter,
            args.green_filter,
            args.blue_filter,
        )

        red = read_image_data(input_files[red_name])
        green = read_image_data(input_files[green_name])
        blue = read_image_data(input_files[blue_name])

        if red.shape != green.shape or red.shape != blue.shape:
            raise ColorImageError(
                f"Channel shapes do not match: red={red.shape}, green={green.shape}, blue={blue.shape}"
            )

        if args.preset == "core_color":
            rgb_core = build_rgb(
                red=red,
                green=green,
                blue=blue,
                lower_percentile=PRESETS["core"]["lower_percentile"],
                upper_percentile=PRESETS["core"]["upper_percentile"],
                clip_max_percentile=PRESETS["core"]["clip_max_percentile"],
                stretch_mode=PRESETS["core"]["stretch_mode"],
                stretch=PRESETS["core"]["stretch"],
                power=PRESETS["core"]["power"],
                gamma=PRESETS["core"]["gamma"],
                brightness=PRESETS["core"]["brightness"],
                highlight_rolloff=PRESETS["core"]["highlight_rolloff"],
                highlight_strength=PRESETS["core"]["highlight_strength"],
                weights=tuple(args.weights),
            )
            rgb_original = build_rgb(
                red=red,
                green=green,
                blue=blue,
                lower_percentile=PRESETS["original"]["lower_percentile"],
                upper_percentile=PRESETS["original"]["upper_percentile"],
                clip_max_percentile=PRESETS["original"]["clip_max_percentile"],
                stretch_mode=PRESETS["original"]["stretch_mode"],
                stretch=PRESETS["original"]["stretch"],
                power=PRESETS["original"]["power"],
                gamma=PRESETS["original"]["gamma"],
                brightness=PRESETS["original"]["brightness"],
                highlight_rolloff=PRESETS["original"]["highlight_rolloff"],
                highlight_strength=PRESETS["original"]["highlight_strength"],
                weights=tuple(args.weights),
            )
            rgb = blend_luminance(rgb_core, rgb_original)
        else:
            rgb = build_rgb(
                red=red,
                green=green,
                blue=blue,
                lower_percentile=args.lower_percentile,
                upper_percentile=args.upper_percentile,
                clip_max_percentile=args.clip_max_percentile,
                stretch_mode=args.stretch_mode,
                stretch=args.stretch,
                power=args.power,
                gamma=args.gamma,
                brightness=args.brightness,
                highlight_rolloff=args.highlight_rolloff,
                highlight_strength=args.highlight_strength,
                weights=tuple(args.weights),
            )
        channel_summary = f"R={red_name}, G={green_name}, B={blue_name}"
    else:
        filter_images = {name: read_image_data(path) for name, path in input_files.items()}
        shapes = {data.shape for data in filter_images.values()}
        if len(shapes) != 1:
            raise ColorImageError(f"Filter shapes do not match: {sorted(shapes)}")

        if args.preset == "core_color":
            rgb_core = build_rgb_from_filters(
                filter_images=filter_images,
                lower_percentile=PRESETS["core"]["lower_percentile"],
                upper_percentile=PRESETS["core"]["upper_percentile"],
                clip_max_percentile=PRESETS["core"]["clip_max_percentile"],
                stretch_mode=PRESETS["core"]["stretch_mode"],
                stretch=PRESETS["core"]["stretch"],
                power=PRESETS["core"]["power"],
                gamma=PRESETS["core"]["gamma"],
                brightness=PRESETS["core"]["brightness"],
                highlight_rolloff=PRESETS["core"]["highlight_rolloff"],
                highlight_strength=PRESETS["core"]["highlight_strength"],
                weights=tuple(args.weights),
            )
            rgb_original = build_rgb_from_filters(
                filter_images=filter_images,
                lower_percentile=PRESETS["original"]["lower_percentile"],
                upper_percentile=PRESETS["original"]["upper_percentile"],
                clip_max_percentile=PRESETS["original"]["clip_max_percentile"],
                stretch_mode=PRESETS["original"]["stretch_mode"],
                stretch=PRESETS["original"]["stretch"],
                power=PRESETS["original"]["power"],
                gamma=PRESETS["original"]["gamma"],
                brightness=PRESETS["original"]["brightness"],
                highlight_rolloff=PRESETS["original"]["highlight_rolloff"],
                highlight_strength=PRESETS["original"]["highlight_strength"],
                weights=tuple(args.weights),
            )
            rgb = blend_luminance(rgb_core, rgb_original)
        else:
            rgb = build_rgb_from_filters(
                filter_images=filter_images,
                lower_percentile=args.lower_percentile,
                upper_percentile=args.upper_percentile,
                clip_max_percentile=args.clip_max_percentile,
                stretch_mode=args.stretch_mode,
                stretch=args.stretch,
                power=args.power,
                gamma=args.gamma,
                brightness=args.brightness,
                highlight_rolloff=args.highlight_rolloff,
                highlight_strength=args.highlight_strength,
                weights=tuple(args.weights),
            )
        channel_summary = "Composite blend from all filters: " + ", ".join(sort_filters(list(input_files.keys())))

    output_path = args.output.expanduser().resolve()
    summary_path = update_analysis_summary(args.input_dir.expanduser().resolve(), output_path)
    if args.no_flip_y:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        image = Image.fromarray(np.round(rgb * 255).astype(np.uint8), mode="RGB")
        image.save(output_path)
    else:
        save_rgb(rgb, output_path)

    print(f"Saved JWST color image to {args.output}")
    print(f"[log] Updated analysis summary: {summary_path}")
    print(f"Channels: {channel_summary}")
    print(f"Stretch: {args.stretch_mode}")


if __name__ == "__main__":
    main()

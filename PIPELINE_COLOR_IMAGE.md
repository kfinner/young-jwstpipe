# JWST Color Image Pipeline

This document describes the standalone JWST color-image builder in `/Users/kfinner/Documents/GitHub/young-jwstpipe-wrapped/make_jwst_color_image.py`.

## Purpose

This helper builds a PNG color image from Stage 3 JWST mosaics.

It is intended to work directly from the reduction output tree, especially:
- `stage3_output/<FILTER>/output_files/*_sci.fits`

## Inputs

The script prefers:
- `*_sci.fits`

If no science mosaics are found, it can fall back to:
- `*_i2d.fits`

## Default channel behavior

By default the script uses all available filters and blends them into RGB according to wavelength ordering.

That means if you have:
- `F150W`
- `F210M`
- `F300M`
- `F444W`

then all four contribute to the final RGB image.

If you want a manual mapping, you can still specify:
- `--red-filter`
- `--green-filter`
- `--blue-filter`

## Presets

The script currently has two named presets:
- `original`
- `core`

### `original`
A more aggressive brightening / display mode closer to the first-look images.

### `core`
A higher white-point mode designed to preserve bright BCG cores better.

## Stretch options

Supported stretch modes:
- `arcsinh`
- `power`
- `log`

Important controls:
- `--lower-percentile`
- `--upper-percentile`
- `--clip-max-percentile`
- `--highlight-rolloff`
- `--highlight-strength`
- `--gamma`
- `--weights`

## Orientation

The output PNG is flipped vertically by default so north is up.

To keep native FITS row order instead, use:
- `--no-flip-y`

## Logging and provenance

The script now appends its exact invocation to the cluster's `ANALYSIS_SUMMARY.md` automatically.

The cluster root is inferred from:
- the `stage3_output` directory structure when possible
- otherwise the output image directory

## Typical commands

### Core-preserving default

```bash
python /Users/kfinner/Documents/GitHub/young-jwstpipe-wrapped/make_jwst_color_image.py   --preset core   --input-dir /Users/kfinner/Work/JWST/VENUS/PLCKG165+67.0/Output_PLCKG165_67_0/stage3_output   --output /Users/kfinner/Work/JWST/VENUS/PLCKG165+67.0/PLCKG165+67.0_color_core.png
```

### Brighter low end with preserved core

```bash
python /Users/kfinner/Documents/GitHub/young-jwstpipe-wrapped/make_jwst_color_image.py   --preset core   --lower-percentile 0.2   --input-dir /Users/kfinner/Work/JWST/VENUS/PLCKG165+67.0/Output_PLCKG165_67_0/stage3_output   --output /Users/kfinner/Work/JWST/VENUS/PLCKG165+67.0/PLCKG165+67.0_color_core_bright.png
```

## Known notes

- The `core` preset is currently the most useful default for clusters with very bright central galaxies.
- Display tuning is still empirical; this tool is for visualization, not calibrated photometric color rendering.

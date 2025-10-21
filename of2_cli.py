# -*- coding: utf-8 -*-
"""
Standalone CLI for running the OF2 workflow outside ArcGIS Pro.

Improvements vs ModelBuilder export:
- Parameterized inputs (workspace, feature classes, expression, output)
- Correctly applies spatial selection to Storm layers
- Avoids unnecessary intermediate disk writes (merges selected layers directly)
- Uses FieldMappings programmatically for clarity and resilience

Requires: ArcGIS Pro Python (Windows) or ArcGIS Server Python (Linux).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import arcpy


def resolve_dataset(path_or_name: str, workspace_gdb: str) -> str:
    """Return absolute path to a dataset, joining to workspace when needed."""
    # If caller passed a full path to a dataset or a URL, use it as-is
    if os.path.isabs(path_or_name) or path_or_name.lower().startswith(("http://", "https://")):
        return path_or_name
    return os.path.join(workspace_gdb, path_or_name)


def ensure_layer(dataset_path: str, layer_name: str, where: Optional[str] = None) -> str:
    """Create a feature layer and return its name."""
    if arcpy.Exists(layer_name):
        arcpy.management.Delete(layer_name)
    arcpy.management.MakeFeatureLayer(in_features=dataset_path, out_layer=layer_name, where_clause=where or "")
    return layer_name


def build_field_mappings(inputs: list[str]) -> arcpy.FieldMappings:
    """Build field mappings to carry only OrigPlan and DocLink with friendly aliases."""
    field_mappings = arcpy.FieldMappings()
    desired = [("OrigPlan", "Original Plan"), ("DocLink", "Document Link")]

    for field_name, alias in desired:
        fmap = arcpy.FieldMap()
        added_any = False
        for src in inputs:
            try:
                fmap.addInputField(src, field_name)
                added_any = True
            except Exception:
                # Field not present in this input; skip
                continue
        if not added_any:
            raise RuntimeError(f"Field '{field_name}' not found in any input: {inputs}")
        out_field = fmap.outputField
        out_field.name = field_name
        out_field.aliasName = alias
        fmap.outputField = out_field
        fmap.mergeRule = "First"
        field_mappings.addFieldMap(fmap)

    return field_mappings


def run_of2(
    expression: str,
    workspace_gdb: str,
    storm_catchment: str,
    storm_cleanout: str,
    parcels: str,
    output_fc: str,
    scratch_gdb: Optional[str] = None,
) -> str:
    """Run the OF2 workflow and return the path to the output feature class."""
    workspace_gdb = os.path.abspath(workspace_gdb)
    scratch_gdb = os.path.abspath(scratch_gdb) if scratch_gdb else workspace_gdb

    with arcpy.EnvManager(
        workspace=workspace_gdb,
        scratchWorkspace=scratch_gdb,
        overwriteOutput=True,
        parallelProcessingFactor="100%",
    ):
        # Resolve dataset paths (support relative names within the GDB)
        parcels_path = resolve_dataset(parcels, workspace_gdb)
        sc_path = resolve_dataset(storm_catchment, workspace_gdb)
        so_path = resolve_dataset(storm_cleanout, workspace_gdb)

        if not arcpy.Exists(parcels_path):
            raise FileNotFoundError(f"Parcels dataset not found: {parcels_path}")
        if not arcpy.Exists(sc_path):
            raise FileNotFoundError(f"Storm Catchment dataset not found: {sc_path}")
        if not arcpy.Exists(so_path):
            raise FileNotFoundError(f"Storm Cleanout dataset not found: {so_path}")

        # Make layers (apply attribute expression to Parcels only)
        parcels_lyr = ensure_layer(parcels_path, "parcels_layer", expression)
        sc_lyr = ensure_layer(sc_path, "storm_catchment_layer")
        so_lyr = ensure_layer(so_path, "storm_cleanout_layer")

        # Spatially select storm layers by parcels
        arcpy.management.SelectLayerByLocation(
            in_layer=[sc_lyr, so_lyr],
            overlap_type="INTERSECT",
            select_features=parcels_lyr,
            selection_type="NEW_SELECTION",
        )

        # Diagnostics
        sc_count = int(arcpy.management.GetCount(sc_lyr)[0])
        so_count = int(arcpy.management.GetCount(so_lyr)[0])
        logging.info("Selected %d catchment and %d cleanout features", sc_count, so_count)

        # Merge selected layers directly to the output using field mappings
        inputs = [sc_lyr, so_lyr]
        field_mappings = build_field_mappings(inputs)

        # Prepare output path (allow either a name within the GDB or a full path)
        if os.path.dirname(output_fc):
            out_path = output_fc
        else:
            out_path = os.path.join(workspace_gdb, output_fc)

        if arcpy.Exists(out_path):
            arcpy.management.Delete(out_path)

        merge_result = arcpy.management.Merge(inputs=inputs, output=out_path, field_mappings=field_mappings)
        out_fc_path = str(merge_result[0])

        # Remove duplicates by OrigPlan
        if "OrigPlan" in [f.name for f in arcpy.ListFields(out_fc_path)]:
            arcpy.management.DeleteIdentical(in_dataset=out_fc_path, fields=["OrigPlan"]) 
        else:
            logging.warning("Field 'OrigPlan' not present in output; skipping DeleteIdentical")

        return out_fc_path


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the OF2 workflow using ArcPy")
    parser.add_argument(
        "--workspace",
        default=r"C:\\Users\\asauls\\City of Kent\\PW GIS - Private - Documents\\Analyst Folders\\Andy\\General GIS\\Default.gdb",
        help="Path to the primary file geodatabase (GDB)",
    )
    parser.add_argument(
        "--scratch",
        default=None,
        help="Optional scratch GDB path (defaults to --workspace)",
    )
    parser.add_argument(
        "--storm-catchment",
        dest="storm_catchment",
        default=r"Storm (Public and Private)\Storm Catchment",
        help="Path or name of Storm Catchment feature class (relative names resolved in --workspace)",
    )
    parser.add_argument(
        "--storm-cleanout",
        dest="storm_cleanout",
        default=r"Storm (Public and Private)\Storm Cleanout",
        help="Path or name of Storm Cleanout feature class (relative names resolved in --workspace)",
    )
    parser.add_argument(
        "--parcels",
        default=r"Parcels",
        help="Path or name of Parcels feature class (relative names resolved in --workspace)",
    )
    parser.add_argument(
        "--expression",
        default="PARCEL_NUMBER = '6195400110'",
        help="SQL where clause to filter Parcels",
    )
    parser.add_argument(
        "--output",
        default=r"OF_property_edit",
        help="Output feature class name or full path (defaults inside --workspace)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        out_fc = run_of2(
            expression=args.expression,
            workspace_gdb=args.workspace,
            storm_catchment=args.storm_catchment,
            storm_cleanout=args.storm_cleanout,
            parcels=args.parcels,
            output_fc=args.output,
            scratch_gdb=args.scratch,
        )
        logging.info("Output written: %s", out_fc)
        print(out_fc)
        return 0
    except Exception as exc:
        logging.exception("OF2 failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

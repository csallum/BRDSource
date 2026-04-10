#!/usr/bin/env python3
"""
================================================================================
Module      : instruments_extractor.py
Author      : Craig Allum
Date        : 24/02/2026
Version     : v1.1
Description :
    B3 Instruments Consolidated Column Extractor.

    Reads a downloaded InstrumentsConsolidated CSV file from the B3 exchange,
    applies configurable row filters, extracts a defined set of columns, and
    writes a clean semicolon-delimited output file ready for ingestion into
    KSDS.

    The module can be invoked directly from the command line for ad-hoc
    processing, or imported by BrazilBDRList.py / BrazilBDRListOnce.py to
    run automatically after each scheduled download.

    B3 files use a Latin-1 encoding and may include a metadata header line
    (e.g. "Status do Arquivo: Final") before the column headers.  Both of
    these edge cases are handled automatically.

Usage (CLI):
    python instruments_extractor.py                        # process latest file
    python instruments_extractor.py process  <filepath>   # process specific file
    python instruments_extractor.py list     <filepath>   # list all columns
    python instruments_extractor.py diagnose <filepath>   # diagnose CSV structure

Dependencies:
    pandas
================================================================================
"""

import glob
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Directory configuration
# ---------------------------------------------------------------------------

INPUT_DIR:  Path = Path("downloads")
OUTPUT_DIR: Path = Path("processed")
LOG_DIR:    Path = Path("logs")

OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# Handlers are created explicitly so that UTF-8 encoding can be enforced on
# both the file and console streams (required for Windows compatibility).
# ---------------------------------------------------------------------------

_log_file = LOG_DIR / f"extractor_{datetime.now().strftime('%Y%m')}.log"

_file_handler = logging.FileHandler(_log_file, encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

# Reconfigure stdout to UTF-8 on Windows to prevent codec errors when
# printing characters outside the default cp1252 range.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass  # Python < 3.7 does not support reconfigure

logging.basicConfig(
    level=logging.INFO,
    handlers=[_file_handler, _console_handler],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column configuration
# Update COLUMNS_TO_EXTRACT with the exact B3 column names required.
# Set EXTRACT_ALL_COLUMNS = True to bypass the list and retain every column.
# ---------------------------------------------------------------------------

COLUMNS_TO_EXTRACT: list[str] = [
    "TckrSymb",       # Ticker Symbol
    "ISIN",           # ISIN Code
    "Asst",           # Asset
    "AsstDesc",       # Asset Description
    "SgmtNm",         # Segment Name
    "MktNm",          # Market Name
    "SctyCtgyNm",     # Security Category Name
    "SpcfctnCd",      # Specification Code
    "XprtnDt",        # Expiration Date
    "XprtnCd",        # Expiration Code
    "TradgStartDt",   # Trading Start Date
    "TradgEndDt",     # Trading End Date
    "BaseCd",         # Base Code
    "ConvsBsBldr",    # Conversion Base Builder
    "CrpnNm",         # Corporation Name
    "CorpGovnLvlNm",  # Corporate Governance Level Name
]

EXTRACT_ALL_COLUMNS: bool = False

# ---------------------------------------------------------------------------
# Output configuration
# ---------------------------------------------------------------------------

# Semicolon delimiter is standard for European / Brazilian data systems
OUTPUT_DELIMITER: str = ";"

# ---------------------------------------------------------------------------
# Filter configuration
# Set ENABLE_FILTERING = False to disable all filters and retain every row.
# ---------------------------------------------------------------------------

ENABLE_FILTERING: bool = True

# Filter 1: Retain only rows where SctyCtgyNm contains 'BDR'
FILTER_SECURITY_CATEGORY: bool = True
SECURITY_CATEGORY_VALUE:  str  = "BDR"

# Filter 2: Retain only rows where SpcfctnCd begins with 'DR2' or 'DR3'
FILTER_SPECIFICATION_CODE:    bool      = True
SPECIFICATION_CODE_PREFIXES:  list[str] = ["DR2", "DR3"]

# Encodings to attempt when reading B3 CSV files (tried in order)
_ENCODINGS: list[str] = ["utf-8", "latin-1", "iso-8859-1", "cp1252"]


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def find_latest_file(pattern: str = "InstrumentsConsolidated_*.csv") -> Path | None:
    """Locate the most recently modified instruments file in the input directory.

    Args:
        pattern: Glob pattern used to match candidate files within
            :data:`INPUT_DIR`.  Defaults to ``InstrumentsConsolidated_*.csv``.

    Returns:
        A :class:`~pathlib.Path` pointing to the most recently modified
        matching file, or ``None`` if no matching files are found.
    """
    try:
        files = glob.glob(str(INPUT_DIR / pattern))

        if not files:
            logger.warning("No files found matching pattern: %s", pattern)
            return None

        latest = max(files, key=os.path.getmtime)
        logger.info("Found latest file: %s", latest)
        return Path(latest)

    except Exception as exc:
        logger.error("Error locating latest file: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_columns(df: pd.DataFrame, required_columns: list[str]) -> list[str]:
    """Identify any required columns that are absent from the dataframe.

    Args:
        df:               The dataframe to inspect.
        required_columns: Column names that must be present.

    Returns:
        A list of column names that are missing from ``df``.  An empty list
        indicates all required columns are present.
    """
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        logger.warning("Missing columns: %s", missing)
        logger.info("Available columns: %s", list(df.columns))

    return missing


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the configured row filters to the dataframe.

    Filters are applied sequentially:

    1. **Security category filter** – retains only rows where
       ``SctyCtgyNm`` contains :data:`SECURITY_CATEGORY_VALUE` (default
       ``"BDR"``).
    2. **Specification code filter** – retains only rows where
       ``SpcfctnCd`` starts with one of the prefixes in
       :data:`SPECIFICATION_CODE_PREFIXES` (default ``["DR2", "DR3"]``).

    If :data:`ENABLE_FILTERING` is ``False`` the dataframe is returned
    unchanged.  Individual filters can be toggled via
    :data:`FILTER_SECURITY_CATEGORY` and :data:`FILTER_SPECIFICATION_CODE`.

    Args:
        df: Input dataframe (full instruments file after reading).

    Returns:
        A filtered copy of ``df``.
    """
    if not ENABLE_FILTERING:
        logger.info("Filtering is DISABLED - returning all rows")
        return df

    original_count = len(df)
    filtered_df    = df.copy()

    logger.info("Applying filters...")

    # -- Filter 1: Security Category --
    if FILTER_SECURITY_CATEGORY:
        if "SctyCtgyNm" not in filtered_df.columns:
            logger.warning(
                "Column 'SctyCtgyNm' not found - skipping security category filter"
            )
        else:
            filtered_df["SctyCtgyNm"] = (
                filtered_df["SctyCtgyNm"].fillna("").astype(str)
            )
            before = len(filtered_df)
            filtered_df = filtered_df[
                filtered_df["SctyCtgyNm"].str.contains(
                    SECURITY_CATEGORY_VALUE, case=False, na=False
                )
            ]
            logger.info(
                "Filter 1 (SctyCtgyNm contains '%s'): %s => %s rows (%s removed)",
                SECURITY_CATEGORY_VALUE,
                f"{before:,}",
                f"{len(filtered_df):,}",
                f"{before - len(filtered_df):,}",
            )

    # -- Filter 2: Specification Code --
    if FILTER_SPECIFICATION_CODE:
        if "SpcfctnCd" not in filtered_df.columns:
            logger.warning(
                "Column 'SpcfctnCd' not found - skipping specification code filter"
            )
        else:
            filtered_df["SpcfctnCd"] = (
                filtered_df["SpcfctnCd"].fillna("").astype(str)
            )
            before    = len(filtered_df)
            condition = filtered_df["SpcfctnCd"].str.startswith(
                tuple(SPECIFICATION_CODE_PREFIXES)
            )
            filtered_df  = filtered_df[condition]
            prefix_label = "' or '".join(SPECIFICATION_CODE_PREFIXES)
            logger.info(
                "Filter 2 (SpcfctnCd starts with '%s'): %s => %s rows (%s removed)",
                prefix_label,
                f"{before:,}",
                f"{len(filtered_df):,}",
                f"{before - len(filtered_df):,}",
            )

    total_removed = original_count - len(filtered_df)
    retention     = (len(filtered_df) / original_count * 100) if original_count else 0.0

    logger.info("=" * 80)
    logger.info("FILTERING SUMMARY:")
    logger.info("  Original rows:  %s", f"{original_count:,}")
    logger.info("  Filtered rows:  %s", f"{len(filtered_df):,}")
    logger.info("  Removed rows:   %s", f"{total_removed:,}")
    logger.info("  Retention rate: %.2f%%", retention)
    logger.info("=" * 80)

    return filtered_df


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def extract_columns(
    input_file: Path,
    columns_to_extract: list[str] | None = None,
    extract_all: bool = False,
) -> Path | None:
    """Extract and filter columns from a B3 InstrumentsConsolidated CSV file.

    Handles B3-specific quirks automatically:

    * Detects and skips a metadata header line (e.g. ``Status do Arquivo: Final``).
    * Auto-detects the column delimiter by counting candidate characters in
      the header row.
    * Tries multiple encodings (UTF-8, Latin-1, ISO-8859-1, CP1252) to
      accommodate both older and newer B3 file formats.

    Filters are applied before column extraction so that row reduction occurs
    on the full dataset rather than the reduced column set.

    Args:
        input_file:         Path to the input CSV file.
        columns_to_extract: List of column names to retain.  Ignored when
                            ``extract_all`` is ``True``.
        extract_all:        When ``True``, all columns are retained and
                            ``columns_to_extract`` is ignored.

    Returns:
        The :class:`~pathlib.Path` of the output file on success, or ``None``
        if reading, filtering, or writing fails for any reason.
    """
    try:
        logger.info("=" * 80)
        logger.info("Starting column extraction from: %s", input_file.name)
        logger.info("=" * 80)
        logger.info("Reading input CSV file...")

        # -- Detect optional metadata header --
        skip_rows = 0
        with open(input_file, "r", encoding="utf-8", errors="ignore") as fh:
            first_line = fh.readline().strip()

        if ("Status" in first_line or "Arquivo" in first_line or ":" in first_line):
            if ";" not in first_line or first_line.count(";") < 5:
                logger.info("Detected metadata header: %s", first_line[:100])
                skip_rows = 1

        # -- Auto-detect delimiter --
        with open(input_file, "r", encoding="utf-8", errors="ignore") as fh:
            for _ in range(skip_rows):
                fh.readline()
            header_line = fh.readline()

        delimiter_candidates = {
            ",":  header_line.count(","),
            ";":  header_line.count(";"),
            "|":  header_line.count("|"),
            "\t": header_line.count("\t"),
        }
        detected_delimiter = max(delimiter_candidates, key=delimiter_candidates.get)
        logger.info(
            "Detected delimiter: '%s' (count in header: %s)",
            detected_delimiter,
            delimiter_candidates[detected_delimiter],
        )

        if delimiter_candidates[detected_delimiter] == 0:
            logger.error(
                "No delimiter detected - file may be corrupted or in an unexpected format"
            )
            logger.info("Header preview: %s", header_line[:200])
            return None

        # -- Read CSV, trying encodings in order --
        df: pd.DataFrame | None = None

        for encoding in _ENCODINGS:
            try:
                df = pd.read_csv(
                    input_file,
                    encoding=encoding,
                    sep=detected_delimiter,
                    skiprows=skip_rows,
                    engine="python",
                    skipinitialspace=True,
                    skip_blank_lines=True,
                )
                logger.info(
                    "Successfully read file with encoding: %s, delimiter: '%s'",
                    encoding,
                    detected_delimiter,
                )
                break
            except Exception as exc:
                logger.warning("Failed with encoding %s: %s", encoding, str(exc)[:150])

        if df is None:
            logger.error("Failed to read CSV file with any supported encoding")
            logger.info(
                "Tip: run 'python instruments_extractor.py diagnose %s' for details",
                input_file,
            )
            return None

        # -- Initial row statistics --
        initial_rows = len(df)
        logger.info(
            "Input file contains %s rows and %s columns",
            f"{initial_rows:,}",
            len(df.columns),
        )
        logger.info("First 10 columns: %s", list(df.columns)[:10])

        # Drop rows where every cell is null
        df = df.dropna(how="all")
        if len(df) < initial_rows:
            logger.info(
                "Removed %s completely empty rows; %s rows remaining",
                f"{initial_rows - len(df):,}",
                f"{len(df):,}",
            )

        if len(df) == 0:
            logger.error("No data rows remain after removing empty rows")
            return None

        # -- Apply row filters --
        if ENABLE_FILTERING:
            df = apply_filters(df)
            if len(df) == 0:
                logger.warning("All rows were removed by the active filters")
                return None

        # -- Select columns --
        if extract_all:
            logger.info("Extracting ALL columns (%s total)", len(df.columns))
            extracted_df = df.copy()
        else:
            missing = validate_columns(df, columns_to_extract)
            if missing:
                logger.warning(
                    "Cannot extract %s missing column(s): %s",
                    len(missing),
                    missing[:5],
                )
                logger.info("Proceeding with available columns only")
                columns_to_extract = [c for c in columns_to_extract if c in df.columns]

            if not columns_to_extract:
                logger.error("No valid columns to extract")
                logger.info("Available columns: %s", list(df.columns)[:10])
                return None

            logger.info(
                "Extracting %s column(s): %s", len(columns_to_extract), columns_to_extract
            )
            extracted_df = df[columns_to_extract].copy()

        if len(extracted_df) == 0:
            logger.error("Extracted dataframe is empty - nothing to save")
            return None

        # -- Log non-null statistics for first 5 columns --
        non_null_counts = extracted_df.notna().sum()
        logger.info("Non-null values per column (first 5 shown):")
        for col in extracted_df.columns[:5]:
            logger.info(
                "  %s: %s / %s rows",
                col,
                f"{non_null_counts[col]:,}",
                f"{len(extracted_df):,}",
            )

        # Drop rows where every extracted column is null
        extracted_df = extracted_df.dropna(how="all")
        logger.info(
            "After removing all-null rows: %s rows remain", f"{len(extracted_df):,}"
        )

        # -- Write output --
        timestamp       = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = (
            f"InstrumentsConsolidated_filtered_{timestamp}.csv"
            if ENABLE_FILTERING
            else f"InstrumentsConsolidated_extracted_{timestamp}.csv"
        )
        output_path = OUTPUT_DIR / output_filename

        logger.info("Writing output to: %s", output_filename)
        logger.info("Using delimiter: '%s'", OUTPUT_DELIMITER)
        extracted_df.to_csv(
            output_path, index=False, encoding="utf-8", sep=OUTPUT_DELIMITER
        )

        file_size = output_path.stat().st_size
        logger.info(
            "Successfully created output file (%s bytes)", f"{file_size:,}"
        )
        logger.info(
            "Output contains %s rows and %s columns",
            f"{len(extracted_df):,}",
            len(extracted_df.columns),
        )
        logger.info("=" * 80)
        logger.info("Extraction completed SUCCESSFULLY")
        logger.info("=" * 80)

        return output_path

    except FileNotFoundError:
        logger.error("Input file not found: %s", input_file)
        return None
    except pd.errors.EmptyDataError:
        logger.error("Input CSV file is empty: %s", input_file)
        return None
    except Exception as exc:
        logger.error("Error during extraction: %s", exc)
        logger.info("Extraction completed with FAILURE")
        return None


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

def process_latest_file() -> Path | None:
    """Find and process the most recently downloaded instruments file.

    Convenience wrapper that combines :func:`find_latest_file` and
    :func:`extract_columns` using the module-level configuration.

    Returns:
        The output :class:`~pathlib.Path` on success, or ``None``.
    """
    try:
        latest_file = find_latest_file()

        if not latest_file:
            logger.error("No input file found to process")
            return None

        return extract_columns(
            latest_file,
            columns_to_extract=COLUMNS_TO_EXTRACT,
            extract_all=EXTRACT_ALL_COLUMNS,
        )

    except Exception as exc:
        logger.error("Unexpected error in process_latest_file: %s", exc)
        return None


def process_specific_file(filepath: str | Path) -> Path | None:
    """Process a specific instruments file by path.

    Args:
        filepath: Absolute or relative path to the CSV file to process
            (string or :class:`~pathlib.Path`).

    Returns:
        The output :class:`~pathlib.Path` on success, or ``None``.
    """
    try:
        input_path = Path(filepath)

        if not input_path.exists():
            logger.error("File not found: %s", filepath)
            return None

        return extract_columns(
            input_path,
            columns_to_extract=COLUMNS_TO_EXTRACT,
            extract_all=EXTRACT_ALL_COLUMNS,
        )

    except Exception as exc:
        logger.error("Error processing specific file: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Diagnostic utilities
# ---------------------------------------------------------------------------

def diagnose_csv_structure(filepath: str | Path) -> None:
    """Print a structural analysis of a CSV file to assist with debugging.

    Reports delimiter counts, field-count consistency across the first 20
    rows, and a preview of the first five lines.  Useful when
    ``extract_columns`` fails due to an unexpected file format.

    Args:
        filepath: Path to the CSV file to inspect.
    """
    try:
        input_path = Path(filepath)

        if not input_path.exists():
            logger.error("File not found: %s", filepath)
            return

        logger.info("Diagnosing CSV structure for: %s", input_path.name)
        logger.info("=" * 80)

        with open(input_path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = [fh.readline() for _ in range(100)]

        first_line = lines[0].strip()
        delimiter_counts = {
            ",":  first_line.count(","),
            ";":  first_line.count(";"),
            "|":  first_line.count("|"),
            "\t": first_line.count("\t"),
        }

        logger.info("First-line delimiter counts:")
        for char, count in delimiter_counts.items():
            label = "Tab" if char == "\t" else repr(char)
            logger.info("  %s: %s", label, count)

        likely = max(delimiter_counts, key=delimiter_counts.get)
        logger.info(
            "Most likely delimiter: '%s' (count: %s)",
            likely,
            delimiter_counts[likely],
        )

        logger.info("Field counts per line (first 20 lines):")
        for i, line in enumerate(lines[:20], 1):
            logger.info("  Line %3d: %3d fields", i, line.count(likely) + 1)

        logger.info("First 5 lines of file:")
        for i, line in enumerate(lines[:5], 1):
            logger.info("  Line %d: %s", i, line.strip()[:200])

        logger.info("=" * 80)

    except Exception as exc:
        logger.error("Error diagnosing file: %s", exc)


def list_available_columns(filepath: str | Path) -> None:
    """Print all column names found in a CSV file.

    Useful for discovering the exact column names present in a new B3 file
    before updating :data:`COLUMNS_TO_EXTRACT`.

    Args:
        filepath: Path to the CSV file to inspect.
    """
    try:
        input_path = Path(filepath)

        if not input_path.exists():
            logger.error("File not found: %s", filepath)
            return

        df = None
        for encoding in _ENCODINGS:
            try:
                df = pd.read_csv(input_path, nrows=0, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            logger.error("Could not read file with any supported encoding")
            return

        logger.info("Available columns in %s:", input_path.name)
        logger.info("=" * 80)
        for idx, col in enumerate(df.columns, 1):
            print(f"  {idx:3d}. {col}")
        logger.info("=" * 80)
        logger.info("Total: %s columns", len(df.columns))

    except Exception as exc:
        logger.error("Error listing columns: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _USAGE = (
        "Usage:\n"
        "  python instruments_extractor.py                     "
        "- Process the most recently downloaded file\n"
        "  python instruments_extractor.py process  <filepath> "
        "- Process a specific file\n"
        "  python instruments_extractor.py list     <filepath> "
        "- List all columns in a file\n"
        "  python instruments_extractor.py diagnose <filepath> "
        "- Diagnose CSV structure issues\n"
    )

    if len(sys.argv) > 1:
        _command = sys.argv[1]

        if _command == "diagnose" and len(sys.argv) > 2:
            diagnose_csv_structure(sys.argv[2])
        elif _command == "list" and len(sys.argv) > 2:
            list_available_columns(sys.argv[2])
        elif _command == "process" and len(sys.argv) > 2:
            _result = process_specific_file(sys.argv[2])
            if _result:
                print(f"\nOutput file: {_result}")
        else:
            print(_USAGE)
    else:
        _result = process_latest_file()
        if _result:
            print(f"\nOutput file: {_result}")

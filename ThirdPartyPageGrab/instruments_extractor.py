#!/usr/bin/env python3
"""
By : Craig Allum
Date : 24/02/2026
Version : v1.0
Purpose :

B3 Instruments Consolidated Column Extractor
Extracts specific columns from downloaded InstrumentsConsolidated CSV file
and creates a new processed CSV file ready from consumption into KSDS
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import glob

# Configuration Information
INPUT_DIR = Path("downloads")
OUTPUT_DIR = Path("processed")
LOG_DIR = Path("logs")

# Setup directories
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Setup logging
log_file = LOG_DIR / f"extractor_{datetime.now().strftime('%Y%m')}.log"

# Configure file handler with UTF-8 encoding
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configure console handler with UTF-8 encoding
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Force UTF-8 encoding on console handler stream
import sys
if sys.platform == 'win32':
    try:
        # Try to reconfigure stdout to UTF-8 as had issues on my local PC
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)


# THIS IS WHERE WE CONFIGURE COLUMNS, LIST THE ONES TO EXTRACT
# Update this list with the exact column names you want to extract
COLUMNS_TO_EXTRACT = [
    'TckrSymb',           # Ticker Symbol
    'ISIN',               # ISIN Code
    'Asst',               # Asset
    'AsstDesc',           # Asset Description
    'SgmtNm',             # Segment Name
    'MktNm',              # Market Name
    'SctyCtgyNm',         # Security Category Name
    'SpcfctnCd',          # Specification Code
    'XprtnDt',            # Expiration Date
    'XprtnCd',            # Expiration Code
    'TradgStartDt',       # Trading Start Date
    'TradgEndDt',         # Trading End Date
    'BaseCd',             # Base Code
    'ConvsBsBldr',        # Conversion Base Builder
    'CrpnNm',             # Corporation Name
    'CorpGovnLvlNm',      # Corporate Governance Level Name
]

# Alternative: Extract ALL columns (set to True to extract everything)
EXTRACT_ALL_COLUMNS = False

# Output CSV delimiter (semicolon is common for European/Brazilian systems)
OUTPUT_DELIMITER = ';'

# CONFIGURE FILTERS HERE
# Set to True to enable filtering
ENABLE_FILTERING = True

# Filter 1: SctyCtgyNm must contain 'BDR'
FILTER_SECURITY_CATEGORY = True
SECURITY_CATEGORY_VALUE = 'BDR'

# Filter 2: SpcfctnCd must start with 'DR2' or 'DR3'
FILTER_SPECIFICATION_CODE = True
SPECIFICATION_CODE_PREFIXES = ['DR2', 'DR3']


def find_latest_file(pattern="InstrumentsConsolidated_*.csv"):
    """
    Finds the most recently downloaded instruments file
    Returns the filepath or None if not found
    """
    try:
        # Search for files matching the pattern
        search_path = INPUT_DIR / pattern
        files = glob.glob(str(search_path))
        
        if not files:
            logger.warning(f"No files found matching pattern: {pattern}")
            return None
        
        # Sort by modification time (most recent first)
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest file: {latest_file}")
        
        return Path(latest_file)
        
    except Exception as e:
        logger.error(f"Error finding latest file: {e}")
        return None


def validate_columns(df, required_columns):
    """
    Validates that all required columns exist in the dataframe
    Returns list of missing columns or empty list if all present
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"Missing columns: {missing_columns}")
        logger.info(f"Available columns: {list(df.columns)}")
    
    return missing_columns


def apply_filters(df):
    """
    Applies configured filters to the dataframe
    
    Filter 1: SctyCtgyNm contains 'BDR'
    Filter 2: SpcfctnCd starts with 'DR2' or 'DR3'
    
    Returns:
        Filtered dataframe
    """
    if not ENABLE_FILTERING:
        logger.info("Filtering is DISABLED - returning all rows")
        return df
    
    original_count = len(df)
    filtered_df = df.copy()
    
    logger.info("Applying filters...")
    
    # Filter 1: Security Category contains 'BDR'
    if FILTER_SECURITY_CATEGORY:
        if 'SctyCtgyNm' not in filtered_df.columns:
            logger.warning("Column 'SctyCtgyNm' not found - skipping security category filter")
        else:
            # Convert to string and handle NaN values
            filtered_df['SctyCtgyNm'] = filtered_df['SctyCtgyNm'].fillna('').astype(str)
            
            before_count = len(filtered_df)
            filtered_df = filtered_df[filtered_df['SctyCtgyNm'].str.contains(SECURITY_CATEGORY_VALUE, case=False, na=False)]
            after_count = len(filtered_df)
            
            logger.info(f"Filter 1 (SctyCtgyNm contains '{SECURITY_CATEGORY_VALUE}'): {before_count:,} => {after_count:,} rows ({before_count - after_count:,} removed)")
    
    # Filter 2: Specification Code starts with DR2 or DR3
    if FILTER_SPECIFICATION_CODE:
        if 'SpcfctnCd' not in filtered_df.columns:
            logger.warning("Column 'SpcfctnCd' not found - skipping specification code filter")
        else:
            # Convert to string and handle NaN values
            filtered_df['SpcfctnCd'] = filtered_df['SpcfctnCd'].fillna('').astype(str)
            
            before_count = len(filtered_df)
            
            # Create condition for starts with DR2 or DR3
            condition = filtered_df['SpcfctnCd'].str.startswith(tuple(SPECIFICATION_CODE_PREFIXES))
            filtered_df = filtered_df[condition]
            
            after_count = len(filtered_df)
            
            prefixes_str = "' or '".join(SPECIFICATION_CODE_PREFIXES)
            logger.info(f"Filter 2 (SpcfctnCd starts with '{prefixes_str}'): {before_count:,} => {after_count:,} rows ({before_count - after_count:,} removed)")
    
    final_count = len(filtered_df)
    total_removed = original_count - final_count
    
    logger.info("=" * 80)
    logger.info(f"FILTERING SUMMARY:")
    logger.info(f"  Original rows:  {original_count:,}")
    logger.info(f"  Filtered rows:  {final_count:,}")
    logger.info(f"  Removed rows:   {total_removed:,}")
    logger.info(f"  Retention rate: {(final_count/original_count*100):.2f}%")
    logger.info("=" * 80)
    
    return filtered_df


def extract_columns(input_file, columns_to_extract=None, extract_all=False):
    """
    Extracts specified columns from the input CSV file
    
    Args:
        input_file: Path to input CSV file
        columns_to_extract: List of column names to extract (None if extract_all=True)
        extract_all: If True, extracts all columns
        
    Returns:
        Path to output file if successful, None otherwise
    """
    try:
        logger.info("=" * 80)
        logger.info(f"Starting column extraction from: {input_file.name}")
        logger.info("=" * 80)
        
        # Read the CSV file
        logger.info("Reading input CSV file...")
        
        # B3 files often have a metadata line at the top, detect this
        skip_rows = 0
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()
            # Check if first line is metadata (like "Status do Arquivo: Parcial")
            if 'Status' in first_line or 'Arquivo' in first_line or ':' in first_line:
                if ';' not in first_line or first_line.count(';') < 5:
                    logger.info(f"Detected metadata header: {first_line[:100]}")
                    skip_rows = 1
        
        # First, detect the actual delimiter by examining header line
        detected_delimiter = None
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            # Skip metadata lines
            for _ in range(skip_rows):
                f.readline()
            # Read header line
            header_line = f.readline()
            
        # Count delimiters in header
        delimiters = {
            ',': header_line.count(','),
            ';': header_line.count(';'),
            '|': header_line.count('|'),
            '\t': header_line.count('\t')
        }
        detected_delimiter = max(delimiters, key=delimiters.get)
        logger.info(f"Detected delimiter: '{detected_delimiter}' (count in header: {delimiters[detected_delimiter]})")
        
        if delimiters[detected_delimiter] == 0:
            logger.error("No delimiter detected - file might be corrupted or wrong format")
            logger.info(f"Header preview: {header_line[:200]}")
            return None
        
        # Try different encodings common in Brazilian files
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None
        successful_encoding = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(
                    input_file, 
                    encoding=encoding,
                    sep=detected_delimiter,
                    skiprows=skip_rows,  # Skip metadata line(s)
                    engine='python',
                    skipinitialspace=True,
                    skip_blank_lines=True
                )
                successful_encoding = encoding
                logger.info(f"Successfully read file with encoding: {encoding}, delimiter: '{detected_delimiter}'")
                break
            except Exception as e:
                logger.warning(f"Failed with encoding {encoding}: {str(e)[:150]}")
        
        if df is None:
            logger.error("Failed to read CSV file with any encoding")
            logger.info("Try running: python instruments_extractor.py diagnose <filepath>")
            return None
        
        # Log initial row count
        initial_rows = len(df)
        logger.info(f"Input file contains {initial_rows:,} rows and {len(df.columns)} columns")
        
        # Log column names for debugging
        logger.info(f"First 10 columns: {list(df.columns)[:10]}")
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        rows_after_drop = len(df)
        if rows_after_drop < initial_rows:
            logger.info(f"Removed {initial_rows - rows_after_drop:,} completely empty rows")
            logger.info(f"Remaining rows: {rows_after_drop:,}")
        
        if len(df) == 0:
            logger.error("No data rows remaining after removing empty rows")
            return None
        
        # Apply filters BEFORE column extraction
        if ENABLE_FILTERING:
            df = apply_filters(df)
            
            if len(df) == 0:
                logger.warning("All rows were filtered out - no data to extract")
                return None
        
        # Extract columns
        if extract_all:
            logger.info("Extracting ALL columns")
            extracted_df = df.copy()
        else:
            # Validate columns exist
            missing = validate_columns(df, columns_to_extract)
            
            if missing:
                logger.warning(f"Cannot extract {len(missing)} missing columns: {missing[:5]}")  # Show first 5
                logger.info("Proceeding with available columns only")
                # Extract only columns that exist
                columns_to_extract = [col for col in columns_to_extract if col in df.columns]
            
            if not columns_to_extract:
                logger.error("No valid columns to extract!")
                logger.info(f"Available columns: {list(df.columns)[:10]}")  # Show first 10
                return None
            
            logger.info(f"Extracting {len(columns_to_extract)} columns: {columns_to_extract}")
            extracted_df = df[columns_to_extract].copy()
        
        # Check for empty dataframe
        if len(extracted_df) == 0:
            logger.error("Extracted dataframe is empty - no rows to save")
            return None
        
        # Log sample of data
        non_null_counts = extracted_df.notna().sum()
        logger.info(f"Non-null values per column:")
        for col in extracted_df.columns[:5]:  # Show first 5 columns
            logger.info(f"  {col}: {non_null_counts[col]:,} / {len(extracted_df):,} rows")
        
        # Remove rows where ALL extracted columns are null
        extracted_df = extracted_df.dropna(how='all')
        logger.info(f"After removing all-null rows: {len(extracted_df):,} rows remain")
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Add 'filtered' to filename if filtering is enabled
        if ENABLE_FILTERING:
            output_filename = f"InstrumentsConsolidated_filtered_{timestamp}.csv"
        else:
            output_filename = f"InstrumentsConsolidated_extracted_{timestamp}.csv"
        
        output_path = OUTPUT_DIR / output_filename
        
        # Save to CSV with semicolon delimiter
        logger.info(f"Writing output to: {output_filename}")
        logger.info(f"Using delimiter: '{OUTPUT_DELIMITER}'")
        extracted_df.to_csv(output_path, index=False, encoding='utf-8', sep=OUTPUT_DELIMITER)
        
        file_size = output_path.stat().st_size
        logger.info(f"Successfully created output file ({file_size:,} bytes)")
        logger.info(f"Output contains {len(extracted_df):,} rows and {len(extracted_df.columns)} columns")
        logger.info("=" * 80)
        logger.info("Extraction completed SUCCESSFULLY")
        logger.info("=" * 80)
        
        return output_path
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
        return None
    except pd.errors.EmptyDataError:
        logger.error("Input CSV file is empty")
        return None
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        logger.info("Extraction completed with FAILURE")
        return None


def process_latest_file():
    """
    Main function to find and process the latest downloaded file
    """
    try:
        # Find the latest file
        latest_file = find_latest_file()
        
        if not latest_file:
            logger.error("No input file found to process")
            return None
        
        # Extract columns
        output_file = extract_columns(
            latest_file,
            columns_to_extract=COLUMNS_TO_EXTRACT,
            extract_all=EXTRACT_ALL_COLUMNS
        )
        
        return output_file
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None


def process_specific_file(filepath):
    """
    Process a specific file by path
    
    Args:
        filepath: Path to the input CSV file (string or Path object)
        
    Returns:
        Path to output file if successful, None otherwise
    """
    try:
        input_path = Path(filepath)
        
        if not input_path.exists():
            logger.error(f"File not found: {filepath}")
            return None
        
        output_file = extract_columns(
            input_path,
            columns_to_extract=COLUMNS_TO_EXTRACT,
            extract_all=EXTRACT_ALL_COLUMNS
        )
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return None


def diagnose_csv_structure(filepath):
    """
    Diagnose CSV file structure issues
    Useful for debugging malformed CSV files
    
    Args:
        filepath: Path to the CSV file
    """
    try:
        input_path = Path(filepath)
        
        if not input_path.exists():
            logger.error(f"File not found: {filepath}")
            return
        
        logger.info(f"Diagnosing CSV structure for: {input_path.name}")
        logger.info("=" * 80)
        
        # Read first 100 lines manually to inspect
        with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = [f.readline() for _ in range(100)]
        
        # Analyze delimiter
        first_line = lines[0].strip()
        comma_count = first_line.count(',')
        semicolon_count = first_line.count(';')
        pipe_count = first_line.count('|')
        tab_count = first_line.count('\t')
        
        logger.info(f"First line delimiter counts:")
        logger.info(f"  Commas (,): {comma_count}")
        logger.info(f"  Semicolons (;): {semicolon_count}")
        logger.info(f"  Pipes (|): {pipe_count}")
        logger.info(f"  Tabs: {tab_count}")
        
        # Likely delimiter
        delimiters = {',': comma_count, ';': semicolon_count, '|': pipe_count, '\t': tab_count}
        likely_delimiter = max(delimiters, key=delimiters.get)
        logger.info(f"Likely delimiter: '{likely_delimiter}' (count: {delimiters[likely_delimiter]})")
        
        # Check field count consistency
        logger.info(f"\nField counts per line (first 20 lines):")
        for i, line in enumerate(lines[:20], 1):
            field_count = line.count(likely_delimiter) + 1
            logger.info(f"  Line {i:3d}: {field_count:3d} fields")
        
        # Show first 5 lines
        logger.info(f"\nFirst 5 lines of file:")
        for i, line in enumerate(lines[:5], 1):
            logger.info(f"Line {i}: {line.strip()[:200]}")  # First 200 chars
        
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error diagnosing file: {e}")


def list_available_columns(filepath):
    """
    Utility function to list all columns in a CSV file
    Useful for discovering column names before extraction
    
    Args:
        filepath: Path to the CSV file
    """
    try:
        input_path = Path(filepath)
        
        if not input_path.exists():
            logger.error(f"File not found: {filepath}")
            return
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(input_path, nrows=0, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            logger.error("Failed to read file")
            return
        
        logger.info(f"Available columns in {input_path.name}:")
        logger.info("=" * 80)
        for idx, col in enumerate(df.columns, 1):
            print(f"{idx:3d}. {col}")
        logger.info("=" * 80)
        logger.info(f"Total: {len(df.columns)} columns")
        
    except Exception as e:
        logger.error(f"Error listing columns: {e}")


if __name__ == "__main__":
    import sys
    
    # Command line usage
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "diagnose" and len(sys.argv) > 2:
            # Diagnose CSV structure issues
            diagnose_csv_structure(sys.argv[2])
        elif command == "list" and len(sys.argv) > 2:
            # List columns in a specific file
            list_available_columns(sys.argv[2])
        elif command == "process" and len(sys.argv) > 2:
            # Process a specific file
            result = process_specific_file(sys.argv[2])
            if result:
                print(f"\nOutput file: {result}")
        else:
            print("Usage:")
            print("  python instruments_extractor.py diagnose <filepath> - Diagnose CSV structure")
            print("  python instruments_extractor.py list <filepath>     - List all columns")
            print("  python instruments_extractor.py process <filepath>  - Process specific file")
            print("  python instruments_extractor.py                     - Process latest file")
    else:
        # Process the latest file
        result = process_latest_file()
        if result:
            print(f"\nOutput file: {result}")
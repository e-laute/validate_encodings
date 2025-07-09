#!/usr/bin/env python3
"""
Batch process MEI files to extract metadata and convert to PROV-O RDF in Turtle format.

Usage:
    python parse_provenance_prov_batch.py <directory> > provenance_batch.ttl

This script walks through a directory structure, finds all .mei and .tei files,
and extracts their metadata as PROV-O RDF statements in Turtle format.
"""
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

def find_mei_files(directory):
    """Find all .mei and .tei files in the directory structure."""
    mei_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.mei') or file.endswith('.tei'):
                mei_files.append(os.path.join(root, file))
    return mei_files

def extract_prov_from_file(file_path):
    """Extract PROV-O RDF from a single MEI file using the parse_provenance_prov.py script."""
    try:
        result = subprocess.run(
            [sys.executable, 'parse_provenance_prov.py', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        return {
            'file_path': file_path,
            'success': True,
            'ttl_output': result.stdout.strip()
        }
    except subprocess.CalledProcessError as e:
        return {
            'file_path': file_path,
            'success': False,
            'error': f"Script error: {e.stderr.strip()}"
        }
    except Exception as e:
        return {
            'file_path': file_path,
            'success': False,
            'error': f"Unexpected error: {e}"
        }

def main(directory):
    """Main function to process all MEI files and generate PROV-O TTL."""
    mei_files = find_mei_files(directory)
    if not mei_files:
        print("# No .mei or .tei files found in the specified directory.", file=sys.stderr)
        return True

    successful_extractions = 0
    failed_extractions = 0
    all_ttl_outputs = []

    print(f"# Processing {len(mei_files)} MEI/TEI files for PROV-O conversion...", file=sys.stderr)
    print("="*60, file=sys.stderr)

    for mei_file in mei_files:
        print(f"# Processing: {mei_file}", file=sys.stderr)
        result = extract_prov_from_file(mei_file)
        
        if result['success']:
            successful_extractions += 1
            print(f"# ✅ Successfully extracted PROV-O from {mei_file}", file=sys.stderr)
            all_ttl_outputs.append(result['ttl_output'])
        else:
            failed_extractions += 1
            print(f"# ❌ Failed to extract PROV-O from {mei_file}: {result['error']}", file=sys.stderr)

    # Generate summary report to stderr
    print("\n" + "="*60, file=sys.stderr)
    print("PROV-O EXTRACTION SUMMARY", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"Total files processed: {len(mei_files)}", file=sys.stderr)
    print(f"Successful extractions: {successful_extractions}", file=sys.stderr)
    print(f"Failed extractions: {failed_extractions}", file=sys.stderr)
    
    if failed_extractions > 0:
        print("\n# FAILED EXTRACTIONS:", file=sys.stderr)
        for result in [r for r in [extract_prov_from_file(f) for f in mei_files] if not r['success']]:
            print(f"# ❌ {result['file_path']}: {result['error']}", file=sys.stderr)

    # Output combined TTL to stdout
    if all_ttl_outputs:
        print("# Combined PROV-O RDF in Turtle format")
        print("# Generated from MEI files in:", directory)
        print("# Generated at:", datetime.now().isoformat())
        print("# Total files processed:", len(mei_files))
        print("# Successful extractions:", successful_extractions)
        print("# Failed extractions:", failed_extractions)
        print()
        
        # Combine all TTL outputs
        combined_ttl = "\n\n".join(all_ttl_outputs)
        print(combined_ttl)
    
    return failed_extractions == 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_provenance_prov_batch.py <directory> > provenance_batch.ttl", file=sys.stderr)
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"The specified path '{directory}' is not a directory.", file=sys.stderr)
        sys.exit(1)
    
    success = main(directory)
    print("\n# PROV-O extraction completed.", file=sys.stderr)
    if not success:
        sys.exit(1)
    sys.exit(0) 
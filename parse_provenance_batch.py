#!/usr/bin/env python3
"""
Batch process MEI files to extract metadata from their headers.

Usage:
    python parse_provenance_batch.py <directory>

This script walks through a directory structure, finds all .mei and .tei files,
and extracts their metadata using the parse_provenance.py script.
"""
import os
import sys
import json
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

def extract_metadata_from_file(file_path):
    """Extract metadata from a single MEI file using the parse_provenance.py script."""
    try:
        result = subprocess.run(
            [sys.executable, 'parse_provenance.py', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        metadata = json.loads(result.stdout.strip())
        return {
            'file_path': file_path,
            'success': True,
            'metadata': metadata
        }
    except subprocess.CalledProcessError as e:
        return {
            'file_path': file_path,
            'success': False,
            'error': f"Script error: {e.stderr.strip()}"
        }
    except json.JSONDecodeError as e:
        return {
            'file_path': file_path,
            'success': False,
            'error': f"JSON parsing error: {e}"
        }
    except Exception as e:
        return {
            'file_path': file_path,
            'success': False,
            'error': f"Unexpected error: {e}"
        }

def main(directory):
    """Main function to process all MEI files and extract metadata."""
    mei_files = find_mei_files(directory)
    if not mei_files:
        print("No .mei or .tei files found in the specified directory.")
        return True

    all_results = []
    successful_extractions = 0
    failed_extractions = 0

    print(f"Found {len(mei_files)} MEI/TEI files to process...")
    print("="*60)

    for mei_file in mei_files:
        print(f"Processing: {mei_file}")
        result = extract_metadata_from_file(mei_file)
        all_results.append(result)
        
        if result['success']:
            successful_extractions += 1
            print(f"✅ Successfully extracted metadata from {mei_file}")
        else:
            failed_extractions += 1
            print(f"❌ Failed to extract metadata from {mei_file}: {result['error']}")

    # Generate summary report
    print("\n" + "="*60)
    print("METADATA EXTRACTION SUMMARY")
    print("="*60)
    print(f"Total files processed: {len(mei_files)}")
    print(f"Successful extractions: {successful_extractions}")
    print(f"Failed extractions: {failed_extractions}")
    
    if failed_extractions > 0:
        print("\nFAILED EXTRACTIONS:")
        for result in all_results:
            if not result['success']:
                print(f"❌ {result['file_path']}: {result['error']}")

    # Save detailed results to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"metadata_extraction_{timestamp}.json"
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_files': len(mei_files),
            'successful_extractions': successful_extractions,
            'failed_extractions': failed_extractions
        },
        'results': all_results
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nDetailed results saved to: {output_file}")
    
    # Print sample of extracted metadata
    if successful_extractions > 0:
        print("\nSAMPLE EXTRACTED METADATA:")
        print("-" * 40)
        for result in all_results[:3]:  # Show first 3 successful extractions
            if result['success']:
                metadata = result['metadata']
                print(f"\nFile: {result['file_path']}")
                
                # Show key metadata sections
                if 'fileDesc' in metadata:
                    file_desc = metadata['fileDesc']
                    if 'titleStmt' in file_desc:
                        title_stmt = file_desc['titleStmt']
                        if 'title' in title_stmt:
                            titles = title_stmt['title']
                            if isinstance(titles, list):
                                for title in titles[:2]:  # Show first 2 titles
                                    if isinstance(title, dict) and '#text' in title:
                                        print(f"  Title: {title['#text'][:80]}...")
                            elif isinstance(titles, dict) and '#text' in titles:
                                print(f"  Title: {titles['#text'][:80]}...")
                
                if 'encodingDesc' in metadata:
                    encoding_desc = metadata['encodingDesc']
                    if 'appInfo' in encoding_desc:
                        app_info = encoding_desc['appInfo']
                        if 'application' in app_info:
                            apps = app_info['application']
                            if isinstance(apps, list):
                                for app in apps[:2]:  # Show first 2 applications
                                    if isinstance(app, dict) and 'name' in app:
                                        name = app['name']
                                        if isinstance(name, dict) and '#text' in name:
                                            print(f"  Application: {name['#text']}")
                                        elif isinstance(name, str):
                                            print(f"  Application: {name}")
                            elif isinstance(apps, dict) and 'name' in apps:
                                name = apps['name']
                                if isinstance(name, dict) and '#text' in name:
                                    print(f"  Application: {name['#text']}")
                                elif isinstance(name, str):
                                    print(f"  Application: {name}")
    
    return failed_extractions == 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_provenance_batch.py <directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"The specified path '{directory}' is not a directory.")
        sys.exit(1)
    
    success = main(directory)
    print("\nMetadata extraction completed.")
    if not success:
        sys.exit(1)
    sys.exit(0) 
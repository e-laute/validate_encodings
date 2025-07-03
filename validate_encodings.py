# This python script recursively traverses a directory structure, 
# finding all .mei files and validating them against MEI's RNG schema using the `lxml` library.
import os
import sys
import io
from lxml import etree
import requests
def validate_mei_file(file_path, schema):
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        doc = etree.fromstring(content)
        schema.assertValid(doc)
        print(f"Validation successful for {file_path}")
    except etree.DocumentInvalid as e:
        print(f"Validation failed for {file_path}: {e}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def find_mei_files(directory):
    mei_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.mei'):
                mei_files.append(os.path.join(root, file))
    return mei_files
def main(directory):
    schemas = dict()
    mei_files = find_mei_files(directory)
    if not mei_files:
        print("No .mei files found in the specified directory.")
        return
    for mei_file in mei_files:
        # read file into DOM and determine schema URL
        try:
            with open(mei_file, 'rb') as f:
                content = f.read()
            doc = etree.fromstring(content)
            # determine schema URL from the document
            # checking in <?xml-model?> declaration 
            schema_url = None
            for pi in doc.getroottree().docinfo.PIs():
                if pi.target == "xml-model":
                    # Parse the pseudo-attributes
                    attrs = pi.pseudo_attrib
                    href = attrs.get("href")
                    if href:
                        schema_url = href
                        break
            if not schema_url:
                print(f"No schema URL found in {mei_file}.")
                continue
            if schema_url not in schemas:
                # fetch the schema file using requests
                schema_response = requests.get(schema_url)
                if schema_response.status_code != 200:
                    print(f"Error fetching schema from {schema_url}: {schema_response.status_code}")
                    sys.exit(1)
                schema_file = io.BytesIO(schema_response.content)
                try:
                    schemas[schema_url] = etree.RelaxNG(etree.parse(schema_file))
                except etree.XMLSyntaxError as e:
                    print(f"Error loading schema from {schema_url}: {e}")
                    sys.exit(1)
            validate_mei_file(mei_file, schemas[schema_url])
        except etree.XMLSyntaxError as e:
            print(f"Error parsing {mei_file}: {e}")
            continue


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_mei.py <directory>")
        sys.exit(1)
    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"The specified path '{directory}' is not a directory.")
        sys.exit(1)
    main(directory)
    print("MEI validation completed.")
    sys.exit(0)

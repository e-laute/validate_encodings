#!/usr/bin/env python3
"""
Extract MEI header metadata (meiHead) and dump it as JSON.

Usage:
    python mei_metadata_to_json.py path/to/input.mei > metadata.json

The script walks the <meiHead> element described in the MEI Guidelines v5
metadata chapter and converts it (including attributes) into a JSON structure.
This is intended as a first step – you can later post‑process or refine the
mapping if a more constrained schema is desired.
"""
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

# Official MEI namespace used in MEI 5.x files
MEI_NS = {"mei": "http://www.music-encoding.org/ns/mei"}

def _strip_ns(tag: str) -> str:
    """Return local part of a tag name (remove namespace)."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _elem_to_dict(elem: ET.Element):
    """Recursively convert an ElementTree node to a python dict.

    * Text‑only elements become the text string directly.
    * Mixed‑content or attribute‑bearing elements become dicts where:
        - "@attributes" holds element attributes (if any)
        - child element(s) are grouped by their local tag name
          (a single child is stored as an object, multiple children
          as a list)
        - "#text" stores significant text that appears *alongside*
          children.
    """
    # Start with attributes
    node: dict = {}
    if elem.attrib:
        node["@attributes"] = dict(elem.attrib)

    # Process children
    children = list(elem)
    if children:
        grouped = defaultdict(list)
        for child in children:
            grouped[_strip_ns(child.tag)].append(_elem_to_dict(child))
        # Promote singletons to objects instead of 1‑element lists
        for key, value in grouped.items():
            node[key] = value[0] if len(value) == 1 else value

    # Handle text nodes
    text = (elem.text or "").strip()
    if text:
        if children or elem.attrib:
            node["#text"] = text
        else:
            # Element is text‑only: return simple string
            return text
    return node


def parse_mei_metadata(path: Path | str):
    """Return a python dict representing the <meiHead> of *path*."""
    tree = ET.parse(path)
    root = tree.getroot()
    local_root = _strip_ns(root.tag)

    if local_root == "mei":
        mei_head = root.find("mei:meiHead", MEI_NS)
    elif local_root == "meiHead":
        mei_head = root
    else:
        raise ValueError("The provided file doesn't look like an MEI document.")

    if mei_head is None:
        raise ValueError("<meiHead> element not found – is the file valid MEI?")

    return _elem_to_dict(mei_head)


def main(argv: list[str]):
    if len(argv) != 2 or argv[1] in {"-h", "--help"}:
        print(__doc__.strip())
        return 1

    mei_path = Path(argv[1])
    if not mei_path.is_file():
        print(f"Error: '{mei_path}' is not a file.", file=sys.stderr)
        return 2

    metadata = parse_mei_metadata(mei_path)
    json.dump(metadata, sys.stdout, ensure_ascii=False, indent=2)
    print()  # newline after JSON
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

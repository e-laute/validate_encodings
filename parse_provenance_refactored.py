"""
Minimal rdflib refactor of parse_provenance_prov.py (step 1):
- Use rdflib to build the RDF graph
- Parse <meiHead> and ingest a subset:
  - Define file and work entities
  - Add dcterms:title from titleStmt
  - Add basic foaf:Person and foaf:Organization from respStmt
  - Link people as dcterms:creator and orgs as dcterms:contributor
  - Attach prov:hadRole using E-LAUTE roles or common LOC relator codes (fallback literal)

CLI:
  python pasrse_provanance_refactored.py <mei_file> [--ttl OUTPUT_TTL]
If --ttl is omitted, Turtle is written to stdout.
"""

import sys
import argparse
import re
from pathlib import Path
import xml.etree.ElementTree as ET

from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, DCTERMS, FOAF


# Namespaces
PROV = Namespace("http://www.w3.org/ns/prov#")
LOC = Namespace("http://id.loc.gov/vocabulary/relators/")
# E-LAUTE namespaces: vocab (predicates/classes) and data (instances)
ELAUTE = Namespace("https://e-laute.info/vocab#")
ELAUTE_DATA = Namespace("https://e-laute.info/data/")

MEI_XML = {"mei": "http://www.music-encoding.org/ns/mei"}


# E-LAUTE-specific roles and a tiny LOC mapping for common roles
ELAUTE_ROLE_MAPPING = {
    "meiEditor": ELAUTE.meiEditor,
    "fronimoEditor": ELAUTE.fronimoEditor,
    "musescoreEditor": ELAUTE.musescoreEditor,
    "metadataContact": ELAUTE.metadataContact,
    "intabulator": ELAUTE.intabulator,
    "provider": ELAUTE.provider,
    "funder": ELAUTE.funder,
    "publisher": ELAUTE.publisher,
}

LOC_ROLE_CODE = {
    # minimal set for first step
    "editor": "edt",
    "arranger": "arr",
    "publisher": "pbl",
    "author": "aut",
    "composer": "cmp",
    "scribe": "scr",
    "collector": "col",
    "funder": "fnd",
}


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _clean_uri(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"[^\w\s-]", "", text)
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    return cleaned.lower()


def _extract_text(elem: ET.Element) -> str:
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text.strip())
    for child in elem:
        if child.text:
            parts.append(child.text.strip())
        if child.tail:
            parts.append(child.tail.strip())
    return " ".join([p for p in parts if p]).strip()


def _role_to_node(role: str):
    if not role:
        return Literal("contributor")
    if role in ELAUTE_ROLE_MAPPING:
        return ELAUTE_ROLE_MAPPING[role]
    code = LOC_ROLE_CODE.get(role)
    if code:
        return LOC[code]
    return Literal(role)


def parse_mei_head(path: Path) -> ET.Element:
    tree = ET.parse(path)
    root = tree.getroot()
    local = _strip_ns(root.tag)
    if local == "mei":
        head = root.find("mei:meiHead", MEI_XML)
    elif local == "meiHead":
        head = root
    else:
        raise ValueError("The provided file doesn't look like an MEI document.")
    if head is None:
        raise ValueError("<meiHead> element not found – is the file valid MEI?")
    return head


def build_graph_from_head(head: ET.Element, file_path: Path) -> Graph:
    g = Graph()
    # Bind prefixes for nicer TTL
    g.bind("prov", PROV)
    g.bind("foaf", FOAF)
    g.bind("dcterms", DCTERMS)
    g.bind("loc", LOC)
    # Prefixes: 'elaute' for data instances, 'elautev' for vocabulary terms
    g.bind("elaute", ELAUTE_DATA)
    g.bind("elautev", ELAUTE)

    file_id = _clean_uri(file_path.stem)

    # Extract contentitem_id from PID identifier in pubStmt
    pub_stmt = head.find("mei:fileDesc/mei:pubStmt", MEI_XML)
    contentitem_id = None
    if pub_stmt is not None:
        identifier = pub_stmt.find("mei:identifier[@type='PID']", MEI_XML)
        if identifier is not None and identifier.text:
            # Extract the part after "o:lau." prefix
            pid_text = identifier.text.strip()
            if pid_text.startswith("o:lau."):
                contentitem_id = pid_text[6:]  # Remove "o:lau." prefix
            else:
                contentitem_id = _clean_uri(pid_text)

    # Fallback to filename stem if no PID found
    if not contentitem_id:
        contentitem_id = _clean_uri(file_path.stem)

    # Source entity minted in E-LAUTE data namespace
    file_node = URIRef(ELAUTE_DATA + f"files/{file_id}")
    # Create an encoding activity for qualified associations
    activity_node = URIRef(ELAUTE_DATA + f"activities/{file_id}_mei")


    # Minimal link from work to file
    g.add((file_node, RDF.type, PROV.Entity))
    # Link encoding entity to its generating activity
    g.add((file_node, PROV.wasGeneratedBy, activity_node))
    g.add((activity_node, RDF.type, ELAUTE.meiEncodingActivity))

    # titleStmt (titles omitted by request) -> process only respStmt
    title_stmt = head.find("mei:fileDesc/mei:titleStmt", MEI_XML)
    if title_stmt is not None:
        # respStmt -> people only (organizations/funders removed)
        for resp in title_stmt.findall("mei:respStmt", MEI_XML):
            # Persons
            for p in resp.findall("mei:persName", MEI_XML):
                role = p.get("role", "creator")
                auth_uri = p.get("authURI") or p.get("authUri") or p.get("auth.uri")
                if auth_uri:
                    agent_node = URIRef(auth_uri)
                else:
                    raise ValueError(f"No authURI found for person {p.text}")

                # Define FOAF/PROV person
                g.add((agent_node, RDF.type, FOAF.Person))
                g.add((agent_node, RDF.type, PROV.Agent))
                fn = p.find("mei:foreName", MEI_XML)
                ln = p.find("mei:famName", MEI_XML)
                if fn is not None and (fn.text or '').strip():
                    g.add((agent_node, FOAF.givenName, Literal((fn.text or '').strip())))
                if ln is not None and (ln.text or '').strip():
                    g.add((agent_node, FOAF.familyName, Literal((ln.text or '').strip())))

                # Add qualified attribution for metadataContact
                if role == "metadataContact":
                    attr_bn = BNode()
                    g.add((file_node, PROV.qualifiedAttribution, attr_bn))
                    g.add((attr_bn, RDF.type, PROV.Attribution))
                    g.add((attr_bn, PROV.agent, agent_node))
                    g.add((attr_bn, PROV.hadRole, _role_to_node(role)))

                # Only add to activity if not metadataContact
                if role != "metadataContact":
                    # Qualified association on the encoding activity
                    g.add((activity_node, PROV.wasAssociatedWith, agent_node))
                    assoc_bn = BNode()
                    g.add((activity_node, PROV.qualifiedAssociation, assoc_bn))
                    g.add((assoc_bn, RDF.type, PROV.Association))
                    g.add((assoc_bn, PROV.agent, agent_node))
                    g.add((assoc_bn, PROV.hadRole, _role_to_node(role)))

    return g


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract MEI header metadata to PROV-O RDF (rdflib refactor)")
    parser.add_argument("mei_file", help="Path to an MEI file")
    parser.add_argument("--ttl", dest="ttl_output", default=None, help="Output Turtle file path; if omitted, print to stdout")
    args = parser.parse_args()

    mei_path = Path(args.mei_file)
    if not mei_path.is_file():
        print(f"Error: '{mei_path}' is not a file.", file=sys.stderr)
        return 2

    try:
        head = parse_mei_head(mei_path)
        graph = build_graph_from_head(head, mei_path)
        if args.ttl_output:
            graph.serialize(destination=args.ttl_output, format="turtle")
            print(f"📝 RDF written to {args.ttl_output}")
        else:
            ttl = graph.serialize(format="turtle")
            # rdflib returns str in recent versions
            print(ttl)
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

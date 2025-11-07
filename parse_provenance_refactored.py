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
from datetime import datetime

from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, DCTERMS, FOAF, XSD


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

# Map contributor roles to E-LAUTE activity classes (from vocab.ttl comments)
# Only map where the README/vocab explicitly indicates a role in parentheses.
# If marked as (missing) or no role given, do not map.
ROLE_TO_ACTIVITY_TYPES = {
    # Editing
    "musescoreEditor": [ELAUTE.musescoreEditingActivity],
    "meiEditor": [
        ELAUTE.meiEditingActivity    ],
    # Typesetting
    "fronimoEditor": [ELAUTE.fronimoTypesettingActivity],
    # Note: Converting with luteconv mentions an application, not a role → skip
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


def _extract_notation_type(filename: str) -> str | None:
    """Extract notation type from filename (CMN, ILT, FLT, GLT).
    
    Looks for uppercase notation type codes at the end of the filename
    before the extension.
    """
    # Valid notation types from vocab.ttl
    valid_types = {"CMN", "ILT", "FLT", "GLT"}
    
    # Remove extension
    stem = Path(filename).stem
    
    # Split by underscore and check each part
    parts = stem.split("_")
    for part in reversed(parts):  # Check from end
        if part.upper() in valid_types:
            return part.upper()
    
    return None


def _detect_edition_type(filename: str) -> str:
    stem = Path(filename).stem.lower()
    if "_dipl" in stem or stem.startswith("dipl_"):
        return "dipl"
    if "_ed" in stem or stem.startswith("ed_"):
        return "ed"
    return "ed"


def _extract_app_names(head: ET.Element) -> set[str]:
    apps: set[str] = set()
    app_info = head.find("mei:encodingDesc/mei:appInfo", MEI_XML)
    if app_info is None:
        return apps
    for app in app_info.findall("mei:application", MEI_XML):
        name_elem = app.find("mei:name", MEI_XML)
        if name_elem is not None:
            text = (name_elem.text or "").strip().lower()
            if text:
                apps.add(text)
    return apps


def _extract_mei_timestamp(head: ET.Element, file_path: Path) -> Literal | None:
    date_elem = head.find("mei:fileDesc/mei:pubStmt/mei:date", MEI_XML)
    if date_elem is not None:
        iso = (date_elem.get("isodate") or "").strip()
        if iso:
            if len(iso) == 10 and iso.count("-") == 2:
                return Literal(iso, datatype=XSD.date)
            return Literal(iso, datatype=XSD.dateTime)
    try:
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
        return Literal(mtime, datatype=XSD.dateTime)
    except Exception:
        return None


def _extract_app_details(head: ET.Element, app_name: str) -> dict[str, str]:
    """Return details for application with given name from encodingDesc/appInfo.

    Keys: isodate, startdate, enddate, version
    """
    details: dict[str, str] = {}
    app_info = head.find("mei:encodingDesc/mei:appInfo", MEI_XML)
    if app_info is None:
        return details
    for app in app_info.findall("mei:application", MEI_XML):
        name_elem = app.find("mei:name", MEI_XML)
        name = (name_elem.text or "").strip().lower() if name_elem is not None else ""
        if name == app_name.lower():
            for key in ("isodate", "startdate", "enddate", "version"):
                val = app.get(key)
                if val:
                    details[key] = val
            break
    return details


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
    # Minimal link from work to file
    g.add((file_node, RDF.type, PROV.Entity))

    # Collect agents by role for pipeline associations
    fronimo_editors: set[URIRef] = set()
    musescore_editors: set[URIRef] = set()
    mei_editors: set[URIRef] = set()

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

                # Track agents per role for pipeline steps
                if role == "fronimoEditor":
                    fronimo_editors.add(agent_node)
                elif role == "musescoreEditor":
                    musescore_editors.add(agent_node)
                elif role == "meiEditor":
                    mei_editors.add(agent_node)

                # Add qualified attribution for metadataContact
                if role == "metadataContact":
                    attr_bn = BNode()
                    g.add((file_node, PROV.qualifiedAttribution, attr_bn))
                    g.add((attr_bn, RDF.type, PROV.Attribution))
                    g.add((attr_bn, PROV.agent, agent_node))
                    g.add((attr_bn, PROV.hadRole, _role_to_node(role)))

    # Decide path based on appInfo with role fallback
    apps = _extract_app_names(head)
    use_fronimo = "luteconv" in apps
    use_musescore = "verovio" in apps
    if use_fronimo and use_musescore:
        # Ambiguous; prefer luteconv by convention
        use_musescore = False
    if not use_fronimo and not use_musescore:
        # Fallback by roles if app info missing
        if fronimo_editors and not musescore_editors:
            use_fronimo = True
        elif musescore_editors and not fronimo_editors:
            use_musescore = True
        elif fronimo_editors and musescore_editors:
            raise ValueError("Both fronimoEditor and musescoreEditor present without appInfo disambiguation")

    # Pipeline Step 1: Fronimo typesetting or MuseScore editing → generated source entity
    if use_fronimo:
        a1 = URIRef(ELAUTE_DATA + f"activities/{file_id}_typesetting_1")
        g.add((a1, RDF.type, ELAUTE.fronimoTypesettingActivity))
        for agent in fronimo_editors:
            g.add((a1, PROV.wasAssociatedWith, agent))
            assoc_bn = BNode()
            g.add((a1, PROV.qualifiedAssociation, assoc_bn))
            g.add((assoc_bn, RDF.type, PROV.Association))
            g.add((assoc_bn, PROV.agent, agent))
            g.add((assoc_bn, PROV.hadRole, _role_to_node("fronimoEditor")))
        e1 = URIRef(ELAUTE_DATA + f"files/{file_id}_generated_ft3")
        g.add((e1, RDF.type, PROV.Entity))
        g.add((e1, PROV.wasGeneratedBy, a1))
        # Pipeline Step 1.5: Luteconv conversion → converted MEI from FT3
        a1_5 = URIRef(ELAUTE_DATA + f"activities/{file_id}_luteconv_1")
        g.add((a1_5, RDF.type, ELAUTE.luteconvConvertingActivity))
        g.add((a1_5, PROV.used, e1))
        # Associate software agent with version if available
        luteconv_details = _extract_app_details(head, "luteconv")
        luteconv_agent = URIRef(ELAUTE_DATA + "software/luteconv")
        g.add((luteconv_agent, RDF.type, PROV.SoftwareAgent))
        version_val = luteconv_details.get("version")
        g.add((a1_5, PROV.wasAssociatedWith, luteconv_agent))
        # Qualified association capturing version per run
        luteconv_assoc_bn = BNode()
        g.add((a1_5, PROV.qualifiedAssociation, luteconv_assoc_bn))
        g.add((luteconv_assoc_bn, RDF.type, PROV.Association))
        g.add((luteconv_assoc_bn, PROV.agent, luteconv_agent))
        if version_val:
            g.add((luteconv_assoc_bn, DCTERMS.hasVersion, Literal(version_val)))
        e_conv = URIRef(ELAUTE_DATA + f"files/{file_id}_converted_mei")
        g.add((e_conv, RDF.type, PROV.Entity))
        g.add((e_conv, PROV.wasGeneratedBy, a1_5))
        g.add((a1_5, PROV.generated, e_conv))
        g.add((e_conv, PROV.wasDerivedFrom, e1))
        # Timing from application metadata
        iso = luteconv_details.get("isodate")
        start = luteconv_details.get("startdate")
        end = luteconv_details.get("enddate")
        if start:
            g.add((a1_5, PROV.startedAtTime, Literal(start, datatype=XSD.dateTime)))
        if end:
            g.add((a1_5, PROV.endedAtTime, Literal(end, datatype=XSD.dateTime)))
            g.add((e_conv, PROV.generatedAtTime, Literal(end, datatype=XSD.dateTime)))
        elif iso:
            if len(iso) == 10 and iso.count("-") == 2:
                g.add((a1_5, PROV.endedAtTime, Literal(iso, datatype=XSD.date)))
                g.add((e_conv, PROV.generatedAtTime, Literal(iso, datatype=XSD.date)))
            else:
                g.add((a1_5, PROV.endedAtTime, Literal(iso, datatype=XSD.dateTime)))
                g.add((e_conv, PROV.generatedAtTime, Literal(iso, datatype=XSD.dateTime)))
        inferred_luteconv = _extract_notation_type(file_path.name)
        if inferred_luteconv:
            g.add((e_conv, ELAUTE.notationType, Literal(inferred_luteconv)))
    elif use_musescore:
        a1 = URIRef(ELAUTE_DATA + f"activities/{file_id}_musescore_editing_1")
        g.add((a1, RDF.type, ELAUTE.musescoreEditingActivity))
        for agent in musescore_editors:
            g.add((a1, PROV.wasAssociatedWith, agent))
            assoc_bn = BNode()
            g.add((a1, PROV.qualifiedAssociation, assoc_bn))
            g.add((assoc_bn, RDF.type, PROV.Association))
            g.add((assoc_bn, PROV.agent, agent))
            g.add((assoc_bn, PROV.hadRole, _role_to_node("musescoreEditor")))
        e1 = URIRef(ELAUTE_DATA + f"files/{file_id}_generated_musescorexml")
        g.add((e1, RDF.type, PROV.Entity))
        g.add((e1, PROV.wasGeneratedBy, a1))
        # Pipeline Step 1.5 (MuseScore path): Verovio conversion → converted MEI from MusicXML
        a1_5_vero = URIRef(ELAUTE_DATA + f"activities/{file_id}_verovio_1")
        # Use a specific activity type if available in vocab; otherwise this still creates a URI
        if hasattr(ELAUTE, 'verovioConvertingActivity'):
            g.add((a1_5_vero, RDF.type, ELAUTE.verovioConvertingActivity))
        else:
            g.add((a1_5_vero, RDF.type, ELAUTE.convertingActivity))
        g.add((a1_5_vero, PROV.used, e1))
        verovio_details = _extract_app_details(head, "verovio")
        verovio_agent = URIRef(ELAUTE_DATA + "software/verovio")
        g.add((verovio_agent, RDF.type, PROV.SoftwareAgent))
        g.add((a1_5_vero, PROV.wasAssociatedWith, verovio_agent))
        # Qualified association with per-run version
        verovio_assoc_bn = BNode()
        g.add((a1_5_vero, PROV.qualifiedAssociation, verovio_assoc_bn))
        g.add((verovio_assoc_bn, RDF.type, PROV.Association))
        g.add((verovio_assoc_bn, PROV.agent, verovio_agent))
        verovio_version = verovio_details.get("version")
        if verovio_version:
            g.add((verovio_assoc_bn, DCTERMS.hasVersion, Literal(verovio_version)))
        e_conv_vero = URIRef(ELAUTE_DATA + f"files/{file_id}_converted_mei")
        g.add((e_conv_vero, RDF.type, PROV.Entity))
        g.add((e_conv_vero, PROV.wasGeneratedBy, a1_5_vero))
        g.add((a1_5_vero, PROV.generated, e_conv_vero))
        g.add((e_conv_vero, PROV.wasDerivedFrom, e1))
        # Timing from application metadata
        iso_v = verovio_details.get("isodate")
        start_v = verovio_details.get("startdate")
        end_v = verovio_details.get("enddate")
        if start_v:
            g.add((a1_5_vero, PROV.startedAtTime, Literal(start_v, datatype=XSD.dateTime)))
        if end_v:
            g.add((a1_5_vero, PROV.endedAtTime, Literal(end_v, datatype=XSD.dateTime)))
            g.add((e_conv_vero, PROV.generatedAtTime, Literal(end_v, datatype=XSD.dateTime)))
        elif iso_v:
            if len(iso_v) == 10 and iso_v.count("-") == 2:
                g.add((a1_5_vero, PROV.endedAtTime, Literal(iso_v, datatype=XSD.date)))
                g.add((e_conv_vero, PROV.generatedAtTime, Literal(iso_v, datatype=XSD.date)))
            else:
                g.add((a1_5_vero, PROV.endedAtTime, Literal(iso_v, datatype=XSD.dateTime)))
                g.add((e_conv_vero, PROV.generatedAtTime, Literal(iso_v, datatype=XSD.dateTime)))
        # Notation for converted MEI in this path is CMN
        g.add((e_conv_vero, ELAUTE.notationType, Literal("CMN")))
    else:
        e1 = None
        e_conv = None

    # Pipeline Step 2: MEI Editing → generates MEI file entity
    a2 = URIRef(ELAUTE_DATA + f"activities/{file_id}_mei_editing_1")
    g.add((a2, RDF.type, ELAUTE.meiEditingActivity))
    for agent in mei_editors:
        g.add((a2, PROV.wasAssociatedWith, agent))
        assoc_bn = BNode()
        g.add((a2, PROV.qualifiedAssociation, assoc_bn))
        g.add((assoc_bn, RDF.type, PROV.Association))
        g.add((assoc_bn, PROV.agent, agent))
        g.add((assoc_bn, PROV.hadRole, _role_to_node("meiEditor")))

    source_for_editing = e1
    if use_fronimo:
        source_for_editing = e_conv
    elif use_musescore:
        source_for_editing = e_conv_vero
    if source_for_editing is not None:
        g.add((a2, PROV.used, source_for_editing))
        g.add((source_for_editing, PROV.wasUsedBy, a2))
    g.add((file_node, PROV.wasGeneratedBy, a2))
    g.add((a2, PROV.generated, file_node))
    if source_for_editing is not None:
        g.add((file_node, PROV.wasDerivedFrom, source_for_editing))

    # Set notation type on resulting MEI depending on path
    if use_musescore:
        g.add((file_node, ELAUTE.notationType, Literal("CMN")))
    else:
        inferred = _extract_notation_type(file_path.name)
        if inferred:
            g.add((file_node, ELAUTE.notationType, Literal(inferred)))

    # Pipeline Step 3 (fronimo only): derivative activity → ILT entity
    if use_fronimo:
        edition_kind = _detect_edition_type(file_path.name)  # 'ed' or 'dipl'
        a3 = URIRef(ELAUTE_DATA + f"activities/{file_id}_derivative_1")
        g.add((a3, RDF.type, ELAUTE.derivativeGeneratingActivity))
        g.add((a3, PROV.used, file_node))
        e2 = URIRef(ELAUTE_DATA + f"files/{file_id}_{edition_kind}_ILT")
        g.add((e2, RDF.type, PROV.Entity))
        g.add((e2, PROV.wasGeneratedBy, a3))
        g.add((e2, PROV.wasDerivedFrom, file_node))
        g.add((e2, ELAUTE.notationType, Literal("ILT")))

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

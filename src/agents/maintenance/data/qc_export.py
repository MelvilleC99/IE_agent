import sys
import os
import json
from datetime import datetime

# Setup sys.path to import config
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import FIREBASE_CONFIG

import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin once
if not firebase_admin._apps:
    cred_path = "/Users/melville/Documents/Industrial_Engineering_Agent/secrets/firebase-service-account.json"
    if not os.path.exists(cred_path):
        raise ValueError(f"Firebase credentials not found at: {cred_path}")
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    print(f"Firebase initialized with credentials from: {cred_path}")

db = firestore.client()

def get_document_dict(collection_name):
    """
    Fetch all docs from a collection and return a dict keyed by document ID.
    """
    docs = db.collection(collection_name).stream()
    doc_dict = {}
    for doc in docs:
        data = doc.to_dict()
        doc_dict[doc.id] = data
    return doc_dict

def parse_timestamp(ts):
    """
    Converts Firestore timestamp to ISO string.
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    try:
        return ts.to_datetime().isoformat()
    except Exception:
        return str(ts)

def enrich_and_format(doc, prod_lines, supervisors, styles):
    """
    Build enriched dict with required fields.
    """
    # Get related info
    line_id = doc.get("productionLineId")
    sup_id = doc.get("supervisorId")
    style_num = doc.get("styleNumber")

    production_line = prod_lines.get(line_id, {}).get("description", None)
    supervisor = supervisors.get(sup_id, {})
    supervisor_name = supervisor.get("name", "")
    supervisor_surname = supervisor.get("surname", "")
    style = styles.get(style_num, {})

    return {
        "productionLine": production_line,
        "supervisorName": supervisor_name,
        "supervisorSurname": supervisor_surname,
        "styleNumber": style_num,
        "cost": style.get("cost"),
        "createdAt": parse_timestamp(doc.get("createdAt")),
        "updatedAt": parse_timestamp(doc.get("updatedAt")),
        "fabricType": style.get("fabricType"),
        "reason": doc.get("reason"),
        "operation": doc.get("operation"),
        "productType": style.get("productType"),
        "productCategory": style.get("productCategory"),
        "status": doc.get("status"),
        "qcId": doc.get("qcId"),
        "count": doc.get("count"),
    }

def main():
    # Load reference collections once
    print("Fetching production lines...")
    production_lines = get_document_dict("productionLines")
    print("Fetching supervisors...")
    supervisors = get_document_dict("supportFunctions")
    print("Fetching styles...")
    # Note: styles key on styleNumber field, so we invert that mapping here
    styles_raw = db.collection("styles").stream()
    styles = {}
    for sdoc in styles_raw:
        sdata = sdoc.to_dict()
        key = sdata.get("styleNumber")
        if key:
            styles[key] = sdata

    # Fetch and enrich reworks
    print("Fetching reworks...")
    reworks_docs = db.collection("reworks").stream()
    reworks_list = []
    for rdoc in reworks_docs:
        data = rdoc.to_dict()
        enriched = enrich_and_format(data, production_lines, supervisors, styles)
        reworks_list.append(enriched)

    # Fetch and enrich rejects
    print("Fetching rejects...")
    rejects_docs = db.collection("rejects").stream()
    rejects_list = []
    for rdoc in rejects_docs:
        data = rdoc.to_dict()
        enriched = enrich_and_format(data, production_lines, supervisors, styles)
        rejects_list.append(enriched)

    # Dump JSON
    with open("reworks.json", "w") as f:
        json.dump(reworks_list, f, indent=2)
    with open("rejects.json", "w") as f:
        json.dump(rejects_list, f, indent=2)

    print(f"Exported {len(reworks_list)} reworks and {len(rejects_list)} rejects to JSON.")

if __name__ == "__main__":
    main()
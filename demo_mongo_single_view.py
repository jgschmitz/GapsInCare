import argparse
from pprint import pprint
from pymongo import MongoClient
from pymongo.errors import OperationFailure

def get_client(uri: str) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=15000)

def seed_and_create_view(admin_uri: str, db_name: str, base_coll: str, view_name: str):
    print(f"=== SETUP: seeding {db_name}.{base_coll} and creating secure view {db_name}.{view_name} ===")
    c = get_client(admin_uri)
    db = c[db_name]

    # Clean slate
    try: db[view_name].drop()
    except Exception: pass
    db[base_coll].drop()

    # Seed base collection (FULL data)
    docs = [
        {"memberId":"M001","LOB":"Medicaid",   "name":{"first":"Ava","last":"Nguyen"},  "dob":"1990-02-10T00:00:00Z","ssn":"111-22-3333","address":{"line1":"1 Main St","city":"Austin","state":"TX"}},
        {"memberId":"M002","LOB":"Commercial", "name":{"first":"Ben","last":"Sanchez"}, "dob":"1985-07-12T00:00:00Z","ssn":"222-33-4444","address":{"line1":"2 Pine Ave","city":"Chicago","state":"IL"}},
        {"memberId":"M003","LOB":"Medicare",   "name":{"first":"Chloe","last":"Patel"}, "dob":"1979-09-30T00:00:00Z","ssn":"333-44-5555","address":{"line1":"3 Oak Rd","city":"Denver","state":"CO"}},
    ]
    db[base_coll].insert_many(docs)

    # Helpful indexes
    db[base_coll].create_index([("memberId", 1)])
    db[base_coll].create_index([("LOB", 1), ("memberId", 1)])

    # Create SECURE VIEW (row + field restrictions)
    try:
        db.command({
            "create": view_name,
            "viewOn": base_coll,
            "pipeline": [
                {"$match": {"LOB": {"$ne": "Medicaid"}}},  # row-level
                {"$unset": ["ssn", "dob", "address"]}      # field-level
            ]
        })
    except OperationFailure as e:
        if e.code == 48:  # NamespaceExists
            db[view_name].drop()
            db.command({
                "create": view_name,
                "viewOn": base_coll,
                "pipeline": [
                    {"$match": {"LOB": {"$ne": "Medicaid"}}},
                    {"$unset": ["ssn", "dob", "address"]}
                ]
            })
        else:
            raise

    print(f"[setup] Inserted {db[base_coll].count_documents({})} docs into {db_name}.{base_coll}.")
    print(f"[setup] Created view {db_name}.{view_name} (no Medicaid; PHI removed).")

def demo_internal(alice_uri: str, db_name: str, base_coll: str):
    print("\n=== DEMO 1: Internal (Alice) sees ALL LOBs + PHI on BASE ===")
    c = get_client(alice_uri)
    coll = c[db_name][base_coll]
    docs = list(coll.find({}, {"_id": 0, "memberId": 1, "LOB": 1, "ssn": 1, "dob": 1}))
    pprint(docs)

def demo_offshore_denied_on_base_and_allowed_on_view(bob_uri: str, db_name: str, base_coll: str, view_name: str):
    print("\n=== DEMO 2: Offshore (Bob) DENIED on BASE, ALLOWED on VIEW (same DB) ===")
    c = get_client(bob_uri)
    base = c[db_name][base_coll]
    view = c[db_name][view_name]

    # Expect DENY on base collection
    try:
        base.find_one()
        print("WARNING: Offshore user could read BASE. In Atlas, remove broad roles (e.g., readAnyDatabase) and grant only 'find' on the VIEW.")
    except OperationFailure as e:
        if e.code == 13:
            print("✔ DENIED on base collection as expected (Unauthorized).")
        else:
            print(f"OperationFailure on base (code={e.code}): {e}")

    # ALLOW on secure view
    docs = list(view.find({}, {"_id": 0, "memberId": 1, "LOB": 1, "ssn": 1, "dob": 1, "address": 1}))
    pprint(docs)
    print(f"Medicaid via VIEW: {view.count_documents({'LOB': 'Medicaid'})} (expected 0)")

def main():
    ap = argparse.ArgumentParser(description="Single-DB secure-view demo (Atlas SRV URIs).")
    ap.add_argument("--db-name", default="gaps_demo", help="Target database name (case-sensitive). Example: Gaps_Demo")
    ap.add_argument("--base-coll", default="members", help="Base collection name.")
    ap.add_argument("--view-name", default="members_secure_v", help="Secure view name.")
    ap.add_argument("--admin-uri", required=False, help="SRV URI for maintenance user (setup).")
    ap.add_argument("--alice-uri", required=True, help="SRV URI for aliceInternal (read on base). &authSource=admin")
    ap.add_argument("--bob-uri",   required=True, help="SRV URI for bobOffshore (find on view only). &authSource=admin")
    ap.add_argument("--skip-setup", dest="skip_setup", action="store_true", help="Skip seeding and view creation.")
    args = ap.parse_args()

    DB = args.db_name
    BASE = args.base_coll
    VIEW = args.view_name

    if not args.skip_setup:
        if not args.admin_uri:
            ap.error("--admin-uri is required unless you pass --skip-setup")
        seed_and_create_view(args.admin_uri, DB, BASE, VIEW)

    demo_internal(args.alice_uri, DB, BASE)
    demo_offshore_denied_on_base_and_allowed_on_view(args.bob_uri, DB, BASE, VIEW)

if __name__ == "__main__":
    main()%                            

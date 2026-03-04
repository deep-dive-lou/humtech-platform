"""Create per-client Metabase collection hierarchy.

Additive only — nothing moved or deleted.

Usage:
    METABASE_API_KEY=mb_... python scripts/organize_collections.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import require_key, find_or_create_collection


def main():
    require_key()

    print("Organising Metabase collections...")
    clients_id = find_or_create_collection("Clients")
    resg_id = find_or_create_collection("RESG", parent_id=clients_id)

    print(f"\nDone!")
    print(f"  Clients collection: {clients_id}")
    print(f"  RESG collection:    {resg_id}")
    print(f"\nUse METABASE_COLLECTION_ID={resg_id} when running dashboard scripts.")


if __name__ == "__main__":
    main()

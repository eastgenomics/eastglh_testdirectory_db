"""
Syncs panel names and versions in the 'east-panels' table with the latest data 
from the PanelApp API. 

For each panel, it checks if the name or version has changed and updates the 
database accordingly.
Supports a dry-run mode for previewing changes without modifying the database.
Updates are wrapped in a transaction and will automatically roll back if any
update fails.
"""

import argparse
import psycopg2
import requests
import pandas as pd
from query_db import DB_CONFIG


def fetch_latest_signoff(panel_id: int) -> tuple:
    """
    Fetch the latest signed-off version for a given panel ID from the API.

    Args:
        panel_id (int): The panel ID to query.

    Returns:
        tuple: A tuple containing the name, version, and signed_off date.
    """
    url = (
        f"https://panelapp.genomicsengland.co.uk/api/v1/panels/signedoff/"
        f"?panel_id={panel_id}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["results"]:
            latest_result = data["results"][0]
            return (
                latest_result["name"],
                latest_result["version"],
                latest_result["signed_off"],
            )
    else:
        print(f"Error fetching {panel_id}, status code: {response.status_code}")
    return None, None, None


def update_panel_info(
    cursor: psycopg2.extensions.cursor,
    panel_id: int,
    current_name: str,
    current_version: str,
    new_name: str,
    new_version: str,
    dry_run: bool = True,
) -> None:
    """
    Update panel information in the database.

    Args:
        cursor (psycopg2.extensions.cursor): Database cursor object
        panel_id (int): Panel ID to update
        current_name (str): Current panel name in database
        current_version (str): Current panel version in database
        new_name (str): New panel name from API
        new_version (str): New panel version from API
        dry_run (bool): If True, only print changes (default)
    """
    updates = []
    changes = []

    if new_name and new_name != current_name:
        updates.append(f"\"panel-name\" = '{new_name}'")
        changes.append(f"name from '{current_name}' to '{new_name}'")

    if new_version and new_version != current_version:
        updates.append(f"\"panel-version\" = '{new_version}'")
        changes.append(f"version from {current_version} to {new_version}")

    if updates:
        update_query = f"""
        UPDATE testdirectory."east-panels"
        SET {', '.join(updates)}
        WHERE "panel-id" = '{panel_id}'
        """

        if dry_run:
            print(f"[DRY RUN] Would update panel {panel_id}: {', '.join(changes)}")
        else:
            try:
                cursor.execute(update_query)
                print(f"Updated panel {panel_id}: {', '.join(changes)}")
            except Exception as e:
                print(f"Error updating panel {panel_id}: {e}")
                raise


def parse_arguments():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(description="Sync PanelApp info with EGLH DB.")
    parser.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="Run without modifying the database (default is dry-run mode)",
    )
    return parser.parse_args()


def main():
    """Entry point"""
    args = parse_arguments()
    # Connect to the database and fetch panel IDs
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                print("Connected to the database successfully.")
                cursor.execute(
                    """
                    SELECT "panel-id", "panel-name", "panel-version"
                    FROM testdirectory."east-panels"
                    WHERE "panel-type-id" = 1
                """
                )
                panel_data = cursor.fetchall()

                # Fetch latest signoff data for each panel ID
                for panel_id, current_name, current_version in panel_data:
                    new_name, new_version, _ = fetch_latest_signoff(panel_id)

                    if new_name or new_version:
                        update_panel_info(
                            cursor,
                            panel_id,
                            current_name,
                            current_version,
                            new_name,
                            new_version,
                            args.dry_run,
                        )

                if not args.dry_run:
                    conn.commit()
                    print("Changes committed to the database.")
                else:
                    print("\n[DRY RUN] No changes committed.")

    except Exception as e:
        print(f"Database operation failed: {e}")
        if not args.dry_run and "conn" in locals():
            conn.rollback()
            print("Changes rolled back due to error.")


if __name__ == "__main__":
    main()

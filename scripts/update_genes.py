"""
Updates ngtd db with high-confidence genes from PanelApp.

For each panel in the 'east-panels' table (type 1), this script:
1. Retrieves high-confidence genes from the PanelApp API.
2. Compares them with existing entries in 'east-genes'.
3. Inserts new genes or removes obsolete ones.
4. Logs all changes.

Supports dry-run mode to preview changes without writing to the database.
"""

import argparse
import requests
import psycopg2
from query_db import DB_CONFIG


def parse_arguments():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(description="Sync PanelApp genes with EGLH DB.")
    parser.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="Run without modifying the database (default is dry-run mode)",
    )
    return parser.parse_args()


def get_high_confidence_genes(panel_id: int, version: str) -> list:
    """Fetch high-confidence genes from the PanelApp API

    Args:
        panel_id (int): PanelApp's ID of panel
        version (str): Latest Signoff Version of the panel.

    Returns:
        list[str]: List of hgnc ids
    """

    url = (
        f"https://panelapp.genomicsengland.co.uk/api/v1/panels/{panel_id}/"
        f"?version={version}"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        panel_data = response.json()

        high_confidence_genes = [
            gene["gene_data"]["hgnc_id"]
            for gene in panel_data["genes"]
            if (
                gene.get("confidence_level") == "3" 
                and "gene_data" in gene 
                and "hgnc_id" in gene["gene_data"]
            )
        ]
        return high_confidence_genes
    except Exception as e:
        print(f"Error fetching data for panel {panel_id}: {e}")
        return []


def get_existing_genes_for_panel(
    east_panel_id: int, cursor: psycopg2.extensions.cursor
) -> set:
    """Get existing genes for a panel from the database.

    Args:
        east_panel_id (int): primary key of panel in "east-panels" table
        cursor (psycopg2.extensions.cursor): A database cursor object

    Returns:
        set[str]: Set of existing hgnc ids for the panel
    """
    try:
        cursor.execute(
            """
            SELECT "hgnc-id" 
            FROM "testdirectory"."east-genes" 
            WHERE "east-panel-id" = %s
            """,
            (east_panel_id,),
        )
        existing_genes = {row[0] for row in cursor.fetchall()}
        print(f"Found {len(existing_genes)} genes for panel {east_panel_id} in db")
        return existing_genes
    except Exception as e:
        print(f"Error fetching genes for panel {east_panel_id}: {e}")
        return set()


def add_genes_to_panel(
    east_panel_id: int,
    genes_to_add: set,
    cursor: psycopg2.extensions.cursor,
    dry_run: bool,
) -> None:
    """Insert new genes for a panel into the database.

    Args:
        east_panel_id (int): Panel identifier.
        genes_to_add (set): Genes to insert.
        cursor (psycopg2.extensions.cursor): Database cursor object.
        dry_run (bool): If True, simulate only.
    """
    for hgnc_id in genes_to_add:
        try:
            if dry_run:
                print(f"[DRY-RUN ADD] Panel {east_panel_id}: Would add gene {hgnc_id}")
            else:
                cursor.execute(
                    """
                    INSERT INTO "testdirectory"."east-genes" ("east-panel-id", "hgnc-id")
                    VALUES (%s, %s)
                """,
                    (east_panel_id, hgnc_id),
                )
                print(f"[ADD] Panel {east_panel_id}: Added gene {hgnc_id}")

        except psycopg2.IntegrityError:
            print(
                f"[SKIP DUPLICATE] Panel {east_panel_id}: Gene {hgnc_id} already exists"
            )
        except Exception as e:
            print(f"Error adding gene {hgnc_id} to panel {east_panel_id}: {e}")


def remove_genes_from_panel(
    east_panel_id: int,
    genes_to_remove: set,
    cursor: psycopg2.extensions.cursor,
    dry_run: bool,
) -> None:
    """Remove genes that are no longer part of the panel.

    Args:
        east_panel_id (int): Panel identifier.
        genes_to_remove (set): Genes to remove.
        cursor (psycopg2.extensions.cursor): Database cursor object.
        dry_run (bool): If True, simulate only.
    """
    for hgnc_id in genes_to_remove:
        try:
            if dry_run:
                print(
                    f"[DRY-RUN REMOVE] Panel {east_panel_id}: Would remove gene {hgnc_id}"
                )
            else:
                cursor.execute(
                    """
                    DELETE FROM "testdirectory"."east-genes"
                    WHERE "east-panel-id" = %s AND "hgnc-id" = %s
                """,
                    (east_panel_id, hgnc_id),
                )
                if cursor.rowcount > 0:
                    print(f"[REMOVE] Panel {east_panel_id}: Removed gene {hgnc_id}")
                else:
                    print(
                        f"[SKIP REMOVE] Panel {east_panel_id}: Gene {hgnc_id} not found in DB"
                    )
        except Exception as e:
            print(f"Error removing gene {hgnc_id} from panel {east_panel_id}: {e}")


def update_db_genes(
    east_panel_id: int,
    hgnc_ids: list[str],
    cursor: psycopg2.extensions.cursor,
    dry_run: bool = True,
) -> None:
    """Insert high-confidence genes for a panel into the database.

    Args:
        east_panel_id (int): primary key of panel in "east-panels" table
        hgnc_ids (List[str]): list of hgnc ids from panelapp
        cursor (psycopg2.extensions.cursor): A database cursor object used to execute
        SQL queries.
        dry_run (bool): If True, simulate only.
    """
    try:
        if not dry_run:
            cursor.execute(f"SAVEPOINT panel_{east_panel_id}")

        db_genes = get_existing_genes_for_panel(east_panel_id, cursor)
        panelapp_genes = set(hgnc_ids)

        genes_to_add = panelapp_genes - db_genes
        genes_to_remove = db_genes - panelapp_genes

        if not genes_to_add and not genes_to_remove:
            print(f"[NO CHANGE] Panel {east_panel_id}: Genes are up to date.")
            return

        if genes_to_add:
            add_genes_to_panel(east_panel_id, genes_to_add, cursor, dry_run)
        if genes_to_remove:
            remove_genes_from_panel(east_panel_id, genes_to_remove, cursor, dry_run)

    except Exception as e:
        print(f"[ERROR] Panel {east_panel_id}: Rolling back changes due to error: {e}")
        if not dry_run:
            cursor.execute(f"ROLLBACK TO SAVEPOINT panel_{east_panel_id}")


def main():
    """Entry point"""
    args = parse_arguments()

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                print("Connected to the database successfully.")

                # Select only PanelApp panels (panel-type-id = 1) for update
                cursor.execute(
                    """
                    SELECT "id", "panel-id", "panel-version"
                    FROM testdirectory."east-panels"
                    WHERE "panel-type-id" = 1
                """
                )

                panel_data = cursor.fetchall()

                # fetchall() returns a list of tuples,
                for east_panel_id, panel_id, version in panel_data:
                    print(f"\nProcessing panel {panel_id}...")

                    hgnc_ids = get_high_confidence_genes(panel_id, version)

                    if hgnc_ids:
                        update_db_genes(east_panel_id, hgnc_ids, cursor, args.dry_run)

                if not args.dry_run:
                    conn.commit()
                    print("Changes committed to the database.")
                else:
                    print("\n[DRY RUN] No changes committed.")

    except Exception as e:
        print(f"Error connecting to database: {e}")


if __name__ == "__main__":
    main()

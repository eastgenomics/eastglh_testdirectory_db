## Database specification for Test Directory

### Original request
Validate the version of TD database hosted on AWS RDS service against the Internal East-GLH Rare and Inherited Disease Test Directory v7 spreadsheet

Add genes data into TD database

### Scripts
1. `query_db.py`: 
Fetches panel and gene data from the Test Directory database and saves it to a CSV file.

2. `parse_east_glh_td_spreadsheet.py`:
Parses the East-GLH Test Directory spreadsheet, retrieves panel info from the PanelApp API, and formats the data for comparison.
*Requires the spreadsheet file as input (-i or --internal_td_spreadsheet).*

3. `compare_dfs.py`:
Compares the data from the spreadsheet and database, identifying mismatches

4. `update_panels_info.py`:
Update panels info to latest sign-off versions from PanelApp. Supports dry-run mode (default) for preview without applying changes

5. `update_genes.py`:
Updates the table, `east-genes` with high-confidence gene data from PanelApp. Supports dry-run mode (default) for preview without applying changes

6. `validate_east_genes_table.py`:
Verifies that `east-genes` table has been populated

7. `generate_genepanels.py`:
Generates a new genepanels file by querying all panels and genes in new ngtd database

8. `compare_genepanels.py`:
Compares the new genepanels with the old (prod) one and summarises any diff to a spreadsheet

9. `check_gene_to_transcript.py`:
Check that all genes in new genepanels are mapped to a clinical transcript in prod g2t file.

### How to Run
1. Set up credentials: Create a `.env` file or set the following environment variables:

```
DB_ENDPOINT=database_host
DB_PORT=database_port
DB_USERNAME=your_database_username
DB_PASSWORD=your_database_password
DB_NAME=database_name
```

2. Install dependencies:
`pip install -r requirements.txt`

3. Run scripts in following order to validate panels info in ngtd database:
- `python query_db.py`
- `python parse_east_glh_td_spreadsheet.py -i path/to/spreadsheet`
- `python compare_dfs.py`

4. To update panels and genes with latest sign-offs from PanelApp:
- `python update_panels_info.py --no-dry-run`
- `python update_genes.py --no-dry-run`
- `python validate_east_genes_table.py`

5. To generate new genepanels and compare diff with current (prod) genepanels:
- `python generate_genepanels.py`
- `python compare_genepanels.py --new_file_id <file_id> --old_file_id <file_id>`

6. To check that genepanels is compactible to prod g2t file:
- `python check_gene_to_transcript.py --genepanels <file_id> --g2t <file_id>`


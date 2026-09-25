# P2K

A hobby observatory for the Dutch P2000 / FLEX pager network.

Captures passively, stores raw evidence immutably, and builds a relational view of who gets paged, when, and where.  

## Pipeline

`rtl_fm -> multimon-ng -> srcctl.py -> messages.db -> models.py -> stats`

**srcctl.py**  
Stage 0 collector. Reads multimon-ng output from stdin, appends lines verbatim to `raw_flex`, no parsing, no filtering.

**model.py**  
Stage 1 ETL. Loads `capcodes.csv` into `ontology`, explodes `raw_flex` into per-capcode `pages`, 
and exposes a live `phenomenology` view joining the two.  
`capcodes.csv`, hand-maintained registry of capcodes (headerless: `capcode,entity,region,city,role,`).  

## Design

Two datasets, one join.

- **Ontology**, who exists: capcode -> entity, region, city, role.
- **Phenomenology**, what was actually paged, when.

`raw_flex` is append-only and immutable. `pages` stores observed events only. 
Enrichment happens via a SQL view, so editing `capcodes.csv` later re-enriches all history without reprocessing raw lines.  

## Usage

Collect:

`rtl_fm -f 169650000hz -s 22050 | multimon-ng -a FLEX -f auto -t raw - | python3 srcctl.py`

Build and inspect:  
```
    ptyhon models.py build-ontology capcodes.csv
    ptyhon models.py build-phenomenology
    ptyhon models.py stats
```

Reprocess a range:  

`python model.py rebuild-phenomenology --from <raw_flex_id>`

## Requirements

- `rtl_fm` (rtl-sdr)
- `multimon-ng`
- Python 3.10+
- SQLite 3 (WAL mode)

## Scope

Hobby project. Passive receive only. `raw_flex` is the source of truth;
everything else is derived and rebuildable.

## Legal Note

*P2000 traffic can contain personal data, do not publish raw messages or addresses, Keep the DB local.*

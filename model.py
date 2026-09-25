#!/usr/bin/env python3
"""
    models.py

    Second-stage ETL for srcctl.py captures.

    Ontology:       who exists, capcode -> entity/region/city/role
                    (hand-maintained in capcodes.csv, loaded here)
    Phenomenology:  what was actually paged, when, exploded from
                    raw_flex (which may carry multiple capcodes per
                    line for group/batch calls) into one row per
                    (page event, capcode).

    Design note:
        * `pages` stores only the observed event + capcode.
        * Enrichment (entity/region/city/role) happens via a SQL VIEW joined
          against `ontology` at read time,
          so editing capcodes.csv later re-enriches all historical data without reprocessing raw_flex.

    CSV format (headerless, 6 fields, trailing field usually empty):
        000100000,Brandweer,Amsterdam-Amstelland,,Proefalarm,
        ^capcode  ^entity  ^region              ^city ^role

    Usage:
        model.py build-ontology capcodes.csv
        model.py build-phenomenology
        model.py rebuild-phenomenology --from ID
        model.py stats
"""
import sys
import csv
import re
import sqlite3
import logging
from datetime import datetime, timezone
from collections import Counter

DB_PATH = 'messages.db'
CAP_SPLIT = re.compile(r'[\s,;]+')

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                     format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('models')


SCHEMA = """
    CREATE TABLE IF NOT EXISTS ontology (
        capcode     TEXT PRIMARY KEY,
        entity      TEXT,
        region      TEXT,
        city        TEXT,
        role        TEXT,
        updated_at  TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS pages (
        id            INTEGER PRIMARY KEY,
        raw_flex_id   INTEGER NOT NULL,
        capcode       TEXT NOT NULL,
        received_at   TEXT NOT NULL,
        message_type  TEXT,
        message_text  TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_pages_capcode ON pages(capcode);
    CREATE INDEX IF NOT EXISTS idx_pages_received_at ON pages(received_at);
    CREATE INDEX IF NOT EXISTS idx_pages_raw_flex_id ON pages(raw_flex_id);

    CREATE TABLE IF NOT EXISTS etl_state (
        key    TEXT PRIMARY KEY,
        value  TEXT
    );

    -- Phenomenology enriched with Ontology, computed live, always fresh.
    DROP VIEW IF EXISTS phenomenology;
    CREATE VIEW phenomenology AS
        SELECT
            p.id, p.raw_flex_id, p.capcode, p.received_at,
            p.message_type, p.message_text,
            o.entity, o.region, o.city, o.role
        FROM pages p
        LEFT JOIN ontology o ON o.capcode = p.capcode;
"""


def get_db(path=DB_PATH):
    db = sqlite3.connect(path, isolation_level=None)
    db.executescript(SCHEMA)
    return db


def get_state(db, key, default=None):
    row = db.execute('SELECT value FROM etl_state WHERE key=?', (key,)).fetchone()
    return row[0] if row else default


def set_state(db, key, value):
    db.execute(
        'INSERT INTO etl_state(key, value) VALUES (?,?) '
        'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
        (key, str(value)),
    )


def norm_capcode(cap):
    """Canonical capcode form: digits only, zero-padded to 9."""
    if cap is None:
        return None
    cap = str(cap).strip()
    if not cap:
        return None
    # keep only leading-digit run (defends against stray punctuation)
    m = re.match(r'^(\d+)', cap)
    if not m:
        return None
    return m.group(1).zfill(9)


# ---------------------------------------------------------------- ontology

def build_ontology(db, csv_path):
    """Load/refresh the capcode registry from a headerless CSV.

    Column order in the file (0-indexed):
        0 capcode   1 entity   2 region   3 city   4 role   5 (ignored)
    """
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    n = 0
    skipped = 0

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = []
        for lineno, row in enumerate(reader, start=1):
            if not row or all(not c.strip() for c in row):
                continue
            if len(row) < 5:
                skipped += 1
                log.warning('ontology: line %d has %d field(s), skipped', lineno, len(row))
                continue

            capcode = norm_capcode(row[0])
            if not capcode:
                skipped += 1
                log.warning('ontology: line %d has no usable capcode, skipped', lineno)
                continue

            def field(i):
                v = row[i].strip() if i < len(row) else ''
                return v or None

            rows.append((
                capcode,
                field(1),  # entity
                field(2),  # region
                field(3),  # city
                field(4),  # role
                now,
            ))
            n += 1

    db.execute('BEGIN')
    db.executemany(
        'INSERT INTO ontology(capcode, entity, region, city, role, updated_at) '
        'VALUES (?,?,?,?,?,?) '
        'ON CONFLICT(capcode) DO UPDATE SET '
        '  entity=excluded.entity, region=excluded.region, city=excluded.city, '
        '  role=excluded.role, updated_at=excluded.updated_at',
        rows,
    )
    db.execute('COMMIT')

    log.info('Ontology: upserted %d capcode(s) from %s (%d skipped)',
             n, csv_path, skipped)


# ----------------------------------------------------------- phenomenology

def build_phenomenology(db, batch_limit=5000):
    """Explode new raw_flex rows into individual (event, capcode) pages."""
    last_id = int(get_state(db, 'last_raw_flex_id', 0) or 0)

    rows = db.execute(
        'SELECT id, received_at, capcodes, message_type, message_text '
        'FROM raw_flex WHERE id > ? ORDER BY id LIMIT ?',
        (last_id, batch_limit),
    ).fetchall()

    if not rows:
        log.info('Phenomenology: nothing new past raw_flex.id=%d', last_id)
        return 0

    inserts = []
    max_id = last_id
    for rid, received_at, capcodes, mtype, mtext in rows:
        max_id = max(max_id, rid)
        if not capcodes:
            continue
        for raw_cap in CAP_SPLIT.split(capcodes.strip()):
            cap = norm_capcode(raw_cap)
            if cap:
                inserts.append((rid, cap, received_at, mtype, mtext))

    db.execute('BEGIN')
    if inserts:
        db.executemany(
            'INSERT INTO pages(raw_flex_id, capcode, received_at, message_type, message_text) '
            'VALUES (?,?,?,?,?)',
            inserts,
        )
    set_state(db, 'last_raw_flex_id', max_id)
    db.execute('COMMIT')

    log.info('Phenomenology: exploded %d raw_flex row(s) -> %d page event(s), advanced to id=%d',
             len(rows), len(inserts), max_id)
    return len(inserts)


def rebuild_phenomenology(db, from_id):
    """Drop pages for raw_flex_id >= from_id, rewind etl_state, and rebuild."""
    db.execute('BEGIN')
    db.execute('DELETE FROM pages WHERE raw_flex_id >= ?', (from_id,))
    set_state(db, 'last_raw_flex_id', max(0, from_id - 1))
    db.execute('COMMIT')
    log.info('Phenomenology: rewound to raw_flex.id=%d', from_id - 1)
    return build_phenomenology(db)


# ------------------------------------------------------------------ stats

def _bar(value, peak, width=40):
    if not peak:
        return ''
    return '#' * int(value / peak * width)


def print_stats(db, top_n=10):
    total = db.execute('SELECT COUNT(*) FROM pages').fetchone()[0]
    if not total:
        print('No page events yet. Run: python3 models.py build-phenomenology')
        return

    unique_caps = db.execute('SELECT COUNT(DISTINCT capcode) FROM pages').fetchone()[0]
    known = db.execute(
        'SELECT COUNT(DISTINCT p.capcode) FROM pages p '
        'JOIN ontology o ON o.capcode = p.capcode'
    ).fetchone()[0]

    print(f'Total page events    : {total}')
    print(f'Unique capcodes seen : {unique_caps}')
    print(f'Known (in ontology)  : {known} ({(known / unique_caps * 100 if unique_caps else 0):.1f}%)')
    print()

    # ---- top busiest capcodes
    print(f'Top {top_n} busiest capcodes:')
    for cap, entity, region, role, cnt in db.execute(
        'SELECT p.capcode, o.entity, o.region, o.role, COUNT(*) c '
        'FROM pages p LEFT JOIN ontology o ON o.capcode = p.capcode '
        'GROUP BY p.capcode ORDER BY c DESC LIMIT ?', (top_n,)
    ):
        label = ' / '.join(x for x in (entity, region, role) if x) or '(unknown)'
        print(f'  {cap:>10}  {cnt:>6}  {label}')
    print()

    # ---- top unknown capcodes (work queue for capcodes.csv)
    unknowns = db.execute(
        'SELECT p.capcode, COUNT(*) c '
        'FROM pages p LEFT JOIN ontology o ON o.capcode = p.capcode '
        'WHERE o.capcode IS NULL '
        'GROUP BY p.capcode ORDER BY c DESC LIMIT ?', (top_n,)
    ).fetchall()
    if unknowns:
        print(f'Top {top_n} unknown capcodes (not in ontology):')
        for cap, cnt in unknowns:
            print(f'  {cap:>10}  {cnt:>6}')
        print()

    # ---- dormant ontology entries (registered, never paged)
    dormant = db.execute(
        'SELECT o.capcode, o.entity, o.role '
        'FROM ontology o LEFT JOIN pages p ON p.capcode = o.capcode '
        'WHERE p.capcode IS NULL LIMIT ?', (top_n,)
    ).fetchall()
    if dormant:
        print(f'Sample of dormant capcodes (in ontology, never paged):')
        for cap, entity, role in dormant:
            label = ' / '.join(x for x in (entity, role) if x) or '(unnamed)'
            print(f'  {cap:>10}  {label}')
        print()

    # ---- activity by hour
    hour_counts = Counter()
    for (received_at,) in db.execute('SELECT received_at FROM pages'):
        try:
            hour_counts[datetime.fromisoformat(received_at).hour] += 1
        except ValueError:
            continue
    if hour_counts:
        peak = max(hour_counts.values())
        print('Activity by hour (UTC):')
        for h in range(24):
            c = hour_counts.get(h, 0)
            print(f'  {h:02d}:00  {c:>6}  {_bar(c, peak)}')
        print()

    # ---- region distribution (relational)
    regions = db.execute(
        'SELECT COALESCE(o.region, "(unknown)") r, COUNT(*) c '
        'FROM pages p LEFT JOIN ontology o ON o.capcode = p.capcode '
        'GROUP BY r ORDER BY c DESC LIMIT ?', (top_n,)
    ).fetchall()
    if regions:
        peak = regions[0][1] if regions else 0
        print(f'Top {top_n} regions by activity:')
        for region, c in regions:
            print(f'  {region:<28}  {c:>6}  {_bar(c, peak, 30)}')
        print()

    # ---- co-occurrence: capcodes paged together in the same raw message
    cooc = db.execute(
        'SELECT a.capcode, b.capcode, COUNT(*) c '
        'FROM pages a JOIN pages b '
        '  ON a.raw_flex_id = b.raw_flex_id AND a.capcode < b.capcode '
        'GROUP BY a.capcode, b.capcode ORDER BY c DESC LIMIT ?', (top_n,)
    ).fetchall()
    if cooc:
        print(f'Top {top_n} capcode co-occurrences (paged together):')
        for a, b, c in cooc:
            print(f'  {a} + {b}   x{c}')
        print()


# ------------------------------------------------------------------- main

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    db = get_db()

    if cmd == 'build-ontology':
        if len(sys.argv) < 3:
            log.error('Usage: models.py build-ontology <capcodes.csv>')
            sys.exit(1)
        build_ontology(db, sys.argv[2])

    elif cmd == 'build-phenomenology':
        build_phenomenology(db)

    elif cmd == 'rebuild-phenomenology':
        if '--from' not in sys.argv:
            log.error('Usage: models.py rebuild-phenomenology --from <raw_flex_id>')
            sys.exit(1)
        from_id = int(sys.argv[sys.argv.index('--from') + 1])
        rebuild_phenomenology(db, from_id)

    elif cmd == 'stats':
        print_stats(db)

    else:
        print(__doc__)
        sys.exit(1)

    db.close()


if __name__ == '__main__':
    main()

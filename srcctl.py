#!/usr/bin/env python3
"""
    srcctl.py

        Purpose:
            Provide source control and structured pipe-delimited parsing in sqlite3 format
            [rtl_fm -> multimon-ng] -> [deconstruct] -> [sqlite3] -> [analyzers]

        Analyzers:
            * Ontology: who exists, what role, what region, what city (capcodes.csv)
            * Phenomenology: who was actually paged, when, with what (message.db)

        Usage:
            rtl_fm -f 169650000hz -s 22050 | multimon-ng -a FLEX -f auto -t raw - | python3 ./srcctl.py


"""
import sys
import os
import time
import signal
import sqlite3
import logging
from datetime import datetime, timezone

# settings
DB_PATH = 'messages.db'
COLLECTOR_VERSION = 'srcctl'
BATCH_MAX = 20
FLUSH_INTERVAL = 1.0 # Flush every 1 second or when batch hits max

# logging layer
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger('scanner')

# sqlite template mapping each pipe-delimited field
SCHEMA = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA busy_timeout=5000;

    CREATE TABLE IF NOT EXISTS raw_flex (
        id                 INTEGER PRIMARY KEY,
        received_at        TEXT NOT NULL,
        received_mono      INTEGER NOT NULL,
        protocol           TEXT,
        multimon_timestamp TEXT,
        properties         TEXT,
        delta              TEXT,
        capcodes           TEXT,
        message_type       TEXT,
        message_text       TEXT,
        raw_line           BLOB NOT NULL,
        collector_version  TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_raw_received_at ON raw_flex(received_at);
    CREATE INDEX IF NOT EXISTS idx_capcodes ON raw_flex(capcodes);
"""

def open_db(path):
    db = sqlite3.connect(path, isolation_level=None)
    db.executescript(SCHEMA)
    return db

def parse_pipe_line(line_bytes):
    """Safely splits multimon-ng pipe output into individual fields."""
    try:
        text = line_bytes.decode('utf-8', errors='ignore')
        # maxsplit=6 ensures message_text remains intact even if it contains a pipe character
        parts = text.split('|', 6)
        if len(parts) >= 7:
            return {
                'protocol': parts[0],
                'multimon_timestamp': parts[1],
                'properties': parts[2],
                'delta': parts[3],
                'capcodes': parts[4],
                'message_type': parts[5],
                'message_text': parts[6]
            }
    except Exception:
        pass

    # Fallback for malformed lines or bit-error distortions
    return {
        'protocol': None, 'multimon_timestamp': None, 'properties': None,
        'delta': None, 'capcodes': None, 'message_type': None, 'message_text': None
    }

def main():
    db = open_db(DB_PATH)
    log.info('Scanning... (output=%s)', DB_PATH)

    batch = []
    last_flush = time.monotonic()

    def flush():
        nonlocal last_flush
        if not batch:
            return
        try:
            db.execute('BEGIN')
            db.executemany(
                'INSERT INTO raw_flex('
                'received_at, received_mono, protocol, multimon_timestamp, '
                'properties, delta, capcodes, message_type, message_text, raw_line, collector_version'
                ') VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                batch,
            )
            db.execute('COMMIT')
            batch.clear()
        except sqlite3.Error as e:
            log.error('Database write error: %s', e)
            try:
                db.execute('ROLLBACK')
            except Exception:
                pass
        finally:
            last_flush = time.monotonic()

    def shutdown(*_):
        flush()
        db.close()
        log.info('Stopped')
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        for raw in sys.stdin.buffer:
            line = raw.rstrip(b'\r\n')
            if not line:
                continue

            parsed = parse_pipe_line(line)
            if not (parsed['capcodes'] is None):
                log.info('Received: %s | %i', parsed['capcodes'], len(parsed['message_text']))

            batch.append((
                datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                time.monotonic_ns(),
                parsed['protocol'],
                parsed['multimon_timestamp'],
                parsed['properties'],
                parsed['delta'],
                parsed['capcodes'],
                parsed['message_type'],
                parsed['message_text'],
                line,
                COLLECTOR_VERSION,
            ))

            now = time.monotonic()
            if len(batch) >= BATCH_MAX or (batch and (now - last_flush) >= FLUSH_INTERVAL):
                flush()

    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    finally:
        shutdown()

if __name__ == '__main__':
    main()

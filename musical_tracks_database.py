import csv
import sqlite3

# Connect to the database (or create it if it doesn't exist)
conn = sqlite3.connect('trackdb.sqlite')
cur = conn.cursor()

# Create tables using executescript.
# This also handles dropping tables if they exist, to allow for rerunning the script.
cur.executescript('''
DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS Track;

CREATE TABLE Artist (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE Genre (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE Album (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    artist_id  INTEGER,
    title   TEXT UNIQUE
);

CREATE TABLE Track (
    id  INTEGER NOT NULL PRIMARY KEY 
        AUTOINCREMENT UNIQUE,
    title TEXT  UNIQUE,
    album_id  INTEGER,
    genre_id  INTEGER,
    len INTEGER, rating INTEGER, count INTEGER
);
''')

# The CSV file to be used for the assignment
fname = 'tracks.csv'

# Caches to store foreign keys and avoid repeated SELECT queries
artist_ids = dict()
genre_ids = dict()
album_ids = dict()

try:
    with open(fname, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        # The CSV file from the zip does not have a header row.
        # Each row contains: Track,Artist,Album,Plays,Rating,Genre,Time
        for row in csvreader:
            if not row: continue # Skip empty rows

            track_title, artist_name, album_title, count, rating, genre_name, length = row

            # Get artist_id, using cache if possible
            if artist_name not in artist_ids:
                cur.execute('INSERT OR IGNORE INTO Artist (name) VALUES (?)', (artist_name, ))
                cur.execute('SELECT id FROM Artist WHERE name = ?', (artist_name, ))
                artist_ids[artist_name] = cur.fetchone()[0]
            artist_id = artist_ids[artist_name]

            # Get genre_id, using cache if possible
            if genre_name not in genre_ids:
                cur.execute('INSERT OR IGNORE INTO Genre (name) VALUES (?)', (genre_name, ))
                cur.execute('SELECT id FROM Genre WHERE name = ?', (genre_name, ))
                genre_ids[genre_name] = cur.fetchone()[0]
            genre_id = genre_ids[genre_name]

            # Get album_id, using cache if possible
            # Note: The schema has a UNIQUE constraint on Album.title, so we use that as the key.
            if album_title not in album_ids:
                cur.execute('INSERT OR IGNORE INTO Album (title, artist_id) VALUES (?, ?)', (album_title, artist_id))
                cur.execute('SELECT id FROM Album WHERE title = ?', (album_title, ))
                album_ids[album_title] = cur.fetchone()[0]
            album_id = album_ids[album_title]

            # Insert or replace track
            cur.execute('''INSERT OR IGNORE INTO Track
                (title, album_id, genre_id, len, rating, count) 
                VALUES ( ?, ?, ?, ?, ?, ? )''', 
                ( track_title, album_id, genre_id, length, rating, count ) )

    # Commit the changes to the database
    conn.commit()
    print("Database 'trackdb.sqlite' created and populated successfully.")

except FileNotFoundError:
    print(f"Error: Could not find '{fname}'. Please download it from http://www.py4e.com/code3/tracks.zip and place it in the same directory as this script.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    cur.close()
    conn.close()
"""
Transforms and Loads data into the local SQLite3 database
"""

import sqlite3
import csv


# load the csv file and insert into a new sqlite3 database
def load(dataset="data/gradstudents.csv"):
    """Transforms and Loads data into the local SQLite3 database"""
    payload = csv.reader(open(dataset, newline=""), delimiter=",")
    # skips the header of csv
    header = next(payload)
    major_idx = header.index("Major")
    major_category_idx = header.index("Major_category")
    grad_total_idx = header.index("Grad_total")
    grad_employed_idx = header.index("Grad_employed")

    conn = sqlite3.connect("gradstudentsDB.db")
    c = conn.cursor()
    c.execute(
        "SELECT Major, Major_category, Grad_total, Grad_employed FROM gradstudentsDB"
    )
    c.execute("DROP TABLE IF EXISTS gradstudentsDB")
    c.execute(
        """
        CREATE TABLE gradstudentsDB (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Major TEXT,
            Major_category TEXT,
            Grad_total INTEGER,
            Grad_employed INTEGER
        )
    """
    )
    relevant_data = [
        (
            row[major_idx],
            row[major_category_idx],
            row[grad_total_idx],
            row[grad_employed_idx],
        )
        for row in payload
    ]
    # insert
    c.executemany(
        """
        INSERT INTO gradstudentsDB (
            Major, 
            Major_category, 
            Grad_total, Grad_employed
            ) 
            VALUES (?, ?, ?, ?)""",
        relevant_data,
    )
    conn.commit()
    conn.close()
    return "gradstudentsDB.db"

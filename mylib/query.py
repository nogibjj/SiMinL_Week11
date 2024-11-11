"""Query the database"""

import sqlite3

# Define a global variable for the log file
LOG_FILE = "query_log.md"


def log_query(query):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")


def general_query(query):
    """runs a query a user inputs"""
    # Connect to the SQLite database
    conn = sqlite3.connect("gradstudentsDB.db")

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Execute the query
    cursor.execute(query)
    # checks if the query is an insert, update or delete operation
    # If the query modifies the database, commit the changes
    if (
        query.strip().lower().startswith("insert")
        or query.strip().lower().startswith("update")
        or query.strip().lower().startswith("delete")
    ):
        conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    log_query(f"{query}")


def create_record(Major, Major_category, Grad_total, Grad_employed):
    """create example query"""
    # ?s are placeholders
    conn = sqlite3.connect("gradstudentsDB.db")
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO gradstudentsDB 
        (Major, Major_category, Grad_total, Grad_employed) 
        VALUES (?, ?, ?, ?)
        """,
        (Major, Major_category, Grad_total, Grad_employed),
    )
    conn.commit()
    conn.close()

    # Log the query
    log_query(
        f"""INSERT INTO gradstudentsDB VALUES (
            {Major}, 
            {Major_category}, 
            {Grad_total}, 
            {Grad_employed},);"""
    )


def update_record(id, Major, Major_category, Grad_total, Grad_employed):
    """update example query"""
    conn = sqlite3.connect("gradstudentsDB.db")
    c = conn.cursor()
    c.execute(
        """
        UPDATE gradstudentsDB 
        SET Major=?, 
        Major_category=?, 
        Grad_total=?, Grad_employed=? 
        WHERE id=?
        """,
        (Major, Major_category, Grad_total, Grad_employed, id),
    )
    conn.commit()
    conn.close()

    # Log the query
    log_query(
        f"""UPDATE gradstudentsDB SET 
        major={Major}, 
        major_category=
        {Major_category},
        grad_total={Grad_total}, grad_employed={Grad_employed}
        WHERE id={id};"""
    )


def delete_record(id):
    """delete example query"""
    conn = sqlite3.connect("gradstudentsDB.db")
    c = conn.cursor()
    c.execute("DELETE FROM gradstudentsDB WHERE id=?", (id,))
    conn.commit()
    conn.close()

    # Log the query
    log_query(f"DELETE FROM gradstudentsDB WHERE id={id};")


def read_data():
    """read data. retrieves all records"""
    conn = sqlite3.connect("gradstudentsDB.db")
    c = conn.cursor()
    c.execute("SELECT * FROM gradstudentsDB")
    data = c.fetchall()
    log_query("SELECT * FROM gradstudentsDB;")
    return data

import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash

DB_FILE = "blast_app.db"

def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    
    # Users table with is_admin column
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        is_admin INTEGER DEFAULT 0
    )
    """)
    
    # Jobs table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        user_id INTEGER,
        organism TEXT,
        progress INTEGER DEFAULT 0,
        status TEXT DEFAULT 'RUNNING',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """)
    
    # Results table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        accession TEXT,
        top_hit TEXT,
        gene TEXT,
        species TEXT,
        bit_score TEXT,
        evalue TEXT,
        FOREIGN KEY(job_id) REFERENCES jobs(id)
    )
    """)
    
    conn.commit()
    conn.close()

# --------------------
# User helpers
# --------------------
def create_user(username, password, is_admin=0):
    conn = get_conn()
    cur = conn.cursor()
    password_hash = generate_password_hash(password)
    try:
        cur.execute("INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)",
                    (username, password_hash, is_admin))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return False
    conn.close()
    return True

def create_admin():
    """Create admin account if it doesn't exist (username=admin, password=admin)"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username='admin'")
    if not cur.fetchone():
        create_user("admin", "admin", is_admin=1)
    conn.close()

def verify_user(username, password):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    if row and check_password_hash(row["password_hash"], password):
        return row["id"], row["is_admin"]
    return None, 0

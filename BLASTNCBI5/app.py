from flask import Flask, render_template, request, redirect, url_for, session, jsonify, make_response
import os
import uuid
import threading
import time
from services import *
from database import init_db, create_user, verify_user, get_conn, create_admin

app = Flask(__name__)
app.secret_key = "super-secret-key"

# Initialize DB and admin
init_db()
create_admin()

# Create results directory - with error handling
try:
    os.makedirs("blast_results_ncbi", exist_ok=True)
except OSError:
    print("Warning: Could not create blast_results_ncbi directory. Results will be stored in DB only.")

TAXID_CHOICES = {
    "human": "9606",
    "zebrafish": "7955"
}

# -------------------------
# Authentication
# -------------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        if create_user(username, password):
            return redirect(url_for("login"))
        else:
            return "Username already exists", 400
    return render_template("register.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        user_id, is_admin = verify_user(username, password)
        if user_id:
            session["user_id"] = user_id
            session["is_admin"] = is_admin
            return redirect(url_for("dashboard"))
        else:
            return "Invalid username or password", 401
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.pop("user_id", None)
    session.pop("is_admin", None)
    return redirect(url_for("login"))

# -------------------------
# Dashboard
# -------------------------
@app.route("/")
@app.route("/dashboard")
def dashboard():
    user_id = session.get("user_id")
    is_admin = session.get("is_admin", 0)
    if not user_id:
        return redirect(url_for("login"))

    conn = get_conn()
    cur = conn.cursor()
    if is_admin:
        cur.execute("""
            SELECT jobs.*, users.username
            FROM jobs
            LEFT JOIN users ON jobs.user_id = users.id
            ORDER BY jobs.created_at DESC
        """)
    else:
        cur.execute("SELECT * FROM jobs WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    jobs = cur.fetchall()
    conn.close()
    return render_template("dashboard.html", jobs=jobs, is_admin=is_admin)

# -------------------------
# Delete job (admin only)
# -------------------------
@app.route("/delete_job/<job_id>", methods=["POST"])
def delete_job(job_id):
    is_admin = session.get("is_admin", 0)
    if not is_admin:
        return "Unauthorized", 403

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM results WHERE job_id=?", (job_id,))
    cur.execute("DELETE FROM jobs WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("dashboard"))

# -------------------------
# Background Job
# -------------------------
def run_blast_job(job_id, accessions, taxid, timeout=900):
    import traceback
    print(f"[DEBUG] Starting job {job_id} with {len(accessions)} accessions (timeout={timeout}s per search)")
    
    total = len(accessions)
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET status='RUNNING', progress=0 WHERE id=?", (job_id,))
        conn.commit()
        conn.close()
        print(f"[DEBUG] Job {job_id} marked as RUNNING")
    except Exception as e:
        print(f"[ERROR] Failed to mark job as RUNNING: {e}")
        return

    for i, acc in enumerate(accessions):
        print(f"[DEBUG] Processing accession {i+1}/{total}: {acc}")
        try:
            time.sleep(3)  # NCBI recommends 3 seconds between requests
            fasta = fetch_fasta(acc)
            print(f"[DEBUG] Fetched FASTA for {acc}")
            
            rid = submit_blast(fasta, taxid=taxid)
            print(f"[DEBUG] Submitted BLAST for {acc}, RID: {rid}")
            
            # Wait with user-specified timeout (None = no timeout)
            wait_timeout = None if timeout == 0 else timeout
            status = wait_for_blast(rid, max_wait_time=wait_timeout)
            print(f"[DEBUG] BLAST status for {acc}: {status}")

            if status == "READY":
                xml = fetch_result(rid)
                top_hit, species, bit_score, evalue = parse_top_hit(xml)
                gene = fetch_gene_symbol(top_hit)
                
                # Try to save XML, but don't fail if we can't
                try:
                    xml_path = os.path.join("blast_results_ncbi", f"{job_id}_{acc}.xml")
                    with open(xml_path, "w") as f:
                        f.write(xml)
                except OSError:
                    # Can't write file - that's ok, results are in DB
                    pass
            elif status == "NO_HITS":
                top_hit = gene = species = bit_score = evalue = "No hit"
            elif status == "TIMEOUT":
                top_hit = gene = "TIMEOUT"
                species = "BLAST search timed out"
                bit_score = evalue = "NA"
            else:
                top_hit = gene = species = bit_score = evalue = "NA"
        except Exception as e:
            print(f"[ERROR] Error processing {acc}: {e}")
            traceback.print_exc()
            top_hit = gene = "ERROR"
            species = str(e)
            bit_score = evalue = "NA"

        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO results (job_id, accession, top_hit, gene, species, bit_score, evalue)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (job_id, acc, top_hit, gene, species, bit_score, evalue))
            progress = int((i + 1) / total * 100)
            cur.execute("UPDATE jobs SET progress=? WHERE id=?", (progress, job_id))
            conn.commit()
            conn.close()
            print(f"[DEBUG] Updated progress to {progress}%")
        except Exception as e:
            print(f"[ERROR] Failed to save results: {e}")

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET status='DONE', progress=100 WHERE id=?", (job_id,))
        conn.commit()
        conn.close()
        print(f"[DEBUG] Job {job_id} completed successfully")
    except Exception as e:
        print(f"[ERROR] Failed to mark job as DONE: {e}")

# -------------------------
# Run BLAST Route
# -------------------------
@app.route("/run", methods=["POST"])
def run():
    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("login"))

    accession_input = request.form.get("accessions", "")
    if not accession_input.strip():
        return "No accession numbers provided", 400

    organism = request.form.get("organism", "zebrafish")
    taxid = TAXID_CHOICES.get(organism, "7955")
    timeout = int(request.form.get("timeout", "900"))  # Default 15 minutes
    accessions = [a.strip() for a in accession_input.splitlines() if a.strip()]
    job_id = uuid.uuid4().hex

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO jobs (id, user_id, organism) VALUES (?, ?, ?)", (job_id, user_id, organism))
    conn.commit()
    conn.close()

    print(f"[INFO] Creating background thread for job {job_id} with timeout={timeout}s")
    thread = threading.Thread(target=run_blast_job, args=(job_id, accessions, taxid, timeout), daemon=True)
    thread.start()
    print(f"[INFO] Background thread started for job {job_id}")

    return redirect(url_for("dashboard"))

# -------------------------
# Status, Results, CSV
# -------------------------
@app.route("/status/<job_id>")
def status(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT progress, status FROM jobs WHERE id=?", (job_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Invalid job ID"}), 404
    return jsonify({"progress": row["progress"], "status": row["status"]})

@app.route("/results/<job_id>")
def results(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM results WHERE job_id=?", (job_id,))
    results = cur.fetchall()
    conn.close()
    return render_template("results.html", results=results, job_id=job_id)

@app.route("/download_csv/<job_id>")
def download_csv(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM results WHERE job_id=?", (job_id,))
    rows = cur.fetchall()
    conn.close()

    headers = ["accession", "top_hit", "gene", "species", "bit_score", "evalue"]

    def generate():
        yield ",".join(headers) + "\n"
        for r in rows:
            # Fixed: properly access Row object columns
            values = [str(r[h]) if r[h] is not None else "NA" for h in headers]
            yield ",".join(values) + "\n"

    response = make_response(generate())
    response.headers["Content-Disposition"] = f"attachment; filename=blast_results_{job_id}.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    print("Starting BLAST app with threading support...")
    app.run(debug=True, threaded=True, use_reloader=False)
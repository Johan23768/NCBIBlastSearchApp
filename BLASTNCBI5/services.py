import os
import time
import requests
import re
from xml.etree import ElementTree as ET

EMAIL = "johangoat@gmail.com"
PROGRAM = "blastp"
DATABASE = "nr"

RESULT_DIR = "blast_results_ncbi"
os.makedirs(RESULT_DIR, exist_ok=True)


def fetch_fasta(accession):
    url = (
        "https://www.ncbi.nlm.nih.gov/sviewer/viewer.fcgi"
        f"?id={accession}&db=protein&report=fasta&retmode=text"
    )
    r = requests.get(url, timeout=10)
    if r.status_code != 200 or not r.text.startswith(">"):
        raise Exception(f"Failed to fetch FASTA for {accession}")
    return r.text.strip()


def submit_blast(fasta, taxid):
    url = "https://blast.ncbi.nlm.nih.gov/Blast.cgi"
    data = {
        "CMD": "Put",
        "PROGRAM": PROGRAM,
        "DATABASE": DATABASE,
        "QUERY": fasta,
        "ENTREZ_QUERY": f"txid{taxid}[Organism]",
        "EMAIL": EMAIL,
    }
    r = requests.post(url, data=data, timeout=10)
    r.raise_for_status()

    match = re.search(r"RID = (\S+)", r.text)
    if not match:
        raise Exception("Failed to obtain RID from BLAST response")
    return match.group(1)


def wait_for_blast(rid, max_wait_time=900):
    """
    Wait for BLAST results with a timeout.
    max_wait_time: Maximum time to wait in seconds (default 15 minutes)
                   Set to 0 for no timeout (wait indefinitely)
    """
    url = "https://blast.ncbi.nlm.nih.gov/Blast.cgi"
    start_time = time.time()
    attempts = 0
    
    while True:
        elapsed = time.time() - start_time
        
        # Only check timeout if max_wait_time > 0
        if max_wait_time > 0 and elapsed > max_wait_time:
            print(f"[WARNING] BLAST search timed out after {int(elapsed)} seconds (>{max_wait_time}s limit)")
            return "TIMEOUT"
        
        time.sleep(8)
        attempts += 1
        
        try:
            params = {"CMD": "Get", "RID": rid, "FORMAT_OBJECT": "SearchInfo"}
            r = requests.get(url, params=params, timeout=30)
            text = r.text

            if "Status=READY" in text:
                if "ThereAreHits=yes" in text:
                    return "READY"
                return "NO_HITS"
            if "Status=FAILED" in text:
                return "FAILED"
            
            # Log progress every 3 attempts (~24 seconds)
            if attempts % 3 == 0:
                if max_wait_time > 0:
                    print(f"[DEBUG] Still waiting for RID {rid}... ({int(elapsed)}s elapsed, max {max_wait_time}s)")
                else:
                    print(f"[DEBUG] Still waiting for RID {rid}... ({int(elapsed)}s elapsed, no timeout)")
                
        except Exception as e:
            print(f"[WARNING] Error checking BLAST status: {e}")
            # Continue trying unless we've exceeded max wait time
            continue


def fetch_result(rid):
    url = "https://blast.ncbi.nlm.nih.gov/Blast.cgi"
    params = {"CMD": "Get", "RID": rid, "FORMAT_TYPE": "XML"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.text


def parse_top_hit(xml_text):
    """
    Returns:
    top_hit_accession, species, bit_score, evalue
    """
    root = ET.fromstring(xml_text)
    hit = root.find(".//Hit")
    if hit is None:
        return "NA", "NA", "NA", "NA"

    accession = hit.findtext("Hit_accession", default="NA")
    hit_def = hit.findtext("Hit_def", default="")

    # Extract first species ONLY
    species_match = re.search(r"\[([^\]]+)\]", hit_def)
    species = species_match.group(1) if species_match else "NA"

    hsp = hit.find(".//Hsp")
    if hsp is not None:
        bit_score = hsp.findtext("Hsp_bit-score", default="NA")
        evalue = hsp.findtext("Hsp_evalue", default="NA")
    else:
        bit_score, evalue = "NA", "NA"

    return accession, species, bit_score, evalue


def fetch_gene_symbol(accession):
    try:
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "protein",
            "id": accession,
            "rettype": "gb",
            "retmode": "text",
            "email": EMAIL,
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()

        match = re.search(r'/gene="([^"]+)"', r.text)
        if match:
            return match.group(1)
    except Exception:
        pass
    return "NA"
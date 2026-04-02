"""HetaDB multimodal evaluation runner.

End-to-end pipeline:
  1. Create a KB named eval_kb (skip if exists)
  2. Upload each dataset's files to raw_files
  3. Trigger parsing; poll until all tasks complete
  4. Run every query from ground_truth.yaml against the KB
  5. Score with keyword-match (Level 1) and optionally LLM judge (Level 2)
  6. Write a markdown report to report/eval_<timestamp>.md

Usage:
    python run_eval.py [--base-url http://localhost:8000] [--llm-judge] [--kb eval_kb]
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVAL_DIR = Path(__file__).parent
CONFIG_FILE = EVAL_DIR.parent.parent / "config.yaml"  # project root config.yaml
DATASETS_DIR = EVAL_DIR / "datasets"
GT_FILE = EVAL_DIR / "ground_truth.yaml"
REPORT_DIR = EVAL_DIR / "report"

TERMINAL = {"completed", "failed", "cancelled"}
POLL_INTERVAL = 5          # seconds between task status polls
PARSE_TIMEOUT = 3600       # max seconds to wait for all parsing tasks


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

class HetaClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.base}{path}"

    def get(self, path, **kw):
        return self.s.get(self._url(path), **kw)

    def post(self, path, **kw):
        return self.s.post(self._url(path), **kw)

    def delete(self, path, **kw):
        return self.s.delete(self._url(path), **kw)

    # --- KB management ---

    def list_kbs(self) -> list[dict]:
        r = self.get("/api/v1/hetadb/files/knowledge-bases")
        r.raise_for_status()
        return r.json().get("data", [])

    def create_kb(self, name: str) -> None:
        r = self.post("/api/v1/hetadb/files/knowledge-bases",
                      json={"name": name})
        r.raise_for_status()

    def ensure_kb(self, name: str) -> None:
        existing = {kb["name"] for kb in self.list_kbs()}
        if name not in existing:
            self.create_kb(name)
            print(f"  Created KB: {name}")
        else:
            print(f"  KB already exists: {name}")

    # --- Dataset / file upload ---

    def list_datasets(self) -> list[str]:
        r = self.get("/api/v1/hetadb/files/raw-files/datasets")
        r.raise_for_status()
        return r.json().get("data", [])

    def create_dataset(self, name: str) -> None:
        r = self.post("/api/v1/hetadb/files/raw-files/datasets",
                      json={"name": name})
        r.raise_for_status()

    def upload_file(self, dataset: str, file_path: Path) -> None:
        url = self._url(f"/api/v1/hetadb/files/raw-files/datasets/{dataset}/file")
        # Use a plain requests.post (not the session) so that the session's
        # default Content-Type: application/json header does not interfere with
        # the multipart/form-data boundary that requests sets automatically when
        # the `files` argument is present.
        with open(file_path, "rb") as f:
            r = requests.post(url, files={"file": (file_path.name, f)})
        r.raise_for_status()

    # --- Parsing ---

    def parse_kb(self, kb_name: str, datasets: list[str], force: bool = True) -> list[dict]:
        r = self.post(f"/api/v1/hetadb/files/knowledge-bases/{kb_name}/parse",
                      json={"datasets": datasets, "mode": 0, "force": force})
        r.raise_for_status()
        return r.json()["data"]["tasks"]

    def get_task(self, task_id: str) -> dict:
        r = self.get(f"/api/v1/hetadb/files/processing/tasks/{task_id}")
        r.raise_for_status()
        return r.json()

    def wait_for_tasks(self, task_refs: list[dict]) -> dict[str, str]:
        """Poll until all tasks reach a terminal state. Returns {task_id: status}."""
        pending = {t["task_id"]: t["dataset"] for t in task_refs}
        results: dict[str, str] = {}
        deadline = time.time() + PARSE_TIMEOUT

        while pending and time.time() < deadline:
            time.sleep(POLL_INTERVAL)
            for task_id in list(pending):
                task = self.get_task(task_id)
                status = task.get("status", "unknown")
                progress = task.get("progress", 0.0)
                print(f"    [{pending[task_id]}] {task_id[:8]}… {status} {progress*100:.0f}%",
                      flush=True)
                if status in TERMINAL:
                    results[task_id] = status
                    del pending[task_id]

        for task_id, dataset in pending.items():
            print(f"  TIMEOUT: task {task_id} ({dataset}) did not complete.")
            results[task_id] = "timeout"

        return results

    # --- KB content check ---

    def get_kb_detail(self, kb_name: str) -> dict:
        r = self.get(f"/api/v1/hetadb/files/knowledge-bases/{kb_name}")
        r.raise_for_status()
        return r.json()

    # --- Query ---

    def query(self, kb_id: str, query: str, query_mode: str = "naive",
              top_k: int = 10) -> dict:
        r = self.post("/api/v1/hetadb/chat",
                      json={"query": query, "kb_id": kb_id,
                            "user_id": "eval_runner",
                            "query_mode": query_mode, "top_k": top_k})
        r.raise_for_status()
        payload = r.json()
        # Raise a descriptive error if the API rejected the request
        if not payload.get("success", False):
            raise RuntimeError(f"Chat API error: {payload.get('message', payload)}")
        return payload


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_keywords(response_text: str, keywords: list[str]) -> tuple[bool, list[str]]:
    """Level 1: check if all required keywords appear in the response (case-insensitive)."""
    if not keywords:
        return True, []
    text_lower = response_text.lower()
    missing = [kw for kw in keywords if kw.lower() not in text_lower]
    return len(missing) == 0, missing


def _load_judge_llm_config() -> dict:
    """Read hetadb.llm from config.yaml at the project root."""
    with open(CONFIG_FILE, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Resolve YAML anchors — PyYAML expands them automatically, so
    # hetadb.llm already has api_key / base_url / model merged in.
    llm_cfg = raw.get("hetadb", {}).get("llm", {})
    required = ("api_key", "base_url", "model")
    missing = [k for k in required if not llm_cfg.get(k)]
    if missing:
        raise ValueError(f"config.yaml hetadb.llm is missing keys: {missing}")
    return llm_cfg


def score_llm_judge(query: str, expected: str, actual: str) -> tuple[int, str]:
    """Level 2: call hetadb.llm directly (no retrieval) to score the answer 0-5.

    Uses the OpenAI-compatible endpoint configured in config.yaml so the judge
    sees only the question + expected + actual answer — not any KB context.

    Returns (score 0-5, explanation string).
    """
    try:
        from openai import OpenAI
    except ImportError:
        return -1, "openai package not installed (pip install openai)"

    try:
        cfg = _load_judge_llm_config()
    except Exception as e:
        return -1, f"Config load error: {e}"

    # Empty / blank responses from the RAG system always score 0 — do not
    # infer the answer from the judge's own training knowledge.
    actual_stripped = actual.strip()
    if not actual_stripped:
        return 0, "Model returned an empty response."

    prompt = (
        "You are an evaluation judge for a retrieval-augmented generation system.\n"
        "Score the model answer from 0 to 5 based on factual correctness.\n\n"
        "CRITICAL: Base your score ONLY on what the model answer says. "
        "Do NOT use your own knowledge to fill gaps. "
        "If the model answer is empty, vague, or says it has no information, give SCORE: 0.\n\n"
        f"Question: {query}\n"
        f"Expected answer: {expected}\n"
        f"Model answer: {actual_stripped}\n\n"
        "Scoring criteria:\n"
        "5 = Fully correct and complete.\n"
        "4 = Mostly correct, minor omission or extra detail.\n"
        "3 = Partially correct, the key fact is present but incomplete.\n"
        "2 = Tangentially related but the key fact is missing.\n"
        "1 = Incorrect but on-topic.\n"
        "0 = Wrong, off-topic, empty, or the model said it has no information.\n\n"
        "Respond with ONLY two lines:\n"
        "SCORE: <0-5>\n"
        "REASON: <one sentence>"
    )

    try:
        llm = OpenAI(
            api_key=cfg["api_key"],
            base_url=cfg["base_url"],
            timeout=float(cfg.get("timeout", 60)),
        )
        resp = llm.chat.completions.create(
            model="qwen3-max",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=128,
            temperature=0,
        )
        text = resp.choices[0].message.content or ""
        m = re.search(r"SCORE:\s*([0-5])", text)
        score = int(m.group(1)) if m else -1
        m2 = re.search(r"REASON:\s*(.+)", text, re.DOTALL)
        reason = m2.group(1).strip().splitlines()[0] if m2 else text[:200]
        return score, reason
    except Exception as e:
        return -1, f"LLM call error: {e}"


# ---------------------------------------------------------------------------
# Setup: upload datasets
# ---------------------------------------------------------------------------

def setup_datasets(client: HetaClient, dataset_names: list[str]) -> None:
    existing = set(client.list_datasets())
    for ds_name in dataset_names:
        ds_dir = DATASETS_DIR / ds_name
        if not ds_dir.exists():
            print(f"  WARNING: dataset directory not found: {ds_dir}")
            continue

        files = [f for f in ds_dir.iterdir() if f.is_file()]
        if not files:
            print(f"  WARNING: no files in {ds_dir}")
            continue

        if ds_name not in existing:
            client.create_dataset(ds_name)
            print(f"  Created dataset: {ds_name}")

        for file_path in files:
            print(f"    Uploading {file_path.name} → {ds_name}")
            try:
                client.upload_file(ds_name, file_path)
            except Exception as e:
                print(f"    ERROR uploading {file_path.name}: {e}")


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(results: list[dict], use_llm_judge: bool, query_mode: str = "naive") -> str:
    total = len(results)
    kw_pass = sum(1 for r in results if r["kw_pass"])
    kw_fail = total - kw_pass

    lines = [
        "# HetaDB Evaluation Report",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"\n## Summary\n",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Query mode | {query_mode} |",
        f"| Total queries | {total} |",
        f"| Keyword match pass | {kw_pass} ({kw_pass/total*100:.1f}%) |",
        f"| Keyword match fail | {kw_fail} ({kw_fail/total*100:.1f}%) |",
    ]

    if use_llm_judge:
        judged = [r for r in results if r.get("llm_score", -1) >= 0]
        if judged:
            avg = sum(r["llm_score"] for r in judged) / len(judged)
            lines.append(f"| LLM judge avg score (0-5) | {avg:.2f} |")

    # Results by dataset
    lines.append("\n## Results by Dataset\n")
    datasets: dict[str, list[dict]] = {}
    for r in results:
        datasets.setdefault(r["dataset"], []).append(r)

    for ds, ds_results in datasets.items():
        ds_pass = sum(1 for r in ds_results if r["kw_pass"])
        lines.append(f"### {ds}  ({ds_pass}/{len(ds_results)} pass)\n")
        lines.append("| ID | Type | Pass | Missing keywords | LLM score |")
        lines.append("|----|------|------|-----------------|-----------|")
        for r in ds_results:
            kw_status = "✅" if r["kw_pass"] else "❌"
            missing = ", ".join(r["missing_kw"]) if r["missing_kw"] else "—"
            llm = str(r.get("llm_score", "—"))
            lines.append(f"| {r['id']} | {r['eval_type']} | {kw_status} | {missing} | {llm} |")
        lines.append("")

    # Failure details
    failures = [r for r in results if not r["kw_pass"]]
    if failures:
        lines.append("## Failure Details\n")
        for r in failures:
            lines.append(f"### {r['id']} ({r['dataset']})")
            lines.append(f"**Query:** {r['query']}\n")
            lines.append(f"**Expected:** {r['expected']}\n")
            lines.append(f"**Missing keywords:** {', '.join(r['missing_kw'])}\n")
            lines.append(f"**Actual response (truncated):**\n```\n{r['actual'][:500]}\n```\n")
            if "llm_reason" in r:
                lines.append(f"**LLM judge reason:** {r['llm_reason']}\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HetaDB evaluation runner")
    parser.add_argument("--base-url", default="http://localhost:8000",
                        help="HetaDB API base URL")
    parser.add_argument("--kb", default="eval_kb",
                        help="Knowledge base name to use (default: eval_kb)")
    parser.add_argument("--llm-judge", action="store_true",
                        help="Enable Level 2 LLM judge scoring")
    parser.add_argument("--query-mode", default="naive",
                        help="Query mode to use (default: naive)")
    parser.add_argument("--skip-setup", action="store_true",
                        help="Skip file upload and parsing (use already-parsed KB)")
    parser.add_argument("--datasets", nargs="*",
                        help="Limit to specific datasets (default: all)")
    args = parser.parse_args()

    client = HetaClient(args.base_url)

    # Load ground truth
    with open(GT_FILE, encoding="utf-8") as f:
        ground_truth: list[dict] = yaml.safe_load(f)

    # Filter by requested datasets
    if args.datasets:
        ground_truth = [g for g in ground_truth if g["dataset"] in args.datasets]
        dataset_names = args.datasets
    else:
        dataset_names = sorted({g["dataset"] for g in ground_truth})

    print(f"\n{'='*60}")
    print(f"HetaDB Eval — KB: {args.kb} | Mode: {args.query_mode} | Queries: {len(ground_truth)}")
    print(f"Datasets: {dataset_names}")
    print(f"{'='*60}\n")

    # ── Step 1: Setup ──
    if not args.skip_setup:
        print("Step 1: Setting up KB and uploading files…")
        client.ensure_kb(args.kb)
        setup_datasets(client, dataset_names)

        print("\nStep 2: Triggering parsing…")
        tasks = client.parse_kb(args.kb, dataset_names, force=True)
        print(f"  Submitted {len(tasks)} parsing task(s). Polling…\n")
        task_results = client.wait_for_tasks(tasks)

        failed_tasks = [tid for tid, s in task_results.items() if s != "completed"]
        if failed_tasks:
            print(f"\nWARNING: {len(failed_tasks)} task(s) did not complete successfully:")
            for tid in failed_tasks:
                print(f"  {tid}: {task_results[tid]}")
            if len(failed_tasks) == len(tasks):
                print("All parsing tasks failed. Aborting.")
                sys.exit(1)
    else:
        print("Skipping setup (--skip-setup).\n")

    # ── Step 2.5: Verify KB has parsed content ──
    print("\nVerifying KB content…")
    try:
        detail = client.get_kb_detail(args.kb)
        datasets_in_kb = detail.get("datasets", [])
        parsed = [d for d in datasets_in_kb if d.get("parsed")]
        unparsed = [d["name"] for d in datasets_in_kb if not d.get("parsed")]
        print(f"  Parsed datasets : {[d['name'] for d in parsed]}")
        if unparsed:
            print(f"  WARNING — not parsed: {unparsed}")
        if not parsed:
            print("  ERROR: no parsed datasets found — queries will hit an empty KB.")
            print("  Fix the upload errors above and re-run without --skip-setup.")
            sys.exit(1)
    except Exception as e:
        print(f"  WARNING: could not verify KB content: {e}")

    # ── Step 3: Run queries ──
    print(f"\nStep 3: Running {len(ground_truth)} queries…\n")
    eval_results = []

    for i, gt in enumerate(ground_truth, 1):
        qid = gt["id"]
        dataset = gt["dataset"]
        query = gt["query"]
        expected = gt["expected"]
        keywords = gt.get("keywords", [])
        eval_type = gt.get("eval_type", "unknown")

        print(f"  [{i:02d}/{len(ground_truth)}] {qid} — {query[:70]}…")

        try:
            resp = client.query(args.kb, query, query_mode=args.query_mode)
            actual = resp.get("response") or ""
            if not actual and resp.get("data"):
                # Fall back to concatenated retrieval snippets
                actual = " ".join(r.get("text", "") for r in resp["data"][:3])
        except Exception as e:
            actual = f"ERROR: {e}"
            print(f"    Query error: {e}")

        kw_pass, missing_kw = score_keywords(actual, keywords)
        status_icon = "✅" if kw_pass else "❌"
        print(f"    {status_icon} keyword match | missing: {missing_kw or 'none'}")

        result = {
            "id": qid,
            "dataset": dataset,
            "query": query,
            "expected": expected,
            "actual": actual,
            "eval_type": eval_type,
            "kw_pass": kw_pass,
            "missing_kw": missing_kw,
        }

        if args.llm_judge:
            llm_score, llm_reason = score_llm_judge(query, expected, actual)
            result["llm_score"] = llm_score
            result["llm_reason"] = llm_reason
            print(f"    LLM judge: {llm_score}/5 — {llm_reason[:80]}")

        eval_results.append(result)

    # ── Step 4: Write report ──
    REPORT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORT_DIR / f"eval_{ts}.md"
    report_md = generate_report(eval_results, use_llm_judge=args.llm_judge, query_mode=args.query_mode)
    report_path.write_text(report_md, encoding="utf-8")

    # Also dump raw JSON for further analysis
    json_path = REPORT_DIR / f"eval_{ts}.json"
    json_path.write_text(
        json.dumps(eval_results, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── Final summary ──
    total = len(eval_results)
    kw_pass = sum(1 for r in eval_results if r["kw_pass"])
    print(f"\n{'='*60}")
    print(f"DONE — {kw_pass}/{total} keyword-match pass ({kw_pass/total*100:.1f}%)")
    print(f"Report: {report_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

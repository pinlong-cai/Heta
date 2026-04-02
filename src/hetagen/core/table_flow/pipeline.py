"""
Table generation to Text-to-SQL pipeline.

Flow: Table Schema -> (Query Generation -> Parallel Retrieval -> CSV -> SQLite) || Text2SQL -> SQL Execution
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional

from hetagen.core.table_flow.config import Config
from hetagen.core.table_flow.table_schema import TableDefiner
from hetagen.core.table_flow.generate_queries import QueryGenerator
from hetagen.core.table_flow.parallel_retrieve import ParallelRetriever
from hetagen.core.table_flow.table_fill import generate_csv
from hetagen.core.table_flow.csv_to_sqlite import csv_to_sqlite, query_sqlite
from hetagen.core.table_flow.text_to_sql import Text2SQLConverter

logger = logging.getLogger(__name__)


class PipelineCancelledError(Exception):
    """Raised when pipeline is cancelled by user."""
    pass


def run_pipeline(
    question: str,
    sql_question: Optional[str] = None,
    output_dir: str = "data",
    top_k: int = 5,
    threshold: float = 0.5,
    max_workers: int = 16,
    verbose: bool = False,
    progress_callback: Optional[Callable[[int], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    """
    Run the complete pipeline: Table Definition -> Data Fill & Text2SQL (parallel) -> SQL Execution.

    Args:
        question: User question for table generation
        sql_question: Question for SQL query (defaults to question)
        output_dir: Output directory for generated files
        top_k: Maximum retrieval results
        threshold: Similarity threshold
        max_workers: Maximum concurrent threads
        verbose: Enable detailed logging
        progress_callback: Optional callback(step: int) for progress updates (0-3)
        cancel_check: Optional callback() -> bool, returns True if cancelled

    Returns:
        Dict containing csv_path, sqlite_path, table_name, schema, sql, results, elapsed_time

    Raises:
        PipelineCancelledError: If cancelled by user
    """
    def report_step(step: int):
        """Report progress step (0-3)."""
        if progress_callback:
            progress_callback(step)

    def check_cancel():
        if cancel_check and cancel_check():
            raise PipelineCancelledError("Pipeline cancelled by user")

    config = Config()
    pipeline_start = time.time()

    logger.info("=" * 50)
    logger.info("Pipeline started")
    logger.info("Question: %s", question[:80] + "..." if len(question) > 80 else question)
    logger.info("=" * 50)

    # Step 1: Define table schema
    report_step(0)
    logger.info("[1/6] Defining table schema...")
    step_start = time.time()
    table_definer = TableDefiner(config)
    schema = table_definer.define_table(question)
    schema_dict = schema.to_dict()
    title = schema_dict.get("title", "table")
    table_name = title
    logger.info("[1/6] Table: %s (%.2fs)", title, time.time() - step_start)

    check_cancel()

    if verbose:
        logger.debug("Schema: %s", json.dumps(schema_dict, ensure_ascii=False))

    def data_pipeline_branch():
        """Data pipeline: Query Generation -> Retrieval -> CSV -> SQLite"""
        branch_results = {}

        # Step 2: Generate queries
        report_step(1)
        logger.info("[2/6] Generating queries...")
        step_start = time.time()
        generator = QueryGenerator(config)
        queries = generator.generate(schema)
        logger.info("[2/6] Generated %d queries (%.2fs)", len(queries), time.time() - step_start)

        check_cancel()

        if verbose:
            logger.debug("Queries: %s", json.dumps(queries, ensure_ascii=False))

        # Step 3: Parallel retrieval
        logger.info("[3/6] Retrieving data...")
        step_start = time.time()
        retriever = ParallelRetriever(
            config=config,
            top_k=top_k,
            threshold=threshold,
            max_workers=max_workers
        )
        try:
            results = retriever.process_batch(queries)
        finally:
            retriever.close()
        logger.info("[3/6] Retrieval complete (%.2fs)", time.time() - step_start)

        check_cancel()

        if verbose:
            logger.debug("Results: %s", json.dumps(results, ensure_ascii=False))

        # Step 4: Generate CSV
        report_step(2)
        logger.info("[4/6] Generating CSV...")
        step_start = time.time()
        csv_content = generate_csv(schema_dict, results)
        csv_path = f"{output_dir}/{title}.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            f.write(csv_content)
        logger.info("[4/6] CSV saved (%.2fs)", time.time() - step_start)

        check_cancel()

        # Step 5: Convert to SQLite
        logger.info("[5/6] Converting to SQLite...")
        step_start = time.time()
        sqlite_path = csv_to_sqlite(csv_path, schema=schema_dict)
        logger.info("[5/6] SQLite saved (%.2fs)", time.time() - step_start)

        branch_results["csv_path"] = csv_path
        branch_results["sqlite_path"] = sqlite_path
        return branch_results

    def text2sql_branch():
        """Text2SQL: Generate SQL query"""
        step_start = time.time()
        sql_q = sql_question or question
        converter = Text2SQLConverter(config)
        sql = converter.convert(sql_q, schema_dict, table_name)
        return {
            "question": sql_q,
            "sql": sql,
            "elapsed_time": time.time() - step_start
        }

    # Execute branches in parallel
    logger.info("Running data pipeline and Text2SQL in parallel...")
    parallel_start = time.time()

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_data = executor.submit(data_pipeline_branch)
        future_sql = executor.submit(text2sql_branch)
        data_result = future_data.result()
        sql_result = future_sql.result()

    logger.info("Parallel execution complete (%.2fs)", time.time() - parallel_start)

    check_cancel()

    csv_path = data_result["csv_path"]
    sqlite_path = data_result["sqlite_path"]
    sql = sql_result["sql"]

    logger.info("Text2SQL: %s (%.2fs)", sql[:60] + "..." if len(sql) > 60 else sql, sql_result["elapsed_time"])

    # Step 6: Execute SQL
    report_step(3)
    logger.info("[6/6] Executing SQL query...")
    step_start = time.time()
    query_results = query_sqlite(sqlite_path, sql)
    logger.info("[6/6] Query returned %d rows (%.2fs)", len(query_results), time.time() - step_start)

    total_elapsed = time.time() - pipeline_start

    logger.info("=" * 50)
    logger.info("Pipeline complete (%.2fs)", total_elapsed)
    logger.info("=" * 50)

    return {
        "csv_path": csv_path,
        "sqlite_path": sqlite_path,
        "table_name": table_name,
        "schema": schema_dict,
        "sql": sql,
        "results": query_results,
        "elapsed_time": total_elapsed
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S"
    )

    example = "找出纳斯达克上市的公司中市值前十的公司。"
    result = run_pipeline(
        question=example,
        output_dir="data",
        top_k=5,
        threshold=0.4,
        max_workers=32,
        verbose=True
    )

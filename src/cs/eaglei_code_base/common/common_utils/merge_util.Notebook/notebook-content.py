# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import Union, List
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List


def _dedup_df(
    df: DataFrame,
    key_cols: List[str],
    order_cols: List[str] = None,
) -> DataFrame:
    """
    Deduplicate *df* by *key_cols*, keeping the row with the highest
    rank according to *order_cols* (descending).

    When *order_cols* is None the function falls back to keeping the
    first occurrence (arbitrary but deterministic within a partition).
    """
    if order_cols:
        order_exprs = [F.col(c).desc() for c in order_cols]
    else:
        # Stable but arbitrary ordering when no explicit order columns are provided
        order_exprs = [F.lit(1)]

    w = Window.partitionBy(*key_cols).orderBy(*order_exprs)

    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────
# merge_nonscd2 - SCD Type 1 (insert / update, no history)
# ─────────────────────────────────────────────────────────────────
def merge_nonscd2(
    source_df: DataFrame,
    target_df: DataFrame,
    condition: Union[str, List[str]],
    order_cols: List[str] = None,
    target_table_path: str = None,  # Delta path; None -> in-memory join
    spark: SparkSession = None,
) -> DataFrame:
    """SCD Type 1 merge: insert/update without history.

    Parameters
    ----------
    source_df        : incoming / staging DataFrame
    target_df        : existing / base DataFrame
    condition        : join key column name(s) as a str or list[str]
    order_cols       : columns used to pick the "latest" duplicate row
    target_table_path: (optional) Delta Lake table path for native MERGE
    spark            : SparkSession (required when target_table_path given)

    Returns
    -------
    DataFrame: merged result (SCD Type 1 semantics).
    """
    # Normalise condition to a list
    key_cols: List[str] = [condition] if isinstance(condition, str) else list(condition)

    # 1. Deduplicate both sides
    src_dedup = _dedup_df(source_df, key_cols, order_cols)
    tgt_dedup = _dedup_df(target_df, key_cols, order_cols)

    # 2. Delta Lake native MERGE (preferred for production)
    if target_table_path and spark:
        join_expr = " AND ".join([f"tgt.{c} = src.{c}" for c in key_cols])
        non_key_cols = [c for c in src_dedup.columns if c not in key_cols]

        delta_tgt = DeltaTable.forPath(spark, target_table_path)
        (
            delta_tgt.alias("tgt")
            .merge(src_dedup.alias("src"), join_expr)
            .whenMatchedUpdate(set={c: F.col(f"src.{c}") for c in non_key_cols})
            .whenNotMatchedInsertAll()
            .execute()
        )
        return delta_tgt.toDF()

    # 3. Pure DataFrame MERGE (in-memory)
    all_cols = tgt_dedup.columns
    non_key_cols = [c for c in all_cols if c not in key_cols]

    # Suffix source cols to avoid ambiguity during join
    src_renamed = src_dedup.select(*[F.col(c).alias(f"_src_{c}") for c in src_dedup.columns])

    joined = tgt_dedup.join(
        src_renamed,
        on=[tgt_dedup[c] == src_renamed[f"_src_{c}"] for c in key_cols],
        how="full",
    )

    # Build the final column expressions
    result_cols = []

    # Keys: coalesce target/source
    for c in key_cols:
        result_cols.append(F.coalesce(F.col(c), F.col(f"_src_{c}")).alias(c))

    # Non-key columns: prefer source when available
    src_cols_prefixed = {f"_src_{x}" for x in src_dedup.columns}
    for c in non_key_cols:
        src_col = f"_src_{c}"
        if src_col in src_cols_prefixed:
            # Matched -> take source value; unmatched -> keep target
            result_cols.append(F.coalesce(F.col(src_col), F.col(c)).alias(c))
        else:
            result_cols.append(F.col(c))

    # Also bring in any SOURCE-ONLY columns not present in target
    tgt_col_set = set(tgt_dedup.columns)
    for c in src_dedup.columns:
        if c not in tgt_col_set:
            result_cols.append(F.col(f"_src_{c}").alias(c))

    return joined.select(*result_cols)


# ─────────────────────────────────────────────────────────────────
# merge_scd2 - SCD Type 2 (full history with create/modified)
# ─────────────────────────────────────────────────────────────────
def merge_scd2(
    source_df: DataFrame,
    target_df: DataFrame,
    condition: Union[str, List[str]],
    order_cols: List[str] = None,
    create_date_col: str = "create_date",
    modified_date_col: str = "modified_date",
    active_flag_col: str = "is_active",
    surrogate_key_col: str = None,         # optional - e.g. "sk_id"
    target_table_path: str = None,         # Delta path for native MERGE
    spark: SparkSession = None,
) -> DataFrame:
    """SCD Type 2 merge: maintains full history with active/expired rows.

    Parameters
    ----------
    source_df         : incoming / staging DataFrame
    target_df         : existing / history DataFrame
    condition         : business key column name(s)
    order_cols        : tiebreak columns for deduplication
    create_date_col   : name of the creation-timestamp column
    modified_date_col : name of the last-modified-timestamp column
    active_flag_col   : boolean column marking the current version
    surrogate_key_col : optional surrogate-key column (monotonically
                        increasing id added to new/inserted rows)
    target_table_path : (optional) Delta Lake table path
    spark             : SparkSession (required when target_table_path given)

    Returns
    -------
    DataFrame: merged result with full SCD Type 2 history.
    """
    key_cols: List[str] = [condition] if isinstance(condition, str) else list(condition)
    now = F.lit(datetime.utcnow())

    # 1. Dedup both sides
    src_dedup = _dedup_df(source_df, key_cols, order_cols)
    tgt_dedup = _dedup_df(
        target_df.filter(F.col(active_flag_col) == True),  # only current rows
        key_cols,
        order_cols,
    )
    tgt_history = target_df.filter(F.col(active_flag_col) == False)  # expired rows

    # 2. Delta Lake native MERGE (preferred for production)
    if target_table_path and spark:
        join_expr = " AND ".join([f"tgt.{c} = src.{c}" for c in key_cols])
        non_key_cols = [
            c for c in src_dedup.columns
            if c not in key_cols
            and c not in {create_date_col, modified_date_col, active_flag_col}
        ]
        # Build change-detection predicate
        change_pred = " OR ".join([f"tgt.{c} <> src.{c}" for c in non_key_cols])

        delta_tgt = DeltaTable.forPath(spark, target_table_path)
        (
            delta_tgt.alias("tgt")
            .merge(src_dedup.alias("src"), join_expr)
            # Expire changed rows
            .whenMatchedUpdate(
                condition=change_pred,
                set={
                    active_flag_col: F.lit(False),
                    modified_date_col: F.current_timestamp(),
                },
            )
            # Insert new or changed rows
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Insert new source rows (brand new business keys)
        new_keys = src_dedup.join(
            delta_tgt.toDF().filter(F.col(active_flag_col) == True),
            on=key_cols,
            how="left_anti",
        )
        if new_keys.count() > 0:
            new_rows = (
                new_keys.withColumn(create_date_col, now)
                .withColumn(modified_date_col, now)
                .withColumn(active_flag_col, F.lit(True))
            )
            new_rows.write.format("delta").mode("append").save(target_table_path)

        return delta_tgt.toDF()

    # 3. Pure DataFrame MERGE (in-memory)
    non_key_scd_excluded = {create_date_col, modified_date_col, active_flag_col}
    non_key_cols = [
        c for c in src_dedup.columns
        if c not in key_cols and c not in non_key_scd_excluded
    ]

    # Rename source columns to avoid ambiguity
    src_renamed = src_dedup.select(*[F.col(c).alias(f"_src_{c}") for c in src_dedup.columns])

    # Join ONLY current (active) target rows with source
    joined = tgt_dedup.join(
        src_renamed,
        on=[tgt_dedup[c] == src_renamed[f"_src_{c}"] for c in key_cols],
        how="full",
    )

    # Classify each row
    first_key = key_cols[0]
    is_matched = (
        F.col(first_key).isNotNull()
        & F.col(f"_src_{first_key}").isNotNull()
    )
    is_new = (
        F.col(first_key).isNull()
        & F.col(f"_src_{first_key}").isNotNull()
    )
    is_tgt_only = (
        F.col(first_key).isNotNull()
        & F.col(f"_src_{first_key}").isNull()
    )

    # Detect actual changes among matched rows
    change_expr = F.lit(False)
    src_cols_prefixed = {f"_src_{x}" for x in src_dedup.columns}
    for c in non_key_cols:
        if f"_src_{c}" in src_cols_prefixed:
            change_expr = change_expr | (F.col(c) != F.col(f"_src_{c}"))

    is_changed = is_matched & change_expr
    is_unchanged = is_matched & ~change_expr

    # Helper to build a row from source columns
    def _src_row(use_existing_create: bool = False):
        cols = []
        for c in key_cols:
            cols.append(F.col(f"_src_{c}").alias(c))
        for c in non_key_cols:
            src_c = f"_src_{c}"
            if src_c in src_cols_prefixed:
                cols.append(F.col(src_c).alias(c))
            else:
                cols.append(F.lit(None).cast("string").alias(c))
        cols.append(
            (F.col(create_date_col) if use_existing_create else now).alias(create_date_col)
        )
        cols.append(now.alias(modified_date_col))
        cols.append(F.lit(True).alias(active_flag_col))
        return cols

    # Helper to keep/expire target rows
    def _tgt_row(expire: bool = False):
        cols = []
        for c in key_cols:
            cols.append(F.col(c))
        for c in non_key_cols:
            cols.append(F.col(c))
        cols.append(F.col(create_date_col))
        cols.append(
            (now if expire else F.col(modified_date_col)).alias(modified_date_col)
        )
        cols.append(
            (F.lit(False) if expire else F.col(active_flag_col)).alias(active_flag_col)
        )
        return cols

    # Build final column list (consistent schema)
    final_cols = key_cols + non_key_cols + [
        create_date_col,
        modified_date_col,
        active_flag_col,
    ]

    # 1) Unchanged matched rows -> keep target row as-is
    unchanged_rows = joined.filter(is_unchanged).select(*[F.col(c) for c in final_cols])

    # 2) Changed matched rows -> expire old, insert new version
    expired_rows = (
        joined.filter(is_changed)
        .select(*_tgt_row(expire=True))
        .toDF(*final_cols)
    )
    new_versions = (
        joined.filter(is_changed)
        .select(*_src_row(use_existing_create=False))
        .toDF(*final_cols)
    )

    # 3) Brand new business keys (source only) -> insert
    inserted_rows = (
        joined.filter(is_new)
        .select(*_src_row(use_existing_create=False))
        .toDF(*final_cols)
    )

    # 4) Target-only rows (deleted from source) -> carry forward as inactive
    tgt_only_rows = joined.filter(is_tgt_only).select(*[F.col(c) for c in final_cols])

    # Combine all segments + historical (already-expired) rows
    result = (
        unchanged_rows
        .unionByName(expired_rows)
        .unionByName(new_versions)
        .unionByName(inserted_rows)
        .unionByName(tgt_only_rows)
        .unionByName(tgt_history.select(*final_cols))
    )

    # Optionally add/refresh surrogate key
    if surrogate_key_col:
        result = result.withColumn(
            surrogate_key_col, F.monotonically_increasing_id()
        )

    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

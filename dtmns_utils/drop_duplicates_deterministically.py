
from .custom import IdentifierContainsNulls, MissingUniqueIdentifier
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

def check_for_nulls(df: DataFrame, col: str) -> None:
    """Raise error if column contains any null values"""
    null_count = df.filter(F.col(col).isNull()).limit(1).count()
    if null_count > 0:
        raise IdentifierContainsNulls(f"Column '{col}' contains null values")


def ensure_distinct_vals(df: DataFrame, col: str) -> None:
    """Raise error if column contains duplicate values"""
    counts = df.agg(
        F.count(col).alias("total"),
        F.countDistinct(col).alias("unique")
    ).collect()[0]
    
    if counts["total"] != counts["unique"]:
        raise MissingUniqueIdentifier(f"Column '{col}' contains {counts['total'] - counts['unique']} duplicate values")


def drop_deterministically(
    df: DataFrame, 
    _window: list[str], 
    order: str,
    validate: bool = True
) -> DataFrame:
    """
    Deterministically drop duplicates using window functions.
    
    Args:
        df: Input DataFrame
        _window: Columns to partition by
        order: Column to order by (must be unique and non-null if validate=True)
        validate: Whether to validate order column (default: True)
    
    Returns:
        DataFrame with duplicates removed (first row per partition kept)
    """
    if validate:
        check_for_nulls(df, order)
        ensure_distinct_vals(df, order)
    
    win = Window.partitionBy(_window).orderBy(order)
    return (df
            .withColumn('_rank', F.row_number().over(win))
            .filter(F.col('_rank') == 1)
            .drop('_rank'))


def exclude_columns(
    df: DataFrame, 
    _exclude: list[str]
) -> list[str]:
    """
    Quickly get a list of windowing columns, excluding specified column names.
    In most cases it is wise to exclude the column that you will order the partition
    by.
    
    Args:
        df: Input DataFrame
        exclude_patterns: Substrings to exclude 
    
    Returns:
        List of column names to drop by.
    """
    
    
    return [
        c for c in df.columns if not any(pattern.lower() in c.lower() for pattern in _exclude)
        ]
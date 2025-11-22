import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Optional

def add_audit_metadata(
    df: DataFrame,
    source_system: str,
    source_table: str,
    load_type: str = "full",
    pipeline_id: Optional[str] = None,
    add_hash_key: bool = True,
    hash_columns: Optional[list] = None
) -> DataFrame:
    """
    Add standardized audit metadata columns to a DataFrame before writing to Silver layer.
        
    Returns:
    --------
    DataFrame with additional audit columns:
        - audit_insert_timestamp: When record was inserted into Silver
        - audit_update_timestamp: When record was last updated
        - audit_source_system: Source system name
        - audit_source_table: Source table name
        - audit_load_type: Type of data load
        - audit_pipeline_id: Pipeline execution identifier
        - audit_source_file: Source file path (if from file)
        - audit_record_hash: Hash of record for change detection (optional)
    """
    
    # Get current timestamp for audit fields
    current_ts = F.current_timestamp()
    
    # Add standard audit columns
    df_with_audit = (df
        .withColumn("audit_insert_timestamp", current_ts)
        .withColumn("audit_update_timestamp", current_ts)
        .withColumn("audit_source_system", F.lit(source_system))
        .withColumn("audit_source_table", F.lit(source_table))
        .withColumn("audit_load_type", F.lit(load_type))
        .withColumn("audit_pipeline_id", F.lit(pipeline_id if pipeline_id else "manual"))
    )
    
    # Try to add source file path if available (for file-based sources)
    try:
        df_with_audit = df_with_audit.withColumn("audit_source_file", F.input_file_name())
    except:
        # If not from files, set to NULL
        df_with_audit = df_with_audit.withColumn("audit_source_file", F.lit(None).cast("string"))
    
    # Add hash key for change detection if requested
    if add_hash_key:
        if hash_columns is None:
            # Use all original columns (excluding audit columns we just added)
            hash_columns = [col for col in df.columns]
        
        # Create concatenated string of all hash columns and compute SHA2 hash
        df_with_audit = df_with_audit.withColumn(
            "audit_record_hash",
            F.sha2(F.concat_ws("||", *hash_columns), 256)
        )
    
    return df_with_audit
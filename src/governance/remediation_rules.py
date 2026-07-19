from typing import Dict, Callable
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, when, lit

# Define a clear type alias for our remediation rule functions
# Each rule takes a column name and returns a compiled Spark Column expression
RemediationRule = Callable[[str], Column]

def fix_empty_status(column_name: str) -> Column:
    """
    Remediates empty or null values in a categorical column by falling back to 'UNKNOWN'.
    
    Why: Standardizes missing order statuses at the Silver entry point to prevent 
    downstream BI/dbt models from failing on NULL integrity constraints.
    """
    c = col(column_name)
    return when((c == "") | (c.isNull()), lit("UNKNOWN")).otherwise(c)

# --- Centralized Remediation Registry (The Decoupled Abstraction) ---
# To register a new auto-fix rule, simply add it to this dictionary mapping.
REMEDIATION_RULES: Dict[str, RemediationRule] = {
    "FIX_EMPTY_STATUS": fix_empty_status
}

def apply_auto_remediation(df: DataFrame, target_column: str, rule_name: str) -> DataFrame:
    """
    Applies a registered auto-remediation rule to a Spark DataFrame and tags appropriate metadata.
    
    Why: Adheres to the single-responsibility and abstraction constraints. Instead of hardcoding 
    mutations in the main pipeline, this utility acts as a generic orchestrator for any registered rule.
    """
    if rule_name not in REMEDIATION_RULES:
        raise ValueError(f"Rule '{rule_name}' is not registered in the remediation registry.")
        
    rule_func = REMEDIATION_RULES[rule_name]
    column_ref = col(target_column)
    
    # Establish the trigger condition for metadata logging (e.g., status is empty or null)
    trigger_condition = (column_ref == "") | (column_ref.isNull())
    
    return (
        df
        .withColumn("_is_remediated", trigger_condition)
        .withColumn(
            "_remediation_rule",
            when(trigger_condition, lit(rule_name)).otherwise(lit(None).cast("string"))
        )
        .withColumn(target_column, rule_func(target_column))
    )
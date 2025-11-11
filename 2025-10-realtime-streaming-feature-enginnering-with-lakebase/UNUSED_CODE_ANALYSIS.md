# Unused Code Analysis

**Date:** November 11, 2025  
**Status:** ✅ **CLEANUP COMPLETED**  
**Scope:** Complete codebase analysis for unused functions, methods, and code

---

## Cleanup Summary

**All identified unused code has been successfully removed.**

### Files Modified:
1. **`utils/lakebase_client.py`** - Reduced from 701 to 623 lines (-78 lines, -11.1%)
2. **`utils/feature_engineering.py`** - Reduced from 721 to 681 lines (-40 lines, -5.5%)

### Total Impact:
- **Lines removed:** 118 lines
- **Files cleaned:** 2
- **Breaking changes:** None
- **Risk level:** Low

---

## Changes Made

### ✅ DELETED - `utils/lakebase_client.py`

#### 1. Removed `write_streaming()` method (~33 lines)

**Previously at:** Lines 341-373

**Reason:** Never called anywhere in the codebase. The project exclusively uses `write_streaming_batch()` and the ForeachWriter pattern for streaming writes.

#### 2. Removed `get_lakebase_client_from_secrets()` function (~38 lines)

**Previously at:** Lines 491-528

**Reason:** Outdated authentication pattern. The current `LakebaseClient` uses Databricks SDK's `WorkspaceClient()` and `generate_database_credential()` for authentication.

---

### ✅ DELETED - `utils/feature_engineering.py`

#### 1. Removed duplicate `import builtins` statement

**Previously at:** Line 73

**Reason:** Duplicate import (already imported on line 67).

#### 2. Removed `write_features_to_lakebase()` method (~30 lines)

**Previously at:** Lines 359-388

**Reason:** Never used. The notebooks directly use the ForeachWriter pattern with `lakebase.get_foreach_writer()` for more explicit control over streaming writes.

#### 3. Removed `if __name__ == "__main__"` block

**Previously at:** Lines 720-721

**Reason:** Called undefined `main()` function, would cause NameError if executed. Module is only imported, never run directly.

---

## ✅ KEPT (Useful API)

### `utils/lakebase_client.py`

**`get_recent_features()` method** - While unused in current notebooks, this provides a convenient API for users to query recent features for a specific user. Useful for real-time feature serving and ML model inference.

---

## Original Analysis (Pre-Cleanup)

Below is the original detailed analysis that led to the cleanup decisions:

---

## 1. Unused Methods in `utils/lakebase_client.py` (ORIGINAL ANALYSIS)

**Status:** UNUSED - No callers found

**Description:** Alternative streaming write method that takes raw data and columns. This was likely an experimental approach that was superseded by `write_streaming_batch()`.

**Current usage:** The codebase uses `write_streaming_batch()` everywhere instead.

**Location:**
```python
def write_streaming(self, data, columns: list, table_name: str, 
                         batch_size: int = 1):
    """
    Write a streaming micro-batch to Lakebase
    
    This function is designed to be used with foreachBatch in PySpark Structured Streaming
    
    Args:
        data: Values as tuple
        columns: List of column names
        table_name: Target table name
    """
```

**Lines:** 341-373

**Recommendation:** **DELETE** - This method is never called. The project uses `write_streaming_batch()` and the ForeachWriter pattern instead.

**Impact:** None - no code depends on this method.

---

### 1.2 `get_recent_features()` method (Lines 394-414)

**Status:** UNUSED - No callers found

**Description:** Convenience method to get recent features for a specific user within a time window. While potentially useful, it's never actually called in the codebase.

**Location:**
```python
def get_recent_features(self, user_id: str, hours: int = 24,
                       table_name: str = "transaction_features") -> pd.DataFrame:
    """
    Get recent features for a user
    
    Args:
        user_id: User ID to query
        hours: Number of hours to look back
        table_name: Table name
        
    Returns:
        Pandas DataFrame with features
    """
```

**Lines:** 394-414

**Recommendation:** **KEEP** - While unused in current notebooks, this is a useful utility method for real-time feature serving and ML model inference. It provides a simple API that users would likely want to use.

**Impact:** None currently, but removing it would reduce API convenience for end users.

**Alternative:** Keep it as part of the public API for LakebaseClient.

---

## 2. Unused Functions in `utils/lakebase_client.py`

### 2.1 `get_lakebase_client_from_secrets()` function (Lines 491-528)

**Status:** UNUSED - No callers found

**Description:** Creates a LakebaseClient using Databricks secrets for authentication. This was likely part of an earlier authentication approach.

**Current authentication:** The project now uses Databricks SDK's `WorkspaceClient()` and `generate_database_credential()` directly in the `LakebaseClient.__init__()` flow.

**Location:**
```python
def get_lakebase_client_from_secrets(spark=None) -> LakebaseClient:
    """
    Create a LakebaseClient using Databricks secrets
    
    Args:
        spark: SparkSession (for accessing dbutils)
        
    Returns:
        Initialized LakebaseClient
    """
```

**Lines:** 491-528

**Recommendation:** **DELETE** - This function is outdated and uses a deprecated authentication pattern. The current `LakebaseClient.__init__()` already handles authentication properly.

**Impact:** None - no code depends on this function.

---

## 3. Unused Methods in `utils/feature_engineering.py`

### 3.1 `write_features_to_lakebase()` method (Lines 359-388)

**Status:** UNUSED - No callers found

**Description:** Streaming write method that wraps the lakebase client write operation with checkpoint management. This was likely part of an earlier design.

**Current approach:** The notebook `01_streaming_fraud_detection_pipeline.ipynb` directly uses the ForeachWriter pattern with `lakebase.get_foreach_writer()` instead.

**Location:**
```python
def write_features_to_lakebase(self, df, lakebase_client, table_name="transaction_features",
                              checkpoint_location=None, trigger_interval="30 seconds"):
    """
    Write streaming features to Lakebase PostgreSQL database
    
    Args:
        df: Streaming DataFrame with features
        lakebase_client: LakebaseClient instance for PostgreSQL connection
        table_name: Target table name in Lakebase
        checkpoint_location: Checkpoint location for streaming
        trigger_interval: Trigger interval for micro-batches
        
    Returns:
        StreamingQuery object
    """
```

**Lines:** 359-388

**Recommendation:** **DELETE** - This method is never used. The current pattern in the notebooks is clearer and more explicit about the streaming query configuration.

**Impact:** None - no code depends on this method.

---

## 4. Unused Code Blocks

### 4.1 `if __name__ == "__main__"` block in `utils/feature_engineering.py` (Lines 720-721)

**Status:** UNUSED - Incomplete implementation

**Description:** The module has a main block that calls `main()`, but there is no `main()` function defined anywhere in the file.

**Location:**
```python
if __name__ == "__main__":
    main()
```

**Lines:** 720-721

**Recommendation:** **DELETE** - This will cause a NameError if someone tries to run the module directly. Either remove it or implement a proper test/demo `main()` function.

**Impact:** None currently (module is only imported, never executed directly).

---

## 5. Potentially Unused Code (Requires User Confirmation)

### 5.1 `_is_holiday()` method in `utils/feature_engineering.py` (Lines 152-163)

**Status:** USED INTERNALLY - Called by `create_time_based_features()`

**Description:** Simplified holiday detection for US holidays. Only checks for 3 major holidays.

**Recommendation:** **KEEP** - This is used internally, though the implementation is very basic. Consider enhancing it with a proper holiday calendar in the future.

---

## 6. Duplicate Imports

### 6.1 `utils/feature_engineering.py` - Duplicate `builtins` import

**Lines:** 67 and 73

**Location:**
```python
import builtins
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator
import logging
import builtins  # <-- DUPLICATE
```

**Recommendation:** **REMOVE** - Delete the duplicate import on line 73.

**Impact:** None - just cleanup.

---

## Summary of Recommendations

### DELETE (Safe to Remove):

1. **`utils/lakebase_client.py`:**
   - `write_streaming()` method (lines 341-373)
   - `get_lakebase_client_from_secrets()` function (lines 491-528)

2. **`utils/feature_engineering.py`:**
   - `write_features_to_lakebase()` method (lines 359-388)
   - `if __name__ == "__main__"` block (lines 720-721)
   - Duplicate `import builtins` (line 73)

### KEEP (Useful API / Used Internally):

1. **`utils/lakebase_client.py`:**
   - `get_recent_features()` - Useful public API method for feature serving

2. **`utils/feature_engineering.py`:**
   - `_is_holiday()` - Used internally by `create_time_based_features()`

---

## Estimated Impact

**Lines to be removed:** ~60-70 lines  
**Files affected:** 2  
**Breaking changes:** None (only internal/unused code)  
**Risk level:** Low - all identified code is genuinely unused

---

## Testing Recommendations

After removal:
1. Run `00_setup.ipynb` to verify table creation still works
2. Run `01_streaming_fraud_detection_pipeline.ipynb` to verify end-to-end pipeline
3. Test `lakebase.read_features()` method to ensure query functionality works
4. Verify no import errors in any notebooks

---

## Additional Observations

### Code Quality Notes:

1. **Good:** The codebase is generally well-structured with minimal dead code
2. **Good:** Most methods are actually used and serve clear purposes
3. **Improvement:** Consider adding type hints to all public methods
4. **Improvement:** The `__main__` blocks in utility modules should either be removed or properly implemented

### Architecture Notes:

1. The ForeachWriter pattern (used in the current notebooks) is more flexible than the removed `write_features_to_lakebase()` method
2. The authentication flow through `LakebaseClient.__init__()` is cleaner than the removed `get_lakebase_client_from_secrets()` function
3. The separation of concerns (data generation, feature engineering, database client) is well done

---

**Analysis Complete:** This analysis covered all Python modules and notebooks in the project.


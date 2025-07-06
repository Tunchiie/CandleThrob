# gcs.py Documentation

## Overview
The `gcs.py` module provides the core cloud storage interface for the CandleThrob financial data pipeline. It offers a unified API for interacting with Google Cloud Storage (GCS) buckets, handling data uploads, downloads, existence checks, and batch operations with optimized performance and robust error handling.

## Architecture

### Design Philosophy
- **Cloud-First**: Designed specifically for Google Cloud Storage integration
- **DataFrame-Centric**: Optimized for pandas DataFrame operations
- **Parquet Format**: Uses Parquet for efficient columnar storage
- **Memory Efficient**: Streaming operations to minimize memory usage
- **Error Resilient**: Comprehensive error handling and logging

### Authentication Strategy
```python
import google.auth
credentials, project_id = google.auth.default()
```

**Authentication Methods:**
1. **Service Account Key**: GOOGLE_APPLICATION_CREDENTIALS environment variable
2. **Default Credentials**: Google Cloud SDK credentials
3. **Metadata Service**: For Google Cloud environments (Compute Engine, Cloud Run)
4. **Application Default Credentials**: Automatic credential discovery

## Core Functions

### upload_to_gcs(bucket_name, data, destination_blob_name)
**Purpose:** Uploads pandas DataFrame to Google Cloud Storage as Parquet format.

```python
def upload_to_gcs(bucket_name:str, data:pd.DataFrame, destination_blob_name:str):
    """
    Uploads a DataFrame to the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        data (pd.DataFrame): The DataFrame to upload.
        destination_blob_name (str): The destination path in the GCS bucket.
    """
```

**Parameters:**
- `bucket_name` (str): GCS bucket name (e.g., "candlethrob-candata")
- `data` (pd.DataFrame): DataFrame containing financial data
- `destination_blob_name` (str): Full path in bucket (e.g., "raw/tickers/AAPL.parquet")

**Process Flow:**
1. **Client Initialization**: Create authenticated GCS client
2. **Bucket Access**: Get reference to specified bucket
3. **Blob Creation**: Create blob object for destination path
4. **DataFrame Serialization**: Convert DataFrame to Parquet in memory buffer
5. **Upload Operation**: Stream data to GCS with proper content type
6. **Logging**: Record successful upload operation

**Memory Optimization:**
```python
buffer = BytesIO()
data.to_parquet(buffer, index=False)
buffer.seek(0)
blob.upload_from_file(buffer, content_type='application/octet-stream')
```

**Benefits:**
- **Memory Efficient**: Uses BytesIO buffer to avoid temporary files
- **Compressed Storage**: Parquet format provides automatic compression
- **Type Preservation**: Maintains DataFrame data types and schema
- **Atomic Operations**: Single operation upload ensures data consistency

**Error Handling:**
- Network timeouts and retries handled by GCS client library
- Authentication errors logged with clear messages
- Bucket access permissions validated during operation
- Data serialization errors caught and reported

### load_from_gcs(bucket_name, source_blob_name) -> pd.DataFrame
**Purpose:** Downloads and loads DataFrame from Google Cloud Storage.

```python
def load_from_gcs(bucket_name:str, source_blob_name:str) -> pd.DataFrame:
    """
    Loads a DataFrame from the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        source_blob_name (str): The source path in the GCS bucket.
    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
```

**Parameters:**
- `bucket_name` (str): Source GCS bucket name
- `source_blob_name` (str): Full path to file in bucket

**Returns:**
- `pd.DataFrame`: Loaded financial data with original schema preserved

**Process Flow:**
1. **Client Authentication**: Establish authenticated GCS connection
2. **Blob Location**: Get reference to specific file in bucket
3. **Download Operation**: Stream file content to memory buffer
4. **Deserialization**: Convert Parquet bytes back to DataFrame
5. **Schema Validation**: Ensure data types and structure preserved
6. **Logging**: Record successful load operation

**Memory Management:**
```python
buffer = BytesIO()
blob.download_to_file(buffer)
buffer.seek(0)
df = pd.read_parquet(buffer)
```

**Performance Characteristics:**
- **Streaming Download**: No temporary file creation
- **Fast Deserialization**: Parquet format optimized for quick reads
- **Schema Preservation**: Column types and names maintained
- **Memory Efficient**: Direct buffer-to-DataFrame conversion

**Use Cases:**
- Loading historical ticker data for analysis
- Retrieving processed indicator data
- Accessing macroeconomic datasets
- Integration with downstream analytics

### blob_exists(bucket_name, blob_name) -> bool
**Purpose:** Checks existence of files in Google Cloud Storage without downloading.

```python
def blob_exists(bucket_name:str, blob_name:str) -> bool:
    """
    Checks if a file or directory exists in the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        blob_name (str): The name of the blob to check.
    Returns:
        bool: True if the file exists, False otherwise.
    """
```

**Parameters:**
- `bucket_name` (str): GCS bucket to check
- `blob_name` (str): Path/filename to verify

**Returns:**
- `bool`: True if file exists, False otherwise

**Implementation:**
```python
client = storage.Client(credentials=credentials, project=project_id)
bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)
exists = blob.exists()
```

**Benefits:**
- **Efficient Check**: Metadata-only operation, no data transfer
- **Fast Response**: Quick validation without download overhead
- **Conditional Logic**: Enables smart data processing decisions
- **Resource Saving**: Prevents unnecessary download attempts

**Use Cases:**
- **Incremental Updates**: Check if ticker data already exists
- **Data Validation**: Verify successful uploads
- **Pipeline Logic**: Conditional processing based on data availability
- **Error Prevention**: Avoid processing non-existent files

### load_all_gcs_data(bucket_name, prefix) -> list[pd.DataFrame]
**Purpose:** Batch loads multiple DataFrames from a common path prefix.

```python
def load_all_gcs_data(bucket_name:str, prefix:str) -> pd.DataFrame:
    """
    Loads all DataFrames from a specific prefix in the Google Cloud Storage bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        prefix (str): The prefix to filter blobs.
    Returns:
        pd.DataFrame: Concatenated DataFrame of all loaded data.
    """
```

**Parameters:**
- `bucket_name` (str): Source GCS bucket
- `prefix` (str): Path prefix to filter files (e.g., "raw/tickers/")

**Returns:**
- `list[pd.DataFrame]`: List of loaded DataFrames (Note: documentation shows concatenated DataFrame, but code returns list)

**Process Flow:**
1. **Blob Enumeration**: List all blobs matching prefix pattern
2. **File Filtering**: Select only Parquet files for processing
3. **Batch Loading**: Load each matching file as DataFrame
4. **Collection**: Accumulate DataFrames in list for return

**Implementation:**
```python
client = storage.Client(credentials=credentials, project=project_id)
bucket = client.bucket(bucket_name)
blobs = bucket.list_blobs(prefix=prefix)

dataframes = []
for blob in blobs:
    if blob.name.endswith('.parquet'):
        df = load_from_gcs(bucket_name, blob.name)
        dataframes.append(df)
```

**Use Cases:**
- **Bulk Data Loading**: Load all ticker files at once
- **Batch Processing**: Process entire directories of financial data
- **Data Aggregation**: Collect multiple datasets for analysis
- **Pipeline Initialization**: Load complete datasets for transformation

**Performance Considerations:**
- **Memory Usage**: Can consume significant memory with many files
- **Network Bandwidth**: Multiple concurrent downloads
- **Processing Time**: Linear with number of files
- **Optimization**: Consider pagination for very large datasets

## Storage Strategy

### File Organization
```
GCS Bucket Structure:
candlethrob-candata/
├── raw/                    # Raw ingested data
│   ├── tickers/           # Individual S&P 500 stock files
│   │   ├── AAPL.parquet
│   │   ├── GOOGL.parquet
│   │   └── ...
│   ├── etfs/              # ETF data files
│   │   ├── SPY.parquet
│   │   ├── QQQ.parquet
│   │   └── ...
│   └── macros/            # Macroeconomic data
│       └── macro_data.parquet
├── processed/             # Transformed data with indicators
│   ├── tickers/           # Enriched stock data
│   └── etfs/              # Enriched ETF data
└── analysis/              # Analysis outputs
    ├── models/
    └── reports/
```

### Naming Conventions
- **Ticker Files**: `{TICKER}.parquet` (e.g., AAPL.parquet)
- **Macro Data**: `macro_data.parquet`
- **Date-Based**: Optional date prefixes for versioning
- **Path Structure**: Environment/stage/type/file hierarchy

### Data Format Standards
- **File Format**: Apache Parquet for all financial data
- **Compression**: Automatic compression via Parquet
- **Schema**: Consistent column names and types
- **Encoding**: UTF-8 for text fields

## Integration Points

### Upstream Producers
- **ingestion/ingest_data.py**: Saves raw OHLCV data
- **ingestion/transform_data.py**: Saves processed indicator data
- **ingestion/fetch_data.py**: Saves macroeconomic data

### Downstream Consumers
- **analysis/**: Loads data for modeling and analysis
- **dashboard/**: Retrieves data for visualization
- **bot/**: Accesses data for trading decisions

### CI/CD Integration
- **GitHub Actions**: Authenticates using service account secrets
- **Environment Variables**: GOOGLE_APPLICATION_CREDENTIALS for key path
- **Workflow Steps**: Upload/download operations in pipeline

## Authentication Configuration

### Local Development
```bash
# Option 1: Service Account Key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Option 2: Application Default Credentials
gcloud auth application-default login
```

### Production Deployment
```yaml
# GitHub Actions example
- name: Authenticate to Google Cloud
  uses: google-github-actions/auth@v1
  with:
    credentials_json: ${{ secrets.GCP_SA_KEY }}

- name: Set up Cloud SDK
  uses: google-github-actions/setup-gcloud@v1
```

### Service Account Permissions
Required IAM roles for the service account:
- **Storage Object Admin**: Full object management
- **Storage Legacy Bucket Reader**: Bucket listing capability
- Or custom role with specific permissions:
  - `storage.objects.create`
  - `storage.objects.delete`
  - `storage.objects.get`
  - `storage.objects.list`

## Error Handling and Logging

### Logging Configuration
```python
logging.basicConfig(
    filename="utils/debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

**Log Levels:**
- **INFO**: Successful operations and status updates
- **WARNING**: Non-critical issues (retries, etc.)
- **ERROR**: Operation failures requiring attention

**Logged Operations:**
- File upload confirmations with sizes
- Download completions with timing
- Existence check results
- Batch operation summaries

### Common Error Scenarios

#### Authentication Failures
```
google.auth.exceptions.DefaultCredentialsError
```
**Causes:**
- Missing GOOGLE_APPLICATION_CREDENTIALS
- Invalid service account key
- Insufficient permissions

**Solutions:**
- Verify environment variable setup
- Check service account permissions
- Validate key file format and content

#### Network Issues
```
google.api_core.exceptions.RetryError
```
**Causes:**
- Network connectivity problems
- GCS service outages
- Timeout during large file operations

**Solutions:**
- Implement retry logic for transient failures
- Check network connectivity and firewall settings
- Consider chunked uploads for large files

#### Permission Errors
```
google.api_core.exceptions.Forbidden
```
**Causes:**
- Insufficient IAM permissions
- Bucket access restrictions
- Project-level security policies

**Solutions:**
- Review and update service account permissions
- Check bucket-level IAM policies
- Verify project access rights

## Performance Optimization

### Memory Management
```python
# Efficient buffer usage
buffer = BytesIO()
data.to_parquet(buffer, index=False)
buffer.seek(0)
# Buffer automatically cleaned up after upload
```

**Strategies:**
- **Streaming Operations**: No temporary file creation
- **Buffer Management**: Automatic memory cleanup
- **Chunked Processing**: For very large datasets
- **Garbage Collection**: Explicit cleanup when needed

### Network Optimization
- **Compression**: Parquet format provides automatic compression
- **Parallel Operations**: Multiple uploads/downloads when possible
- **Connection Pooling**: Reused connections in batch operations
- **Regional Storage**: Bucket location optimization

### Storage Efficiency
- **Columnar Format**: Parquet optimized for analytical queries
- **Data Types**: Appropriate type selection for space efficiency
- **Compression Codecs**: Snappy compression by default
- **Schema Evolution**: Forward/backward compatibility support

## Usage Examples

### Basic Operations
```python
from utils.gcs import upload_to_gcs, load_from_gcs, blob_exists

# Upload DataFrame
import pandas as pd
df = pd.DataFrame({'price': [100, 101, 102], 'volume': [1000, 1500, 1200]})
upload_to_gcs("candlethrob-candata", df, "test/sample.parquet")

# Check if file exists
if blob_exists("candlethrob-candata", "raw/tickers/AAPL.parquet"):
    # Load existing data
    aapl_data = load_from_gcs("candlethrob-candata", "raw/tickers/AAPL.parquet")
    print(f"Loaded {len(aapl_data)} records for AAPL")
```

### Batch Operations
```python
from utils.gcs import load_all_gcs_data

# Load all ticker files
all_tickers = load_all_gcs_data("candlethrob-candata", "raw/tickers/")
print(f"Loaded {len(all_tickers)} ticker datasets")

# Process each ticker
for ticker_df in all_tickers:
    # Perform analysis on each ticker
    process_ticker_data(ticker_df)
```

### Conditional Processing
```python
from utils.gcs import blob_exists, load_from_gcs, upload_to_gcs

def update_ticker_data(ticker, new_data):
    blob_path = f"raw/tickers/{ticker}.parquet"
    
    if blob_exists("candlethrob-candata", blob_path):
        # Load existing data
        existing_data = load_from_gcs("candlethrob-candata", blob_path)
        # Append new data
        combined_data = pd.concat([existing_data, new_data])
    else:
        # Use new data only
        combined_data = new_data
    
    # Save updated dataset
    upload_to_gcs("candlethrob-candata", combined_data, blob_path)
```

## Best Practices

### Data Management
1. **Consistent Naming**: Use standardized file naming conventions
2. **Schema Consistency**: Maintain consistent DataFrame structures
3. **Version Control**: Consider timestamp or version prefixes for critical data
4. **Backup Strategy**: Implement backup and recovery procedures

### Performance
1. **Batch Operations**: Group related operations for efficiency
2. **Memory Monitoring**: Watch memory usage with large datasets
3. **Network Optimization**: Use appropriate GCS regions
4. **Caching**: Cache frequently accessed data locally when appropriate

### Security
1. **Principle of Least Privilege**: Minimal required permissions
2. **Credential Management**: Secure storage of authentication keys
3. **Access Logging**: Monitor and log data access patterns
4. **Data Encryption**: Use GCS encryption features

### Error Handling
1. **Retry Logic**: Implement retries for transient failures
2. **Graceful Degradation**: Handle failures without crashing
3. **Comprehensive Logging**: Log all operations for debugging
4. **Validation**: Verify data integrity after operations

## Troubleshooting

### Debug Commands
```python
# Test authentication
from google.cloud import storage
client = storage.Client()
buckets = list(client.list_buckets())
print(f"Accessible buckets: {[b.name for b in buckets]}")

# Test specific bucket access
bucket = client.bucket("candlethrob-candata")
blobs = list(bucket.list_blobs(max_results=5))
print(f"Sample files: {[b.name for b in blobs]}")

# Test file operations
from utils.gcs import blob_exists
print(f"Test file exists: {blob_exists('candlethrob-candata', 'test/sample.parquet')}")
```

### Common Issues and Solutions

#### Slow Upload/Download
- Check network connectivity and bandwidth
- Consider GCS bucket location relative to processing location
- Monitor for concurrent operations affecting performance

#### Memory Issues with Large Files
- Implement chunked processing for very large datasets
- Monitor memory usage during operations
- Consider streaming operations for massive files

#### Authentication Problems
- Verify GOOGLE_APPLICATION_CREDENTIALS environment variable
- Check service account key validity and format
- Ensure proper IAM permissions are assigned

## Future Enhancements

### Potential Improvements
1. **Async Operations**: Add async/await support for concurrent operations
2. **Progress Tracking**: Add progress bars for large file operations
3. **Caching Layer**: Implement local caching for frequently accessed data
4. **Data Validation**: Add schema validation and data quality checks
5. **Compression Options**: Support different compression algorithms
6. **Metadata Handling**: Enhanced metadata storage and retrieval

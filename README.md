# MongoDB to S3 Streaming Export Service

A Spring Boot service that efficiently streams MongoDB collections to AWS S3 with support for large datasets, compression, and intelligent upload strategies.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
  - [Upload Strategy Selection](#upload-strategy-selection)
  - [Memory Efficiency](#memory-efficiency)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Configuration Parameters](#configuration-parameters)
- [Usage](#usage)
  - [Service Method](#service-method)
  - [Method Parameters](#method-parameters)
- [Output Formats](#output-formats)
  - [Without Data Wrapper](#without-data-wrapper)
  - [With Data Wrapper](#with-data-wrapper)
- [S3 Object Metadata](#s3-object-metadata)
- [Performance Considerations](#performance-considerations)
  - [Memory Usage](#memory-usage)
  - [Batch Size Optimization](#batch-size-optimization)
  - [Network Considerations](#network-considerations)
- [Error Handling](#error-handling)
  - [Automatic Cleanup](#automatic-cleanup)
  - [Common Issues](#common-issues)
- [Monitoring and Logging](#monitoring-and-logging)
  - [Progress Monitoring](#progress-monitoring)
  - [Log Levels](#log-levels)
- [Security](#security)
  - [AWS Credentials](#aws-credentials)
  - [Required S3 Permissions](#required-s3-permissions)
- [Dependencies](#dependencies)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)
  - [Basic Export](#basic-export)
  - [Filtered Export with Compression](#filtered-export-with-compression)
  - [Scheduled Export](#scheduled-export)
- [API Reference](#api-reference)

---

## Features

- **True Streaming Export**: Processes MongoDB collections without loading entire datasets into memory
- **Intelligent Upload Strategy**: Automatically chooses between single upload and multipart upload based on data size
- **Compression Support**: Optional GZIP compression to reduce storage costs and transfer time
- **Flexible Filtering**: Support for MongoDB query filters to export specific data subsets
- **Data Wrapper**: Optional JSON wrapper for exported data
- **Metadata Tracking**: Comprehensive metadata attached to S3 objects for audit trails
- **Batch Processing**: Configurable batch sizes for optimal memory usage
- **Progress Monitoring**: Real-time logging of export progress
- **Error Handling**: Robust error handling with automatic multipart upload cleanup

[↑ Back to Top](#table-of-contents)

---

## Architecture

### Upload Strategy Selection

The service automatically determines the optimal upload method:

- **Single Upload**: Used for data ≤ 5 MiB (faster, simpler)
- **Multipart Upload**: Used for data > 5 MiB (handles large datasets, supports resume)

### Memory Efficiency

- Streams data in configurable batches to minimize memory footprint
- Uses MongoDB cursors for efficient data retrieval
- Processes compression on-the-fly

[↑ Back to Top](#table-of-contents)

---

## Quick Start

1. **Clone and build** the project
2. **Configure** environment variables (see [Configuration](#configuration))
3. **Run** the Spring Boot application
4. **Call** the export service method

```java
@Autowired
private MongoToS3ExportService exportService;

long recordCount = exportService.exportCollectionToS3Streaming(
    "myCollection", 
    "{}", 
    "exports/data.json", 
    "data", 
    10000, 
    true, 
    true
);
```

[↑ Back to Top](#table-of-contents)

---

## Configuration

### Environment Variables

Configure the following environment variables or Spring Boot properties:

#### MongoDB Configuration
```yaml
spring:
  application:
    name: mongo-to-s3-exporter
  data:
    mongodb:
      uri: ${PRI_MONGODB_URI:mongodb://localhost:27017}
      database: ${PRI_MONGODB_DATABASE:demo-database}
```

#### AWS S3 Configuration
```yaml
aws:
  clientid: ${AWS_ACCESS_KEY_ID:your-access-key}
  secret: ${AWS_SECRET_ACCESS_KEY:your-secret-key}
  s3:
    bucketName: ${AWS_S3_BUCKET_NAME:your-bucket-name}
    bucketRegion: ${AWS_S3_BUCKET_REGION:eu-central-1}
    roleArn: ${AWS_S3_ROLE_ARN:arn:aws:iam::account:role/your-role}
    roleSessionName: ${AWS_S3_ROLE_SESSION_NAME:your-session-name}
```

#### Export Configuration
```yaml
export:
  s3:
    key: ${EXPORT_S3_KEY:test.json}
  collection:
    name: ${EXPORT_COLLECTION_NAME:DemoEntity}
    filter:
      json: ${EXPORT_COLLECTION_FILTER_JSON:'{}'}
  datawrapper:
    key: ${EXPORT_DATAWRAPPER_KEY:dataList}
```

### Configuration Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `PRI_MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017` | `mongodb://user:pass@host:27017/db` |
| `PRI_MONGODB_DATABASE` | MongoDB database name | `my-demo-database` | `production_db` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `dd` | `AKIAIOSFODNN7EXAMPLE` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `dd+OsPUI1/gR5Y2O9Bg` | `ereI/K7MDENG/bPxRfzcvgMPLEKEY` |
| `AWS_S3_BUCKET_NAME` | S3 bucket name | `my-export-bucket` | `my-export-bucket` |
| `AWS_S3_BUCKET_REGION` | AWS region | `eu-central-1` | `us-west-2` |
| `AWS_S3_ROLE_ARN` | IAM role ARN (if using roles) | `arn:aws:iam::12345:role/exporter-cer-access-role` | `arn:aws:iam::123456789012:role/S3AccessRole` |
| `AWS_S3_ROLE_SESSION_NAME` | Role session name | `mongos3streamer` | `export-session` |
| `EXPORT_S3_KEY` | S3 object key (file path) | `test.json` | `exports/users-2024-01-15.json.gz` |
| `EXPORT_COLLECTION_NAME` | MongoDB collection name | `DemoEntity` | `users` |
| `EXPORT_COLLECTION_FILTER_JSON` | MongoDB query filter | `'{}'` | `'{"status": "active", "createdAt": {"$gte": "2024-01-01"}}'` |
| `EXPORT_DATAWRAPPER_KEY` | JSON wrapper key (optional) | `dataList` | `users` |

[↑ Back to Top](#table-of-contents)

---

## Usage

### Service Method

```java
@Autowired
private MongoToS3ExportService exportService;

public void exportData() {
    long recordCount = exportService.exportCollectionToS3Streaming(
        "InsurableEntity",           // collection name
        "{\"status\": \"active\"}",  // filter JSON
        "exports/data.json",         // S3 key
        "dataList",                  // data wrapper key (optional)
        10000,                       // batch size
        true,                        // compression enabled
        true                         // include metadata
    );
    
    log.info("Exported {} records", recordCount);
}
```

### Method Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `collectionName` | String | MongoDB collection name | `"users"` |
| `filterJson` | String | MongoDB query filter (JSON string) | `"{\"status\": \"active\"}"` |
| `s3Key` | String | S3 object key (file path) | `"exports/users.json"` |
| `datawrapperKey` | String | Optional JSON wrapper key | `"users"` (creates `{"users": [...]}`) |
| `batchSize` | int | MongoDB cursor batch size | `10000` |
| `compressionEnabled` | boolean | Enable GZIP compression | `true` |
| `includeMetadata` | boolean | Include export metadata in S3 object | `true` |

[↑ Back to Top](#table-of-contents)

---

## Output Formats

### Without Data Wrapper
```json
{"_id": "507f1f77bcf86cd799439011", "name": "John", "status": "active"}
{"_id": "507f1f77bcf86cd799439012", "name": "Jane", "status": "active"}
```

### With Data Wrapper
When using `datawrapperKey: "users"`:
```json
{
  "users": [
    {"_id": "507f1f77bcf86cd799439011", "name": "John", "status": "active"},
    {"_id": "507f1f77bcf86cd799439012", "name": "Jane", "status": "active"}
  ]
}
```

[↑ Back to Top](#table-of-contents)

---

## S3 Object Metadata

When `includeMetadata` is enabled, the following metadata is attached to S3 objects:

| Key | Description | Example |
|-----|-------------|---------|
| `collection` | Source MongoDB collection | `InsurableEntity` |
| `datawrapperKey` | JSON wrapper key used | `dataList` |
| `exportTime` | Export timestamp | `2024-01-15T10:30:45.123` |
| `filter` | MongoDB filter applied | `{"status": "active"}` |
| `batchSize` | Batch size used | `10000` |
| `compressed` | Whether compression was used | `true` |
| `uploadType` | Upload method used | `single` or `multipart` |
| `format` | Data format | `json-array` |

[↑ Back to Top](#table-of-contents)

---

## Performance Considerations

### Memory Usage
- The service loads all data into memory before upload to determine the optimal upload strategy
- For very large datasets (> 1GB), consider splitting exports by date ranges or other criteria
- Monitor JVM heap size and adjust accordingly

### Batch Size Optimization
- **Small collections (< 100K records)**: Use batch size 5,000-10,000
- **Medium collections (100K-1M records)**: Use batch size 10,000-25,000
- **Large collections (> 1M records)**: Use batch size 25,000-50,000

### Network Considerations
- Multipart uploads provide better reliability for large files
- GZIP compression typically reduces file size by 70-90% for JSON data
- Consider network bandwidth when choosing batch sizes

[↑ Back to Top](#table-of-contents)

---

## Error Handling

### Automatic Cleanup
- Failed multipart uploads are automatically aborted to prevent storage charges
- Comprehensive error logging for troubleshooting

### Common Issues
1. **OutOfMemoryError**: Reduce batch size or split export into smaller chunks
2. **S3 Access Denied**: Verify AWS credentials and S3 bucket permissions
3. **MongoDB Connection Timeout**: Check MongoDB URI and network connectivity
4. **Invalid Filter JSON**: Validate MongoDB query syntax

[↑ Back to Top](#table-of-contents)

---

## Monitoring and Logging

### Progress Monitoring
- Progress logged every 50,000 records processed
- Total record count and data size reported
- Upload method selection logged

### Log Levels
- **INFO**: Progress updates, upload strategy decisions
- **DEBUG**: Detailed part upload information (multipart uploads)
- **ERROR**: Comprehensive error details with stack traces

[↑ Back to Top](#table-of-contents)

---

## Security

### AWS Credentials
- Use IAM roles instead of access keys when possible
- Follow principle of least privilege for S3 permissions
- Rotate credentials regularly

### Required S3 Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::your-bucket-name/*"
    }
  ]
}
```

[↑ Back to Top](#table-of-contents)

---

## Dependencies

### Required Dependencies
```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-mongodb</artifactId>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>s3</artifactId>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
    </dependency>
</dependencies>
```

[↑ Back to Top](#table-of-contents)

---

## Best Practices

1. **Use Compression**: Enable GZIP compression for JSON exports (significant size reduction)
2. **Monitor Memory**: Watch heap usage for large exports
3. **Batch Size Tuning**: Start with 10,000 and adjust based on performance
4. **Error Handling**: Always check return values and handle exceptions
5. **Metadata**: Enable metadata for audit trails and debugging
6. **Filtering**: Use MongoDB indexes on filter fields for better performance
7. **Scheduling**: For regular exports, use Spring @Scheduled annotations
8. **Testing**: Test with small datasets first to validate configuration

[↑ Back to Top](#table-of-contents)

---

## Troubleshooting

### Common Solutions

**Export fails with OutOfMemoryError:**
- Reduce batch size
- Increase JVM heap size: `-Xmx4g`
- Consider splitting large collections by date ranges

**S3 upload fails:**
- Verify AWS credentials and region configuration
- Check S3 bucket permissions
- Ensure bucket exists and is accessible

**MongoDB connection issues:**
- Verify MongoDB URI format and credentials
- Check network connectivity
- Validate database and collection names

**Slow export performance:**
- Add indexes on filter fields
- Increase batch size (if memory allows)
- Consider parallel processing for very large datasets

[↑ Back to Top](#table-of-contents)

---

## Examples

### Basic Export
```java
@Service
public class DataExportService {
    
    @Autowired
    private MongoToS3ExportService exportService;
    
    public void exportUsers() {
        long count = exportService.exportCollectionToS3Streaming(
            "users",                    // collection
            "{}",                       // no filter
            "exports/all-users.json",   // S3 key
            null,                       // no wrapper
            10000,                      // batch size
            false,                      // no compression
            true                        // include metadata
        );
        log.info("Exported {} users", count);
    }
}
```

### Filtered Export with Compression
```java
@Service
public class ActiveUserExportService {
    
    @Autowired
    private MongoToS3ExportService exportService;
    
    public void exportActiveUsers() {
        String filter = "{\"status\": \"active\", \"lastLogin\": {\"$gte\": \"2024-01-01\"}}";
        
        long count = exportService.exportCollectionToS3Streaming(
            "users",                           // collection
            filter,                            // filter active users
            "exports/active-users.json.gz",    // S3 key with .gz extension
            "activeUsers",                     // JSON wrapper
            25000,                             // larger batch size
            true,                              // enable compression
            true                               // include metadata
        );
        
        log.info("Exported {} active users with compression", count);
    }
}
```

### Scheduled Export
```java
@Component
public class ScheduledExportJob {
    
    @Autowired
    private MongoToS3ExportService exportService;
    
    @Scheduled(cron = "0 0 2 * * *") // Daily at 2 AM
    public void dailyUserExport() {
        try {
            String todayKey = "exports/users-" + LocalDate.now() + ".json.gz";
            
            long count = exportService.exportCollectionToS3Streaming(
                "users",
                "{}",
                todayKey,
                "users",
                20000,
                true,
                true
            );
            
            log.info("Daily export completed: {} users exported to {}", count, todayKey);
            
        } catch (Exception e) {
            log.error("Daily export failed", e);
            // Add alerting/notification logic here
        }
    }
}
```

[↑ Back to Top](#table-of-contents)

---

## API Reference

### MongoToS3ExportService

#### Method: `exportCollectionToS3Streaming`

**Signature:**
```java
public long exportCollectionToS3Streaming(
    String collectionName,
    String filterJson,
    String s3Key,
    String datawrapperKey,
    int batchSize,
    boolean compressionEnabled,
    boolean includeMetadata
)
```

**Parameters:**
- `collectionName` (String, required): Name of the MongoDB collection to export
- `filterJson` (String, optional): MongoDB query filter in JSON format. Use `"{}"` for no filter
- `s3Key` (String, required): S3 object key (file path) where data will be stored
- `datawrapperKey` (String, optional): Key name for JSON wrapper. Use `null` for no wrapper
- `batchSize` (int, required): Number of documents to process in each batch
- `compressionEnabled` (boolean, required): Whether to compress the output with GZIP
- `includeMetadata` (boolean, required): Whether to include export metadata in S3 object

**Returns:**
- `long`: Number of records exported

**Throws:**
- `RuntimeException`: If export operation fails (wraps underlying exceptions)

**Example Usage:**
```java
// Export all documents without wrapper
long count1 = exportService.exportCollectionToS3Streaming(
    "products", "{}", "exports/products.json", null, 10000, false, true
);

// Export filtered documents with wrapper and compression
long count2 = exportService.exportCollectionToS3Streaming(
    "orders", 
    "{\"status\": \"completed\"}", 
    "exports/completed-orders.json.gz", 
    "orders", 
    15000, 
    true, 
    true
);
```

[↑ Back to Top](#table-of-contents)

---

**Created with ❤️ for efficient MongoDB to S3 data streaming**

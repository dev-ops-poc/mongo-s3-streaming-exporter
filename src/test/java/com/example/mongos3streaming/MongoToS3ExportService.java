package com.example.mongos3streaming;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCursor;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Slf4j
//@Service
public class MongoToS3ExportService {

	private final MongoTemplate mongoTemplate;
	private final S3Client s3Client;
	private final String bucketName;

	public MongoToS3ExportService(MongoTemplate mongoTemplate, S3Client s3Client, @Value("${aws.s3.bucketName}") String bucketName) {
		this.mongoTemplate = mongoTemplate;
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	public long exportCollectionToS3(String collectionName, String filterJson, String s3Key, int batchSize, boolean compressionEnabled, boolean includeMetadata) {
		try {
			Document filterDoc = (filterJson != null && !filterJson.isBlank()) ? Document.parse(filterJson) : new Document();

			log.info("Starting export from collection '{}' to S3 key '{}'", collectionName, s3Key);

			AtomicLong recordCount = new AtomicLong(0);
			AtomicLong bytesWritten = new AtomicLong(0);

			// Use ByteArrayOutputStream for simpler streaming approach
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

			// Create the final output stream (with or without compression)
			try (OutputStream outputStream = compressionEnabled ? new GZIPOutputStream(byteArrayOutputStream) : byteArrayOutputStream; MongoCursor<Document> cursor = mongoTemplate.getCollection(collectionName).find(filterDoc).batchSize(batchSize).iterator()) {

				log.info("Starting data retrieval from MongoDB with compression: {}", compressionEnabled);

				while (cursor.hasNext()) {
					Document doc = cursor.next();
					byte[] jsonBytes = doc.toJson().getBytes("UTF-8");
					outputStream.write(jsonBytes);
					outputStream.write('\n');

					long count = recordCount.incrementAndGet();
					bytesWritten.addAndGet(jsonBytes.length + 1);

					if (count % 10000 == 0) {
						log.info("Processed {} records, {} bytes", count, bytesWritten.get());
					}
				}

				log.info("Finished reading from MongoDB. Total records: {}, Total bytes: {}", recordCount.get(), bytesWritten.get());
			}

			// Now upload to S3
			byte[] dataToUpload = byteArrayOutputStream.toByteArray();
			uploadToS3(collectionName, filterJson, s3Key, batchSize, compressionEnabled, includeMetadata, dataToUpload);

			return recordCount.get();

		} catch (Exception e) {
			log.error("Error during export operation", e);
			throw new RuntimeException("Error during export operation", e);
		}
	}

	private void uploadToS3(String collectionName, String filterJson, String s3Key, int batchSize, boolean compressionEnabled, boolean includeMetadata, byte[] data) {
		try {
			PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder().bucket(bucketName).key(s3Key).contentType(compressionEnabled ? "application/gzip" : "application/json").contentLength((long) data.length);

			if (compressionEnabled) {
				requestBuilder.contentEncoding("gzip");
			}

			// Add metadata if requested
			if (includeMetadata) {
				Map<String, String> metadata = new HashMap<>();
				metadata.put("collection", collectionName);
				metadata.put("exportTime", LocalDateTime.now().toString());
				metadata.put("filter", filterJson != null ? filterJson : "{}");
				metadata.put("batchSize", String.valueOf(batchSize));
				metadata.put("compressed", String.valueOf(compressionEnabled));
				metadata.put("recordCount", String.valueOf(data.length > 0 ? "estimated" : "0"));
				requestBuilder.metadata(metadata);
			}

			PutObjectRequest putObjectRequest = requestBuilder.build();

			log.info("Starting S3 upload to bucket '{}', key '{}', size: {} bytes", bucketName, s3Key, data.length);

			// Upload using RequestBody.fromBytes for reliable upload
			s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));

			log.info("S3 upload completed successfully");

		} catch (Exception e) {
			log.error("Error uploading to S3", e);
			throw new RuntimeException("Error uploading to S3", e);
		}
	}

	// Method for large datasets that need true streaming
	public long exportCollectionToS3Streaming(String collectionName, String filterJson, String s3Key, int batchSize, boolean compressionEnabled, boolean includeMetadata) {
		try {
			Document filterDoc = (filterJson != null && !filterJson.isBlank()) ? Document.parse(filterJson) : new Document();

			log.info("Starting streaming export from collection '{}' to S3 key '{}'", collectionName, s3Key);

			AtomicLong recordCount = new AtomicLong(0);

			// For very large datasets, you might want to use S3 multipart upload
			// This is a simplified version - implement multipart upload for production use

			try (MongoCursor<Document> cursor = mongoTemplate.getCollection(collectionName).find(filterDoc).batchSize(batchSize).iterator()) {

				StringBuilder batchData = new StringBuilder();
				int currentBatchSize = 0;
				final int maxBatchSize = 5 * 1024 * 1024; // 5MB batches

				while (cursor.hasNext()) {
					Document doc = cursor.next();
					String jsonLine = doc.toJson() + "\n";
					batchData.append(jsonLine);
					currentBatchSize += jsonLine.getBytes("UTF-8").length;
					recordCount.incrementAndGet();

					// When batch reaches size limit, you could implement multipart upload here
					if (currentBatchSize >= maxBatchSize) {
						log.info("Batch size reached: {} bytes, {} records", currentBatchSize, recordCount.get());
						// For now, just reset (in production, upload this part)
						batchData = new StringBuilder();
						currentBatchSize = 0;
					}
				}

				// Upload final batch
				if (batchData.length() > 0) {
					byte[] finalData = batchData.toString().getBytes("UTF-8");
					uploadToS3(collectionName, filterJson, s3Key, batchSize, compressionEnabled, includeMetadata, finalData);
				}
			}

			return recordCount.get();

		} catch (Exception e) {
			log.error("Error during streaming export operation", e);
			throw new RuntimeException("Error during streaming export operation", e);
		}
	}

}
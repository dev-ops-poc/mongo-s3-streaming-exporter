package com.example.mongos3streaming.service;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@Slf4j
@Service
public class MongoToS3ExportService {

	private final MongoTemplate mongoTemplate;
	private final S3Client s3Client;
	private final String bucketName;

	// AWS S3 multipart upload constraints
	private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB minimum (except last part)
	private static final int MAX_PART_SIZE = 100 * 1024 * 1024; // 100 MiB maximum for this implementation
	private static final int SINGLE_UPLOAD_THRESHOLD = MIN_PART_SIZE; // Use single upload if total size is less than 5 MiB

	public MongoToS3ExportService(MongoTemplate mongoTemplate, S3Client s3Client, @Value("${aws.s3.bucketName}") String bucketName) {
		this.mongoTemplate = mongoTemplate;
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	// True streaming method with multipart upload for large datasets
	public long exportCollectionToS3Streaming(String collectionName, String filterJson, String s3Key, String datawrapperKey, int batchSize, boolean compressionEnabled, boolean includeMetadata) {
		ByteArrayOutputStream totalBuffer = new ByteArrayOutputStream();

		boolean hasDataWrapper = datawrapperKey != null && !datawrapperKey.isBlank();
		try {
			Document filterDoc = (filterJson != null && !filterJson.isBlank()) ? Document.parse(filterJson) : new Document();

			log.info("Starting streaming export from collection '{}' to S3 key '{}'", collectionName, s3Key);

			AtomicLong recordCount = new AtomicLong(0);

			// First pass: collect all data to determine if we need multipart upload
			try (MongoCursor<Document> cursor = mongoTemplate.getCollection(collectionName).find(filterDoc).batchSize(batchSize).iterator()) {

				OutputStream tempOutputStream = compressionEnabled ? new GZIPOutputStream(totalBuffer) : totalBuffer;

				// Start JSON structure
				if (hasDataWrapper) {
					byte[] jsonStart = ("{\"" + datawrapperKey + "\":[").getBytes(StandardCharsets.UTF_8);
					tempOutputStream.write(jsonStart);
				}

				boolean isFirstRecord = true;
				while (cursor.hasNext()) {
					Document doc = cursor.next();

					// Prepare the record with comma if needed
					StringBuilder recordBuilder = new StringBuilder();
					if (!isFirstRecord && hasDataWrapper) {
						recordBuilder.append(",");
					}
					recordBuilder.append(doc.toJson());

					byte[] jsonBytes = recordBuilder.toString().getBytes(StandardCharsets.UTF_8);
					tempOutputStream.write(jsonBytes);

					recordCount.incrementAndGet();
					isFirstRecord = false;

					if (recordCount.get() % 50000 == 0) {
						log.info("Processed {} records", recordCount.get());
					}
				}

				// Close JSON structure
				if (hasDataWrapper) {
					tempOutputStream.write("]}".getBytes(StandardCharsets.UTF_8));
				}
				tempOutputStream.close();
			}

			byte[] allData = totalBuffer.toByteArray();
			log.info("Total data size: {} bytes, records: {}", allData.length, recordCount.get());

			// Decide between single upload or multipart upload
			if (allData.length <= SINGLE_UPLOAD_THRESHOLD) {
				log.info("Using single upload for small data size: {} bytes", allData.length);
				uploadSingleObject(collectionName, datawrapperKey, filterJson, s3Key, batchSize, compressionEnabled, includeMetadata, allData);
			} else {
				log.info("Using multipart upload for large data size: {} bytes", allData.length);
				uploadMultipart(collectionName, datawrapperKey, filterJson, s3Key, batchSize, compressionEnabled, includeMetadata, allData);
			}

			return recordCount.get();

		} catch (Exception e) {
			log.error("Error during streaming export operation", e);
			throw new RuntimeException("Error during streaming export operation", e);
		}
	}

	private void uploadSingleObject(String collectionName, String datawrapperKey, String filterJson, String s3Key, int batchSize, boolean compressionEnabled, boolean includeMetadata, byte[] data) {
		try {
			PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder().bucket(bucketName).key(s3Key).contentType("application/json").contentLength((long) data.length);

			if (compressionEnabled) {
				requestBuilder.contentEncoding("gzip");
			}

			// Add metadata if requested
			if (includeMetadata) {
				Map<String, String> metadata = createMetadata(collectionName, datawrapperKey, filterJson, batchSize, compressionEnabled);
				metadata.put("uploadType", "single");
				requestBuilder.metadata(metadata);
			}

			PutObjectRequest putObjectRequest = requestBuilder.build();

			log.info("Starting single S3 upload to bucket '{}', key '{}', size: {} bytes", bucketName, s3Key, data.length);

			s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));

			log.info("Single S3 upload completed successfully");

		} catch (Exception e) {
			log.error("Error uploading single object to S3", e);
			throw new RuntimeException("Error uploading single object to S3", e);
		}
	}

	private void uploadMultipart(String collectionName, String datawrapperKey, String filterJson, String s3Key, int batchSize, boolean compressionEnabled, boolean includeMetadata, byte[] allData) {
		String uploadId = null;
		List<CompletedPart> completedParts = new ArrayList<>();

		try {
			// Initialize multipart upload
			CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder().bucket(bucketName).key(s3Key).contentType("application/json").contentEncoding(compressionEnabled ? "gzip" : null).metadata(includeMetadata ? createMetadata(collectionName, datawrapperKey, filterJson, batchSize, compressionEnabled) : null).build();

			CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(createRequest);
			uploadId = createResponse.uploadId();

			log.info("Initiated multipart upload with ID: {}", uploadId);

			// Split data into parts ensuring minimum size requirement
			int totalSize = allData.length;
			int partNumber = 1;
			int offset = 0;

			while (offset < totalSize) {
				int remainingBytes = totalSize - offset;
				int partSize;

				// Calculate part size ensuring AWS requirements
				if (remainingBytes <= MAX_PART_SIZE) {
					// This is the last part - can be any size
					partSize = remainingBytes;
				} else {
					// Not the last part - ensure it's at least MIN_PART_SIZE
					partSize = Math.min(MAX_PART_SIZE, Math.max(MIN_PART_SIZE, remainingBytes / 2));

					// Ensure we don't create a next part that would be smaller than MIN_PART_SIZE
					// unless it would be the final part
					int nextPartSize = remainingBytes - partSize;
					if (nextPartSize > 0 && nextPartSize < MIN_PART_SIZE) {
						// Adjust current part size to make next part either >= MIN_PART_SIZE or make
						// this the last part
						partSize = remainingBytes; // Make this the last part
					}
				}

				// Create part data
				byte[] partData = new byte[partSize];
				System.arraycopy(allData, offset, partData, 0, partSize);

				// Upload part
				CompletedPart completedPart = uploadPart(uploadId, s3Key, partNumber, partData);
				completedParts.add(completedPart);

				log.info("Uploaded part {} of size {} bytes (offset: {}, remaining: {})", partNumber, partSize, offset, remainingBytes - partSize);

				offset += partSize;
				partNumber++;
			}

			// Complete multipart upload
			CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder().bucket(bucketName).key(s3Key).uploadId(uploadId).multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build();

			s3Client.completeMultipartUpload(completeRequest);
			log.info("Successfully completed multipart upload with {} parts", completedParts.size());

		} catch (Exception e) {
			log.error("Error during multipart upload", e);

			// Abort multipart upload on error
			if (uploadId != null) {
				try {
					AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder().bucket(bucketName).key(s3Key).uploadId(uploadId).build();
					s3Client.abortMultipartUpload(abortRequest);
					log.info("Aborted multipart upload: {}", uploadId);
				} catch (Exception abortException) {
					log.error("Failed to abort multipart upload: {}", uploadId, abortException);
				}
			}

			throw new RuntimeException("Error during multipart upload", e);
		}
	}

	private CompletedPart uploadPart(String uploadId, String s3Key, int partNumber, byte[] partData) {
		try {
			UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(bucketName).key(s3Key).uploadId(uploadId).partNumber(partNumber).contentLength((long) partData.length).build();

			String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(partData)).eTag();

			log.debug("Uploaded part {} with ETag: {}, size: {} bytes", partNumber, etag, partData.length);

			return CompletedPart.builder().partNumber(partNumber).eTag(etag).build();

		} catch (Exception e) {
			log.error("Error uploading part {}", partNumber, e);
			throw new RuntimeException("Error uploading part " + partNumber, e);
		}
	}

	private Map<String, String> createMetadata(String collectionName, String datawrapperKey, String filterJson, int batchSize, boolean compressionEnabled) {
		Map<String, String> metadata = new HashMap<>();
		metadata.put("collection", collectionName);
		metadata.put("datawrapperKey", datawrapperKey);
		metadata.put("exportTime", LocalDateTime.now().toString());
		metadata.put("filter", filterJson != null ? filterJson : "{}");
		metadata.put("batchSize", String.valueOf(batchSize));
		metadata.put("compressed", String.valueOf(compressionEnabled));
		metadata.put("uploadType", "multipart");
		metadata.put("format", "json-array");
		return metadata;
	}
}
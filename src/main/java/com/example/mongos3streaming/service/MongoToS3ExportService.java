package com.example.mongos3streaming.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@Slf4j
@Service
public class MongoToS3ExportService {

	private final MongoTemplate mongoTemplate;
	private final S3Client s3Client;
	private final String bucketName;

	// AWS S3 multipart upload constraints
	private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB minimum (except last part)
	private static final int BUFFER_SIZE = 10 * 1024 * 1024; // 10 MiB buffer for accumulating data

	public MongoToS3ExportService(MongoTemplate mongoTemplate, S3Client s3Client, @Value("${aws.s3.bucketName}") String bucketName) {
		this.mongoTemplate = mongoTemplate;
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	public long exportCollectionToS3Streaming(String collectionName, String filterJson, String s3Key, String datawrapperKey, int batchSize, boolean compressionEnabled, boolean includeMetadata) {
		String uploadId = null;
		List<CompletedPart> completedParts = new ArrayList<>();
		AtomicLong recordCount = new AtomicLong(0);

		try {
			Document filterDoc = (filterJson != null && !filterJson.isBlank()) ? Document.parse(filterJson) : new Document();

			log.info("Starting true streaming export from collection '{}' to S3 key '{}'", collectionName, s3Key);

			// Always use multipart upload for true streaming
			uploadId = initializeMultipartUpload(s3Key, compressionEnabled, includeMetadata, collectionName, datawrapperKey, filterJson, batchSize);

			// Stream data in chunks
			try (MongoCursor<Document> cursor = mongoTemplate.getCollection(collectionName).find(filterDoc).batchSize(batchSize).iterator()) {

				StreamingBuffer buffer = new StreamingBuffer(compressionEnabled, BUFFER_SIZE);
				boolean hasDataWrapper = datawrapperKey != null && !datawrapperKey.isBlank();
				int partNumber = 1;

				// Start JSON structure
				if (hasDataWrapper) {
					buffer.write(("{\"" + datawrapperKey + "\":[").getBytes(StandardCharsets.UTF_8));
				}

				boolean isFirstRecord = true;
				while (cursor.hasNext()) {
					Document doc = cursor.next();

					// Prepare the record
					StringBuilder recordBuilder = new StringBuilder();
					if (!isFirstRecord && hasDataWrapper) {
						recordBuilder.append(",");
					}
					recordBuilder.append(doc.toJson());

					byte[] jsonBytes = recordBuilder.toString().getBytes(StandardCharsets.UTF_8);
					buffer.write(jsonBytes);

					recordCount.incrementAndGet();
					isFirstRecord = false;

					// Check if buffer is ready for upload
					if (buffer.size() >= MIN_PART_SIZE) {
						byte[] partData = buffer.getAndReset();
						CompletedPart part = uploadPart(uploadId, s3Key, partNumber, partData);
						completedParts.add(part);
						partNumber++;

						log.info("Uploaded part {} (size: {} bytes) after processing {} records", partNumber - 1, partData.length, recordCount.get());
					}

					if (recordCount.get() % 50000 == 0) {
						log.info("Processed {} records", recordCount.get());
					}
				}

				// Close JSON structure
				if (hasDataWrapper) {
					buffer.write("]}".getBytes(StandardCharsets.UTF_8));
				}

				// Upload final part if there's remaining data
				byte[] finalData = buffer.getAndClose();
				if (finalData.length > 0) {
					CompletedPart finalPart = uploadPart(uploadId, s3Key, partNumber, finalData);
					completedParts.add(finalPart);
					log.info("Uploaded final part {} (size: {} bytes)", partNumber, finalData.length);
				}
			}

			// Complete multipart upload
			completeMultipartUpload(uploadId, s3Key, completedParts);
			log.info("Successfully completed streaming export with {} parts, {} total records", completedParts.size(), recordCount.get());

			return recordCount.get();

		} catch (Exception e) {
			log.error("Error during streaming export operation", e);
			abortMultipartUpload(uploadId, s3Key);
			throw new RuntimeException("Error during streaming export operation", e);
		}
	}

	private String initializeMultipartUpload(String s3Key, boolean compressionEnabled, boolean includeMetadata, String collectionName, String datawrapperKey, String filterJson, int batchSize) {
		CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder().bucket(bucketName).key(s3Key).contentType("application/json");

		if (compressionEnabled) {
			builder.contentEncoding("gzip");
		}

		if (includeMetadata) {
			builder.metadata(createMetadata(collectionName, datawrapperKey, filterJson, batchSize, compressionEnabled));
		}

		CreateMultipartUploadResponse response = s3Client.createMultipartUpload(builder.build());
		String uploadId = response.uploadId();
		log.info("Initiated multipart upload with ID: {}", uploadId);
		return uploadId;
	}

	private CompletedPart uploadPart(String uploadId, String s3Key, int partNumber, byte[] partData) {
		try {
			UploadPartRequest request = UploadPartRequest.builder().bucket(bucketName).key(s3Key).uploadId(uploadId).partNumber(partNumber).contentLength((long) partData.length).build();

			String etag = s3Client.uploadPart(request, RequestBody.fromBytes(partData)).eTag();
			log.debug("Uploaded part {} with ETag: {}, size: {} bytes", partNumber, etag, partData.length);

			return CompletedPart.builder().partNumber(partNumber).eTag(etag).build();

		} catch (Exception e) {
			log.error("Error uploading part {}", partNumber, e);
			throw new RuntimeException("Error uploading part " + partNumber, e);
		}
	}

	private void completeMultipartUpload(String uploadId, String s3Key, List<CompletedPart> completedParts) {
		CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().bucket(bucketName).key(s3Key).uploadId(uploadId).multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build();

		s3Client.completeMultipartUpload(request);
	}

	private void abortMultipartUpload(String uploadId, String s3Key) {
		if (uploadId != null) {
			try {
				AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder().bucket(bucketName).key(s3Key).uploadId(uploadId).build();
				s3Client.abortMultipartUpload(request);
				log.info("Aborted multipart upload: {}", uploadId);
			} catch (Exception e) {
				log.error("Failed to abort multipart upload: {}", uploadId, e);
			}
		}
	}

	private Map<String, String> createMetadata(String collectionName, String datawrapperKey, String filterJson, int batchSize, boolean compressionEnabled) {
		Map<String, String> metadata = new HashMap<>();
		metadata.put("collection", collectionName);
		metadata.put("datawrapperKey", datawrapperKey != null ? datawrapperKey : "");
		metadata.put("exportTime", LocalDateTime.now().toString());
		metadata.put("filter", filterJson != null ? filterJson : "{}");
		metadata.put("batchSize", String.valueOf(batchSize));
		metadata.put("compressed", String.valueOf(compressionEnabled));
		metadata.put("uploadType", "streaming-multipart");
		metadata.put("format", "json-array");
		return metadata;
	}

	// Helper class for streaming buffer management
	private static class StreamingBuffer {
		private ByteArrayOutputStream buffer;
		private OutputStream outputStream;
		private final boolean compressionEnabled;

		public StreamingBuffer(boolean compressionEnabled, int maxSize) throws IOException {
			this.compressionEnabled = compressionEnabled;
			reset();
		}

		private void reset() throws IOException {
			if (outputStream != null) {
				outputStream.close();
			}
			buffer = new ByteArrayOutputStream();
			outputStream = compressionEnabled ? new GZIPOutputStream(buffer) : buffer;
		}

		public void write(byte[] data) throws IOException {
			outputStream.write(data);
		}

		public int size() {
			return buffer.size();
		}

		public byte[] getAndReset() throws IOException {
			outputStream.flush();
			if (compressionEnabled) {
				((GZIPOutputStream) outputStream).finish();
			}
			byte[] data = buffer.toByteArray();
			reset();
			return data;
		}

		public byte[] getAndClose() throws IOException {
			outputStream.flush();
			if (compressionEnabled) {
				((GZIPOutputStream) outputStream).finish();
			}
			outputStream.close();
			return buffer.toByteArray();
		}
	}
}
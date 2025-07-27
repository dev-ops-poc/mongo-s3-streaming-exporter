package com.example.mongos3streaming.runner;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.example.mongos3streaming.service.MongoToS3ExportService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MongoS3ExportRunner implements CommandLineRunner {

	@Autowired
	private MongoToS3ExportService exportService;

	@Value("${export.collection.name}")
	private String collectionName;

	@Value("${export.filter.json:}")
	private String filterJson;

	@Value("${export.s3.key:}")
	private String s3Key;

	@Value("${export.batch.size:1000}")
	private int batchSize;

	@Value("${export.buffer.size:8192}")
	private int bufferSize;

	@Value("${export.compression.enabled:false}")
	private boolean compressionEnabled;

	@Value("${export.include.metadata:true}")
	private boolean includeMetadata;
	
	@Value("${export.datawrapper.key:dataList}")
	private String datawrapperKey;
	
	@Override
	public void run(String... args) throws Exception {

		log.info("""
				
				==================================================
				🟢 Starting MongoDB to S3 Export Job
				--------------------------------------------------
				📁 Collection       : {}
				📦 S3 Key           : {}
				📦 Batch Size       : {}
				🧠 Buffer Size      : {}
				📦 Compression      : {}
				🔑 Data Wrapper Key : {}
				📝 Include Metadata : {}
				🔍 Filter           : {}
				==================================================
				""", collectionName, s3Key, batchSize, bufferSize, compressionEnabled, datawrapperKey, includeMetadata, filterJson != null ? filterJson : "{}");
		try {
			// Validate required parameters
			if (collectionName == null || collectionName.isBlank()) {
				throw new IllegalArgumentException("Collection name is required. Set export.collection.name property or EXPORT_COLLECTION_NAME environment variable.");
			}
			if (s3Key == null || s3Key.isBlank()) {
				String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
				String extension = compressionEnabled ? ".json.gz" : ".json";
				s3Key = String.format("exports/%s/%s_%s%s", collectionName, collectionName, timestamp, extension);
			}
			// Execute export
			long startTime = System.currentTimeMillis();
			long recordsExported = exportService.exportCollectionToS3Streaming(collectionName, filterJson, s3Key, datawrapperKey, batchSize, compressionEnabled,  includeMetadata);
			long duration = System.currentTimeMillis() - startTime;

			log.info("Export completed successfully. Records exported: {}, Duration: {}ms", recordsExported, duration);

		} catch (Exception e) {
			log.error("Export job failed", e);
			System.exit(1);
		}
	}

}
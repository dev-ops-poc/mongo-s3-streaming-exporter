package com.example.mongos3streaming.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

@Configuration
public class S3Configuration {

	@Value("${aws.clientid}")
	private String accessKeyId;

	@Value("${aws.secret}")
	private String secretAccessKey;

	@Value("${aws.s3.bucketRegion}")
	private String bucketRegion;

	@Value("${aws.s3.roleArn}")
	private String roleArn;

	@Value("${aws.s3.roleSessionName}")
	private String roleSessionName;

	@Bean
	public S3Client amazonS3() {
		Region region = Region.of(bucketRegion);
		AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);

		// Check if roleArn is provided and not empty
		if (roleArn != null && !roleArn.trim().isEmpty()) {
			// Use STS role assumption
			StsClient stsClient = StsClient.builder().region(region).credentialsProvider(StaticCredentialsProvider.create(basicCredentials)).build();

			AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(roleSessionName).build();

			AwsCredentialsProvider credentialsProvider = StsAssumeRoleCredentialsProvider.builder().stsClient(stsClient).refreshRequest(assumeRoleRequest).build();

			return S3Client.builder().region(region).credentialsProvider(credentialsProvider).build();
		} else {
			// Use basic credentials directly (no role assumption)
			return S3Client.builder().region(region).credentialsProvider(StaticCredentialsProvider.create(basicCredentials)).build();
		}
	}
}

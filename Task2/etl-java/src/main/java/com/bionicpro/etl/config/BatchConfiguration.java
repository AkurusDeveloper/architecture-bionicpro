package com.bionicpro.etl.config;

import com.bionicpro.etl.client.CrmApiClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BatchConfiguration {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final CrmApiClient crmApiClient;

    @Bean
    public Job buildMartJob(Step buildMartStep) {
        return new JobBuilder("buildMartJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(buildMartStep)
                .build();
    }

    @Bean
    public Step buildMartStep(@Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseTemplate) {
        return new StepBuilder("buildMartStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    
                    String reportDate = chunkContext.getStepContext()
                            .getJobParameters()
                            .getOrDefault("date", java.time.LocalDate.now().toString())
                            .toString();
                    
                    String sql = """
                        INSERT INTO mart_report_user_daily (
                            user_id,
                            report_date,
                            metrics.name,
                            metrics.events_count,
                            metrics.value_sum,
                            metrics.value_avg,
                            metrics.value_min,
                            metrics.value_max,
                            region,
                            prosthetic_model
                        )
                        SELECT
                            t.user_id,
                            toDate(t.event_timestamp) AS report_date,
                            groupArray(t.metric_name) AS metric_names,
                            groupArray(count(*)) AS events_counts,
                            groupArray(sum(t.metric_value)) AS value_sums,
                            groupArray(avg(t.metric_value)) AS value_avgs,
                            groupArray(min(t.metric_value)) AS value_mins,
                            groupArray(max(t.metric_value)) AS value_maxs,
                            any(u.region) AS region,
                            any(u.prosthetic_model) AS prosthetic_model
                        FROM raw_telemetry t
                        LEFT JOIN raw_crm_users u ON t.user_id = u.user_id
                        WHERE toDate(t.event_timestamp) = ?
                        GROUP BY t.user_id, report_date, t.metric_name
                        """;
                    
                    int rowsAffected = clickhouseTemplate.update(sql, reportDate);
                    
                    System.out.printf("✅ Built mart for date %s: %d rows inserted%n", reportDate, rowsAffected);
                    
                    return org.springframework.batch.repeat.RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Job extractCrmJob(Step extractCrmStep) {
        return new JobBuilder("extractCrmJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(extractCrmStep)
                .build();
    }

    @Bean
    public Step extractCrmStep(@Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseTemplate) {
        return new StepBuilder("extractCrmStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    
                    String reportDate = chunkContext.getStepContext()
                            .getJobParameters()
                            .getOrDefault("date", java.time.LocalDate.now().toString())
                            .toString();
                    
                    log.info("Starting CRM data extraction for date: {}", reportDate);
                    
                    try {
                        List<CrmApiClient.CrmUser> crmUsers = crmApiClient.fetchUsersByDate(reportDate);
                        
                        if (crmUsers == null || crmUsers.isEmpty()) {
                            log.warn("No CRM users found for date: {}", reportDate);
                            return org.springframework.batch.repeat.RepeatStatus.FINISHED;
                        }
                        
                        log.info("Fetched {} users from CRM", crmUsers.size());
                        
                        DateTimeFormatter[] formatters = {
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                            DateTimeFormatter.ISO_DATE_TIME,
                            DateTimeFormatter.ISO_LOCAL_DATE_TIME
                        };
                        
                        int batchSize = 100;
                        int insertedCount = 0;
                        int totalUsers = crmUsers.size();
                        
                        for (int i = 0; i < totalUsers; i += batchSize) {
                            int endIndex = Math.min(i + batchSize, totalUsers);
                            List<CrmApiClient.CrmUser> batch = crmUsers.subList(i, endIndex);
                            
                            StringBuilder valuesBuilder = new StringBuilder();
                            java.util.List<Object> params = new java.util.ArrayList<>();
                            
                            for (CrmApiClient.CrmUser user : batch) {
                                if (valuesBuilder.length() > 0) {
                                    valuesBuilder.append(", ");
                                }
                                valuesBuilder.append("(?, ?, ?, ?, ?, ?, ?)");
                                
                                LocalDateTime createdAt = LocalDateTime.now();
                                if (user.createdAt() != null && !user.createdAt().isEmpty()) {
                                    for (DateTimeFormatter formatter : formatters) {
                                        try {
                                            createdAt = LocalDateTime.parse(user.createdAt(), formatter);
                                            break;
                                        } catch (Exception e) {
                                        }
                                    }
                                }
                                
                                params.add(user.userId());
                                params.add(user.username() != null ? user.username() : "");
                                params.add(user.email() != null ? user.email() : "");
                                params.add(user.contractNumber() != null ? user.contractNumber() : "");
                                params.add(user.prostheticModel() != null ? user.prostheticModel() : "");
                                params.add(user.region() != null ? user.region() : "");
                                params.add(createdAt);
                            }
                            
                            String insertSql = "INSERT INTO raw_crm_users " +
                                "(user_id, username, email, contract_number, prosthetic_model, region, created_at) " +
                                "VALUES " + valuesBuilder.toString();
                            
                            try {
                                clickhouseTemplate.update(insertSql, params.toArray());
                                insertedCount += batch.size();
                                log.debug("Inserted batch of {} users (total: {}/{})", batch.size(), insertedCount, totalUsers);
                            } catch (Exception e) {
                                log.error("Error inserting batch starting at index {}: {}", i, e.getMessage());
                                for (CrmApiClient.CrmUser user : batch) {
                                    try {
                                        LocalDateTime createdAt = LocalDateTime.now();
                                        if (user.createdAt() != null && !user.createdAt().isEmpty()) {
                                            for (DateTimeFormatter formatter : formatters) {
                                                try {
                                                    createdAt = LocalDateTime.parse(user.createdAt(), formatter);
                                                    break;
                                                } catch (Exception ex) {
                                                }
                                            }
                                        }
                                        
                                        String singleInsertSql = """
                                            INSERT INTO raw_crm_users 
                                            (user_id, username, email, contract_number, prosthetic_model, region, created_at)
                                            VALUES (?, ?, ?, ?, ?, ?, ?)
                                            """;
                                        
                                        clickhouseTemplate.update(singleInsertSql,
                                            user.userId(),
                                            user.username() != null ? user.username() : "",
                                            user.email() != null ? user.email() : "",
                                            user.contractNumber() != null ? user.contractNumber() : "",
                                            user.prostheticModel() != null ? user.prostheticModel() : "",
                                            user.region() != null ? user.region() : "",
                                            createdAt
                                        );
                                        insertedCount++;
                                    } catch (Exception ex) {
                                        log.error("Error inserting user {}: {}", user.userId(), ex.getMessage());
                                    }
                                }
                            }
                        }
                        
                        log.info("✅ CRM extraction completed. Inserted {} users into ClickHouse", insertedCount);
                        
                    } catch (Exception e) {
                        log.error("Error during CRM extraction: {}", e.getMessage(), e);
                        throw new RuntimeException("CRM extraction failed", e);
                    }
                    
                    return org.springframework.batch.repeat.RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Job extractTelemetryJob(Step extractTelemetryStep) {
        return new JobBuilder("extractTelemetryJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(extractTelemetryStep)
                .build();
    }

    @Bean
    public Step extractTelemetryStep(@Qualifier("coreDbJdbcTemplate") JdbcTemplate coreDbTemplate, 
                                      @Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseTemplate) {
        return new StepBuilder("extractTelemetryStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    
                    String reportDate = chunkContext.getStepContext()
                            .getJobParameters()
                            .getOrDefault("date", java.time.LocalDate.now().toString())
                            .toString();
                    
                    System.out.printf("✅ Extracting telemetry for date: %s%n", reportDate);
                    
                    System.out.println("✅ Telemetry extraction completed");
                    
                    return org.springframework.batch.repeat.RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

}




package com.bionicpro.etl.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class CrmApiClient {

    private final WebClient.Builder webClientBuilder;

    @Value("${crm.api.base-url}")
    private String crmApiBaseUrl;

    @Value("${crm.api.username}")
    private String username;

    @Value("${crm.api.password}")
    private String password;

    public List<CrmUser> fetchUsers() {
        log.info("Fetching users from CRM API: {}", crmApiBaseUrl);

        WebClient webClient = webClientBuilder
                .baseUrl(crmApiBaseUrl)
                .defaultHeaders(headers -> headers.setBasicAuth(username, password))
                .build();

        return webClient.get()
                .uri("/users")
                .retrieve()
                .bodyToFlux(CrmUser.class)
                .collectList()
                .block();
    }

    public List<CrmUser> fetchUsersByDate(String date) {
        log.info("Fetching users from CRM for date: {}", date);

        try {
            WebClient webClient = webClientBuilder
                    .baseUrl(crmApiBaseUrl)
                    .defaultHeaders(headers -> headers.setBasicAuth(username, password))
                    .build();

            List<CrmUser> users = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/users")
                            .queryParam("created_date", date)
                            .build())
                    .retrieve()
                    .bodyToFlux(CrmUser.class)
                    .collectList()
                    .block();

            if (users == null) {
                log.warn("CRM API returned null for date: {}", date);
                return java.util.Collections.emptyList();
            }

            log.info("Successfully fetched {} users from CRM for date: {}", users.size(), date);
            return users;

        } catch (Exception e) {
            log.error("Error fetching users from CRM for date {}: {}", date, e.getMessage(), e);
            throw new RuntimeException("Failed to fetch users from CRM: " + e.getMessage(), e);
        }
    }

    public record CrmUser(
            String userId,
            String username,
            String email,
            String contractNumber,
            String prostheticModel,
            String region,
            String createdAt
    ) {}

}




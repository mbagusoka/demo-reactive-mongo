package com.example;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ReactiveAuditorAware;
import org.springframework.data.mongodb.config.EnableReactiveMongoAuditing;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

interface PersonRepository extends ReactiveMongoRepository<Person, String> {

    @Query("{ id: { $exists: true }}")
    Flux<Person> retrieveAll(Pageable pageable);
}

@SpringBootApplication
public class DemoReactiveMongoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoReactiveMongoApplication.class, args);
    }
}

@Component
@RequiredArgsConstructor
@Log4j2
class Initializer {

    private final PersonRepository personRepository;

    @EventListener(ApplicationReadyEvent.class)
    public void initDb() {
        personRepository.deleteAll()
            .thenMany(Flux.fromStream(IntStream.range(0, 100).mapToObj(n -> "Person:" + n))
                .map(Person::new)
                .flatMap(person -> {
                    log.info(() -> "Save with thread: " + Thread.currentThread());

                    return personRepository.save(person);
                })
            )
            .subscribe();
    }
}

@Document("persons")
@Data
@EqualsAndHashCode(of = "id", callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
class Person extends AuditEntity implements Serializable {

    private static final long serialVersionUID = -7540201601713475435L;

    @Id
    private String id;

    private String name;

    public Person(String name) {
        this.name = name;
    }
}

@Data
class AuditEntity {

    @CreatedBy
    private String createdBy;

    @CreatedDate
    private Instant createdDate;

    @LastModifiedBy
    private String lastModifiedBy;

    @LastModifiedDate
    private Instant lastModifiedDate;
}

@Service
@RequiredArgsConstructor
@Log4j2
class PersonService {

    private final PersonRepository personRepository;

    public Flux<Person> findAll() {
        return personRepository.retrieveAll(PageRequest.of(0, 200))
            .map(person -> {
                person.setName("FETCHED " + person.getName());

                if (person.getName().contains("5")) {
                    throw new RuntimeException("Error");
                }

                return person;
            })
            .delayElements(Duration.ofSeconds(1L))
            .onErrorContinue((e, person) -> log.info("This person {} is troubled because of {}",
                person, e.getMessage()
            ));
    }
}

@RestController
@RequiredArgsConstructor
@RequestMapping("/person")
class PersonController {

    private final PersonService personService;

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Person> findAll() {
        return personService.findAll();
    }
}

@RestControllerAdvice
class ExceptionTranslator {

    @ExceptionHandler(Exception.class)
    public Mono<RestError> handle(Exception e) {
        return Mono.just(new RestError(e.getMessage()));
    }

    @Value
    private static class RestError {

        String message;
    }
}

@Configuration
@EnableReactiveMongoAuditing
class AuditConfiguration {

    @Bean
    public ReactiveAuditorAware<String> auditProvider() {
        return () -> Mono.just("SYSTEM");
    }
}
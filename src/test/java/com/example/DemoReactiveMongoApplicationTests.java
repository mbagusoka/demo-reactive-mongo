package com.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class MonoFluxTest {

    @Test
    void testCollect() {
        List<String> strings = Flux.just("Oka", "Fathya")
            .map(name -> {
                if (name.equals("Oka")) {
                    throw new RuntimeException("Error");
                }

                return name;
            })
            .onErrorContinue((t, o) -> {
            })
            .collectList()
            .block();

        Assertions.assertThat(strings.size()).isEqualTo(1);
    }

    @Test
    void testSubscribe() {
        Flux<String> a = Flux.just("LALA");

        String x = a.collectList().block().get(0);
    }

    @Test
    void testSubscription() {
        List<Integer> elements = new ArrayList<>();
        Flux.just(1, 2, 3)
            .flatMap(this::returned)
            .log()
//            .subscribeOn(Schedulers.immediate())
            .subscribe(new Subscriber<Integer>() {
                int onNextAmount;
                private Subscription s;

                @Override
                public void onSubscribe(Subscription s) {
                    this.s = s;
                    s.request(2);
                }

                @Override
                public void onNext(Integer integer) {
                    elements.add(integer);
                    onNextAmount++;
                    if (onNextAmount % 2 == 0) {
                        s.request(2);
                    }
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onComplete() {
                }
            });
    }

    @Test
    void testError() {
        Flux.just(1, 2, 3)
            .map(x -> {
                if (x == 1) {
                    throw new IllegalArgumentException("Error njing");
                }

                return x;
            })
            .concatMap(x -> Mono.just(x))
            .subscribe();
    }

    @Test
    void testConnectableFlux() {
        ConnectableFlux<Object> flux = Flux.create(
                objectFluxSink -> {
                    while (true) {
                        objectFluxSink.next(System.currentTimeMillis());
                    }
                }
            )
            .sample(Duration.ofSeconds(2))
            .publish();

        flux.subscribe(System.out::println);
        flux.subscribe(System.out::println);

        flux.connect();

        flux.subscribe(x -> System.out.println("Telat subscribe " + x));
    }

    private Mono<Integer> returned(int i) {
        System.out.println(i);

        return Mono.just(i);
    }
}

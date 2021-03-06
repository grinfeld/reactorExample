package com.mikeruoft.reactor;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Mikhail Grinfeld
 */
public class SimplifiedMrExampleWithReactor {

    @Test
    public void fluxTest() throws InterruptedException {

        // thread pool to run individual tasks
        ExecutorService executor = Executors.newFixedThreadPool(5);

        // let's define some stream which emitted every 3 seconds
        Flux.interval(Duration.ofSeconds(3))
            // every 3 seconds when event is emitted - let's take from DB list of RoutingInfo
            .flatMap(i -> fromAkaDB())
            // subscribe - actually, starts the flow.
            // Until compiler riches line below, nothing has been executed
            .subscribe(l -> {
                // at this point we define what we will do WHEN we receive list of RoutingInfos
                // i.e. lazy definitions of steps we'll perform when flow starts
                Flux<List<List<RoutingInfo>>> sendMcStream =
                    // create stream of RoutingInfo from list we received "from" DB
                    Flux.fromIterable(l)
                    // enrich individual RoutingInfo with additional data
                    .map(SimplifiedMrExampleWithReactor::setServices)
                    // actual we can make this task parallel, too
                    // just uncomment next line
                    // .subscribeOn(Schedulers.newParallel("my Thread", 5))
                    // grouped by same message id
                    .groupBy(RoutingInfo::getMessageId, Function.identity())
                    // convert GroupedFlux (each group) into List
                    .flatMap(groups -> groups.collect(Collectors.toList()))
                    // filter empty lists
                    .filter(rl -> !rl.isEmpty())
                    // we divide into bulks with specified maximum size
                    .flatMap(rl -> rl.size() > MAX_BULK ?
                        Mono.just(Lists.partition(rl, getBulkSize())) :
                        Mono.just(Collections.singletonList(rl))
                    );

                // here we actually starts the flow
                sendMcStream.subscribe(
                    lr -> {
                        // converts LIst of List of RoutingInfo into stream of individual
                        // lists of routing info
                        Flux.fromIterable(lr)
                        // filter empty lists
                        .filter(rl -> !rl.isEmpty())
                        // convert to stream of MessageContainer
                        .map(sp -> getMC (
                            sp.get(0).getMessageId(),
                            sp.stream().map(RoutingInfo::getDevice).collect(Collectors.toList())
                        ))
                        // fetches data from this Flux only first 3 seconds
                        .takeMillis(3000L)
                        // assign to this stream thread pool executor to use for parallel execution
                        .subscribeOn(Schedulers.fromExecutor(executor))
                        // or we could create executors from Reactor API, by uncomment next line
                        // .subscribeOn(Schedulers.newParallel("Send MC Executor", 5))
                        // here we actually start sending MC
                        .subscribe(SimplifiedMrExampleWithReactor::sendMc);
                    }
                );
            });

            Thread.sleep(10000L);
    }

    private static Flux<List<RoutingInfo>> fromAkaDB() {
        // since it's not real method to DB, using lazy
        // flux initialization, Flux.defer
        return Flux.defer(() -> Flux.just(Arrays.asList(
                new RoutingInfo(1L, "1"),
                new RoutingInfo(2L, "2"),
                new RoutingInfo(3L, "3"),
                new RoutingInfo(6L, "4"),
                new RoutingInfo(2L, "5"),
                new RoutingInfo(5L, "6"),
                new RoutingInfo(1L, "7"),
                new RoutingInfo(2L, "8"),
                new RoutingInfo(6L, "9"),
                new RoutingInfo(1L, "10"),
                new RoutingInfo(7L, "11"),
                new RoutingInfo(9L, "12"),
                new RoutingInfo(1L, "13")
        )));

    }

    private static final int MAX_BULK = 3;
    private static final Random r = new Random();
    private static int getBulkSize() {
        return r.nextInt(MAX_BULK) + 1;
    }

    private static void sendMc(MessageContainer mc) {
        System.out.println(new Date() + " " + mc);
    }

    private static MessageContainer getMC(long messageId, List<String> devices) {
        System.out.println(new Date() + " " + Thread.currentThread().getName() + " " + messageId + " " + devices.stream().collect(Collectors.joining(",")));
        return new MessageContainer();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class MessageContainer {
        private long messageId;
        private String device;

        @Override
        public String toString() {
            return Thread.currentThread().getName() + " MessageContainer{}";
        }
    }

    private static RoutingInfo setServices(RoutingInfo ri) {
        return ri;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RoutingInfo {
        private long messageId;
        private String device;
    }
}

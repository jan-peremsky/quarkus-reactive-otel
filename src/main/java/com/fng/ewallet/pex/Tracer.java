package com.fng.ewallet.pex.util;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.quarkus.logging.Log;
import io.quarkus.opentelemetry.runtime.QuarkusContextStorage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.ContextInternal;
import io.vertx.mutiny.core.Vertx;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utility class/CDI bean to manage {@link Span}s on Mutiny reactive pipelines.
 * <p>
 * //TODO JaPe - see <dependency>   <groupId>com.azure</groupId>    <artifactId>azure-core-tracing-opentelemetry</artifactId>     <version>1.0.0-beta.29</version>* </dependency>
 * at https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/core/azure-core-tracing-opentelemetry
 * try to interconnect with Cosmos own instrumentation
 */
@ApplicationScoped
public class Tracer {

    private static final String SPAN_STACK_KEY = "SPAN-AND-SCOPE-STACK";

    @Inject
    Vertx vertx;

    @Inject
    io.opentelemetry.api.trace.Tracer tracer;

    record SpanAndScope(Span span, Scope scope) {
    }

    /**
     * Amends given pipeline so that it runs on a duplicated {@link Vertx} {@link io.vertx.mutiny.core.Context} with a new {@link Span} injected into it named `spanName`.
     * If a valid remoteSpanContext is provided - it is set as the parent span of the newly created named span.
     * Use like this:
     * <pre>{@code
     *      Optional<SpanContext> spanContext = ...get remote SpanContext somewhere;
     *      String spanName = "my-span";
     *
     *      var pipeline = Uni.createFrom().item(() -> parseEventPayload(jsonNode, eventProcessorHandler.getPayloadType()))
     *      .flatMap(eventPayload -> processEvent(eventPayload, eventProcessorHandler.getProcessFunction()));
     *
     *      //wrap and run
     *      CompletableFuture<Void> completableFuture = tracer.injectSpanAndRun(pipeline, spanName, spanContext);
     *
     *      //await
     *      try {
     *      completableFuture.get(eventProcessorConfig.processTimeout().toMillis(), TimeUnit.MILLISECONDS);
     *      } catch (Exception e) {
     *      throw new RuntimeException(e); //wrap and rethrow
     *      }
     * }</pre>
     *
     * @param pipeline
     * @param spanName
     * @param remoteSpanContext
     * @return
     */
    public CompletableFuture<Void> injectSpanAndRun(Uni<?> pipeline, String spanName, Optional<SpanContext> remoteSpanContext) {
        //we need a new "duplicate" vertx context
        ContextInternal internal = (ContextInternal) vertx.getOrCreateContext().getDelegate();
        io.vertx.mutiny.core.Context ctx = io.vertx.mutiny.core.Context.newInstance(internal.duplicate());

        //let's run our pipeline on the duplicate context, otherwise Vertx would create a new ad-hoc context for each call to vertx.getOrCreateContext() or similar
        CompletableFuture<Void> future = new CompletableFuture<>();
        ctx.runOnContext(() -> {
            Log.debugf("Running pipeline: %s on: %s", pipeline, ctx.getDelegate());
            withRootSpan(pipeline, spanName, remoteSpanContext, SpanKind.CONSUMER)
                    .subscribe().with(i -> {
                        future.complete(null);
                    }, th -> {
                        future.completeExceptionally(th);
                    });
        });

        return future; //return a hook to the result to be
    }

    /**
     * Inject new span (either root one or child of the provided remote {@link SpanContext}) into current pipeline.
     *
     * @param pipeline
     * @param spanName
     * @param remoteSpanContext
     * @param spanKind
     * @param <T>
     * @return
     */
    private <T> Uni<T> withRootSpan(Uni<T> pipeline, String spanName, Optional<SpanContext> remoteSpanContext, SpanKind spanKind) {

        //ensure our tracing data will  be preserved
        checkRunningOnDuplicateContext();

        Span span;
        Scope scope;
        spanKind = spanKind == null ? SpanKind.CONSUMER : spanKind;
        if (remoteSpanContext.isEmpty() || !remoteSpanContext.get().isValid()) {
            //our new span as a root span
            span = tracer.spanBuilder(spanName).setSpanKind(spanKind).startSpan();
        } else {
            //our new span as a child span of the span provided by the emitter
            span = tracer.spanBuilder(spanName).setSpanKind(spanKind).setParent(Context.root().with(Span.wrap(remoteSpanContext.get()))).startSpan();
        }
        //store it in Quarkus persistor of OTEL stuff - this will make it "current" as calling Context.makeCurrent() - but calling just Context.makeCurrent() is not enough - this was taken from the `SuperHeroes` Quarkus demo
        scope = QuarkusContextStorage.INSTANCE.attach(Context.current().with(span));

        final SpanAndScope ss = new SpanAndScope(span, scope);

        return pipeline.withContext((pipe, ctx) -> {
            Deque<SpanAndScope> stack = new LinkedList<>();
            stack.push(ss);
            ctx.put(SPAN_STACK_KEY, stack);
            return pipeline
                    //end span and close the root scope
                    .onItem().invoke(() -> endSpanCloseScope(ctx, null))
                    .onFailure().invoke(th -> endSpanCloseScope(ctx, th))
                    .onCancellation().invoke(() -> endSpanCloseScope(ctx, null));
        });
    }

    /**
     * Injects new child span (or sub-span) to the already prepared reactive pipeline. I.e. {@link #injectSpanAndRun(Uni, String, Optional)} was already used an this pipeline's Mutiny context is initialized properly.
     * If called on an uninitialized pipeline - a warning is issued.
     * Example usage:
     * <pre>{@code
     *      //prepare an Uni pipeline
     *      Uni<CosmosBatchResponse> pipeline = tracer.wrapMono(mono).onItem().invoke(() -> persistorConfig.fireMultiBatchEvent(multiBatch));
     *      //run it as a sub-span named `HybridPersistor.persist`
     *      return tracer.withSpan(pipeline, "HybridPersistor.persist");
     * }</pre>
     *
     * @param pipeline
     * @param spanName
     * @param spanKind
     * @param <T>
     * @return
     */
    public <T> Uni<T> withSpan(Uni<T> pipeline, String spanName, SpanKind spanKind) {

        return pipeline.withContext((pipe, ctx) -> {
            //get current span and check if valid
            Optional<Span> currentSpan = currentMutinySpan(ctx);
            if (currentSpan.isEmpty()) {
                Log.debugf("`withSpan()` called from pipeline not amended by the `Tracer` - does not contain current `Span` in the context. See `Tracer.injectSpanAndRun()`");
                return pipeline;
            }
            if (!currentSpan.get().getSpanContext().isValid()) { //no real span used
                return pipeline;
            }
            //ensure our tracing data will  be preserved
            checkRunningOnDuplicateContext();

            var span = tracer.spanBuilder(spanName).setSpanKind(spanKind == null ? SpanKind.INTERNAL : spanKind).startSpan();
            Scope scope = span.makeCurrent();

            //get the span stack from context or in case it's missing, create a new empty one
            Deque<SpanAndScope> stack = ctx.getOrElse(SPAN_STACK_KEY, () -> {
                var emptyStack = new LinkedList<SpanAndScope>();
                ctx.put(SPAN_STACK_KEY, emptyStack);
                return emptyStack;
            });
            stack.push(new SpanAndScope(span, scope));
            ctx.put(SPAN_STACK_KEY, stack); //stack this span and scope in the prepared context stores stack holder
            return pipeline
                    //span must be ended and scope closed
                    .onItem().invoke(() -> endSpanCloseScope(ctx, null))
                    .onFailure().invoke(th -> endSpanCloseScope(ctx, th))
                    .onCancellation().invoke(() -> endSpanCloseScope(ctx, null));
        });
    }

    /**
     * Does the same as calling {@link #withSpan(Uni, String, SpanKind)} with {@link SpanKind#INTERNAL}
     *
     * @param pipeline
     * @param spanName
     * @param <T>
     * @return
     */
    public <T> Uni<T> withSpan(Uni<T> pipeline, String spanName) {
        return withSpan(pipeline, spanName, SpanKind.INTERNAL);
    }


    private void endSpanCloseScope(io.smallrye.mutiny.Context ctx, Throwable failure) {
        SpanAndScope ss = ctx.<Deque<SpanAndScope>>getOrElse(SPAN_STACK_KEY, LinkedList::new).poll(); //retrieve and remove
        if (ss != null) {
            Log.tracef("Ending span: %s and removing from scope", ss.span.getSpanContext());
            if (failure != null) {
                ss.span.recordException(failure);
                String msg = failure.getMessage();
                ss.span.setStatus(StatusCode.ERROR, msg.substring(0, Math.min(128, msg.length())));
            }
            ss.span.end(); //stop span clock
            ss.scope.close(); //remove from scope
        }
    }


    private void checkRunningOnDuplicateContext() {
        if (!((ContextInternal) vertx.getOrCreateContext().getDelegate()).isDuplicate()) {
            throw new UnsupportedOperationException("Not running on the vertx duplicate context");
        }
    }

    private io.vertx.mutiny.core.Context getDuplicateVertxContext() { //TODO JaPe - in DEV mode we should run on worker thread to avoid BlockingThreadExceptions ...
        ContextInternal internal = (ContextInternal) vertx.getOrCreateContext().getDelegate();
        io.vertx.mutiny.core.Context ctx;
        if (!internal.isDuplicate()) {
            ctx = io.vertx.mutiny.core.Context.newInstance(internal.duplicate());
        } else {
            ctx = vertx.getOrCreateContext();
        }
        return ctx;
    }

    /**
     * Utility function allowing to add event to current span on the reactive pipeline. Use as here:
     * <pre>{@code
     *  Uni.createFrom().item(command)
     *      .withContext(addEvent("processCommand/phase1"))
     *      .withContext(setAttribute("key","value"))
     *      .withContext(setErrorStatus(new RuntimeException()))
     *      .withContext(consumeSpan(span -> Log.info(span)))
     *      .flatMap(this::preprocessBasedOnCommandType)
     *
     * }</pre>
     *
     * @param event
     * @param <T>
     * @return
     */
    public static <T> BiFunction<Uni<T>, io.smallrye.mutiny.Context, Uni<T>> addEvent(String event) {
        return (uni, ctx) -> {
            currentMutinySpan(ctx).ifPresent(span -> span.addEvent(event));
            return uni;
        };
    }

    /**
     * Utility function allowing to set attribute to current span on the reactive pipeline. Use as here:
     * <pre>{@code
     *  Uni.createFrom().item(command)
     *      .withContext(addEvent("processCommand/phase1"))
     *      .withContext(setAttribute("key","value"))
     *      .withContext(setErrorStatus(new RuntimeException()))
     *      .withContext(consumeSpan(span -> Log.info(span)))
     *      .flatMap(this::preprocessBasedOnCommandType)
     *
     *
     * }</pre>
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    public static <T> BiFunction<Uni<T>, io.smallrye.mutiny.Context, Uni<T>> setAttribute(String key, String value) {
        return (uni, ctx) -> {
            currentMutinySpan(ctx).ifPresent(span -> span.setAttribute(AttributeKey.stringKey(key), value));
            return uni;
        };
    }

    /**
     * Utility function allowing to record exception to current span on the reactive pipeline. Use as here:
     * <pre>{@code
     *  Uni.createFrom().item(command)
     *      .withContext(addEvent("processCommand/phase1"))
     *      .withContext(setAttribute("key","value"))
     *      .withContext(setErrorStatus(new RuntimeException()))
     *      .withContext(consumeSpan(span -> Log.info(span)))
     *      .flatMap(this::preprocessBasedOnCommandType)
     *
     *
     * }</pre>
     *
     * @param ex
     * @param <T>
     * @return
     */
    public static <T> BiFunction<Uni<T>, io.smallrye.mutiny.Context, Uni<T>> setErrorStatus(Throwable ex) {
        return (uni, ctx) -> {
            currentMutinySpan(ctx).ifPresent(span -> {
                span.recordException(ex);
                String msg = ex.getMessage();
                span.setStatus(StatusCode.ERROR, msg.substring(0, Math.min(128, msg.length())));
            });
            return uni;
        };
    }

    /**
     * Utility function allowing to consume current span on the reactive pipeline. Use as here:
     * <pre>{@code
     *  Uni.createFrom().item(command)
     *      .withContext(addEvent("processCommand/phase1"))
     *      .withContext(setAttribute("key","value"))
     *      .withContext(setErrorStatus(new RuntimeException()))
     *      .withContext(consumeSpan(span -> Log.info(span)))
     *      .flatMap(this::preprocessBasedOnCommandType)
     *
     *
     * }</pre>
     *
     * @param spanConsumer
     * @param <T>
     * @return
     */
    public static <T> BiFunction<Uni<T>, io.smallrye.mutiny.Context, Uni<T>> consumeSpan(Consumer<Span> spanConsumer) {
        return (uni, ctx) -> {
            currentMutinySpan(ctx).ifPresent(spanConsumer::accept);
            return uni;
        };
    }

    /**
     * Tries to read the {@link Span} from the {@link io.smallrye.mutiny.Context}. If not present, fallbacks to calling {@link Span#current()}
     *
     * @param ctx
     * @return
     */
    public static Optional<Span> currentMutinySpan(io.smallrye.mutiny.Context ctx) {
        return Optional.ofNullable(ctx.<Deque<SpanAndScope>>getOrElse(SPAN_STACK_KEY, LinkedList::new))
                .map(Deque::peek)
                .map(SpanAndScope::span)
                .or(() -> {
                    Log.debugf("Mutiny context does not contain SpanAndScope data -> falling back to Span.current()");
                    return Optional.of(Span.current()).filter(s -> s.getSpanContext().isValid());
                });
    }

    /**
     * Wraps Cosmos {@link Mono} pipeline into the Mutiny {@link Uni} preserving tracing data stored in the vertx context.
     *
     * @param mono
     * @param <T>
     * @return
     */
    public <T> Uni<T> wrapMono(Mono<T> mono) {
        return Uni.createFrom().item(getDuplicateVertxContext())
                .flatMap(vertxContext -> Uni.createFrom().publisher(mono)
                        .emitOn(task -> vertxContext.runOnContext(task))
                )
                .map(Function.identity()); //needed to kick in the emitter thread
    }

    /**
     * Converts the provided {@link Mono} to {@link Uni} and wraps with the named sub-{@link Span}.
     *
     * @param mono
     * @param spanName
     * @param <T>
     * @return
     */
    public <T> Uni<T> wrapMonoWithSpan(Mono<T> mono, String spanName) {
        return withSpan(wrapMono(mono), spanName);
    }

    /**
     * Wraps Cosmos {@link Flux} pipeline into the Mutiny {@link Multi} preserving tracing data stored in the vertx context.
     *
     * @param flux
     * @param <T>
     * @return
     */
    public <T> Multi<T> wrapFlux(Flux<T> flux) {
        return Multi.createFrom().item(getDuplicateVertxContext())
                .flatMap(vertxContext -> Multi.createFrom().publisher(flux)
                        .emitOn(task -> vertxContext.runOnContext(task))
                )
                .map(Function.identity()); //needed to kick in the emitter thread
    }

    /**
     * Converts string `traceparent` attribute to {@link SpanContext} if possible.
     *
     * @param traceparent
     * @return
     */
    public static Optional<SpanContext> fromTraceparent(String traceparent) {
        if (traceparent == null)
            return Optional.empty();
        String[] parts = traceparent.split("-");
        if (parts.length >= 4) {
            SpanContext spanContext = SpanContext.createFromRemoteParent(parts[1], parts[2], TraceFlags.fromHex(parts[3], 0), TraceState.getDefault());
            if (spanContext.isValid()) {
                return Optional.of(spanContext);
            }
        }
        return Optional.empty();
    }

    /**
     * Encodes {@link SpanContext} as a string to be sent via Cosmos
     *
     * @param spanContext
     * @return
     */
    public static String toTraceparent(SpanContext spanContext) {
        //TODO JaPe - start using w3c format propagator: W3CTraceContextPropagator
        return spanContext != null && spanContext.isValid() ? "00-" + spanContext.getTraceId() + "-" + spanContext.getSpanId() + "-" + spanContext.getTraceFlags().asHex() : null;
    }
}

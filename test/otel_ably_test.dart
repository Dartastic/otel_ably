// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_ably/otel_ably.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_ably-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('publish records PRODUCER span with messaging.* attrs', () async {
    await tracedAblyPublish<String>(
      channel: 'orders',
      event: 'order.created',
      payloadSize: 200,
      clientId: 'client-1',
      invoke: () async => const AblyCallResult(
        value: 'ok',
        messageId: 'msg-1',
      ),
    );
    final span = spans.findSpanByName('publish orders');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'ably');
    expect(a['messaging.operation'], 'publish');
    expect(a['messaging.destination.name'], 'orders');
    expect(a['ably.event.name'], 'order.created');
    expect(a['messaging.message.body.size'], 200);
    expect(a['ably.client.id'], 'client-1');
    expect(a['messaging.message.id'], 'msg-1');
  });

  test('attach records CLIENT span', () async {
    await tracedAblyAttach<String>(
      channel: 'orders',
      invoke: () async => 'attached',
    );
    final span = spans.findSpanByName('attach orders');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.client);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'ably');
    expect(a['messaging.operation'], 'subscribe');
    expect(a['messaging.destination.name'], 'orders');
  });

  test('records exception on throw', () async {
    await expectLater(
      tracedAblyPublish<String>(
        channel: 'c',
        event: 'e',
        invoke: () async => throw StateError('boom'),
      ),
      throwsA(isA<StateError>()),
    );
    expect(
      spans.findSpanByName('publish c')!.status,
      SpanStatusCode.Error,
    );
  });

  test('suppression skips spans', () async {
    await runWithoutAblyInstrumentationAsync(() async {
      await tracedAblyPublish<String>(
        channel: 'c',
        event: 'e',
        invoke: () async => const AblyCallResult(value: 'v'),
      );
      await tracedAblyAttach<String>(
        channel: 'c',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
    expect(spans.findSpansStartingWith('attach '), isEmpty);
  });
}

// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_ably_suppression.dart';

const _tracerName = 'dartastic_otel_libs_ably';
const _systemValue = 'ably';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from a `tracedAbly*` invoke callback.
class AblyCallResult<T> {
  const AblyCallResult({required this.value, this.messageId});
  final T value;
  final String? messageId;
}

/// Wraps an Ably realtime publish in a PRODUCER span using OTel
/// messaging semconv.
Future<T> tracedAblyPublish<T>({
  required String channel,
  required String event,
  required Future<AblyCallResult<T>> Function() invoke,
  int? payloadSize,
  String? clientId,
}) async {
  if (ablyInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }

  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'publish',
    'messaging.destination.name': channel,
    'ably.event.name': event,
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (clientId != null) m['ably.client.id'] = clientId;

  final span = _tracer().startSpan(
    'publish $channel',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.messageId != null) {
      span.addAttributes(OTel.attributes([
        OTel.attributeString('messaging.message.id', r.messageId!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps an Ably channel attach / subscribe in a CLIENT span.
Future<T> tracedAblyAttach<T>({
  required String channel,
  required Future<T> Function() invoke,
}) async {
  if (ablyInstrumentationSuppressed()) {
    return invoke();
  }

  final span = _tracer().startSpan(
    'attach $channel',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap({
      'messaging.system': _systemValue,
      'messaging.operation': 'subscribe',
      'messaging.destination.name': channel,
    }),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

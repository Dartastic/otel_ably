// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_ably/otel_ably.dart';

/// Traces Ably calls with OpenTelemetry.
///
/// `otel_ably` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Ably call in
/// [tracedAblyPublish]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'ably-example');

  // Wraps an Ably realtime publish in a PRODUCER span using OTel messaging semconv.
  final ablyPublishId = await tracedAblyPublish<String>(
    channel: 'orders',
    event: 'order.created',
    payloadSize: 512,
    clientId: 'client_8f3a2c',
    invoke: () async {
      // Make the Ably call here, then report what it returned.
      const id = 'ably_8f3a2c';
      return const AblyCallResult(value: id, messageId: 'message_8f3a2c');
    },
  );
  OTelLog.info('tracedAblyPublish returned $ablyPublishId');

  // Wraps an Ably channel attach / subscribe in a CLIENT span.
  final ablyAttachId = await tracedAblyAttach<String>(
    channel: 'orders',
    invoke: () async {
      // Make the Ably call here, then report what it returned.
      return 'ably_8f3a2c';
    },
  );
  OTelLog.info('tracedAblyAttach returned $ablyAttachId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutAblyInstrumentationAsync.
  await runWithoutAblyInstrumentationAsync(
    () => tracedAblyPublish<String>(
      channel: 'orders',
      event: 'order.created',
      payloadSize: 512,
      clientId: 'client_8f3a2c',
      invoke: () async {
        const id = 'ably_8f3a2c';
        return const AblyCallResult(value: id, messageId: 'message_8f3a2c');
      },
    ),
  );

  await OTel.shutdown();
}

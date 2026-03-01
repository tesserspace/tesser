export function makeEnvelope(args: { protocolVersion: string; correlationId: string }) {
  return {
    protocol_version: args.protocolVersion,
    correlation_id: args.correlationId,
    request_id: crypto.randomUUID(),
  };
}

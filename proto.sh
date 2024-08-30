prom_src="src/java/proto/prometheus"
otel_src="src/java/proto"
riemann_src="src/java/proto"
dest="src/java"

protoc -I="${riemann_src}" --java_out "${dest}" "${riemann_src}/riemann/proto.proto"

protoc -I="${prom_src}" --java_out "${dest}" "${prom_src}/types.proto"
protoc -I="${prom_src}" --java_out "${dest}" "${prom_src}/remote.proto"
protoc -I="${prom_src}/gogoproto" --java_out "${dest}" "${prom_src}/gogoproto/gogo.proto"

protoc -I="${otel_src}" --java_out "${dest}" "${otel_src}/opentelemetry/proto/common/v1/common.proto"
protoc -I="${otel_src}" --java_out "${dest}" "${otel_src}/opentelemetry/proto/resource/v1/resource.proto"
protoc -I="${otel_src}" --java_out "${dest}" "${otel_src}/opentelemetry/proto/trace/v1/trace.proto"
protoc -I="${otel_src}" --java_out "${dest}" "${otel_src}/opentelemetry/proto/collector/trace/v1/trace_service.proto"

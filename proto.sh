src="src/java/proto/prometheus"
dest="src/java"

~/.local/bin/protoc -I="${src}" --java_out "${dest}" "${src}/types.proto"
~/.local/bin/protoc -I="${src}" --java_out "${dest}" "${src}/remote.proto"
~/.local/bin/protoc -I="${src}/gogoproto" --java_out "${dest}" "${src}/gogoproto/gogo.proto"

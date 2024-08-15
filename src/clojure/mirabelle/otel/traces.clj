(ns mirabelle.otel.traces
  (:import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
           io.opentelemetry.proto.trace.v1.ResourceSpans
           io.opentelemetry.proto.resource.v1.Resource
           io.opentelemetry.proto.trace.v1.ScopeSpans
           io.opentelemetry.proto.trace.v1.Span
           io.opentelemetry.proto.common.v1.KeyValue
           io.opentelemetry.proto.common.v1.AnyValue
           io.opentelemetry.proto.common.v1.AnyValue$ValueCase
           com.google.protobuf.ByteString
           io.opentelemetry.proto.common.v1.InstrumentationScope
           io.opentelemetry.proto.trace.v1.Span$Event
           io.opentelemetry.proto.trace.v1.Span$Link
           io.opentelemetry.proto.trace.v1.Span$Builder
           io.opentelemetry.proto.trace.v1.Status$StatusCode
           io.opentelemetry.proto.trace.v1.Span$SpanKind
           org.apache.commons.codec.binary.Hex))

(declare key-value-list->map)

(defn byte-string->string
  [^ByteString byte-string]
  (Hex/encodeHexString (.toByteArray byte-string)))

(defn string->byte-string
  [^String s]
  (ByteString/copyFrom (Hex/encode ^"[B" (.getBytes s))))

(defn any-value->value
  [^AnyValue any-value]
  (let [value (.getValueCase any-value)]
    (condp = value
      AnyValue$ValueCase/STRING_VALUE (.getStringValue any-value)
      AnyValue$ValueCase/BOOL_VALUE (.getBoolValue any-value)
      AnyValue$ValueCase/INT_VALUE (.getIntValue any-value)
      AnyValue$ValueCase/ARRAY_VALUE (map any-value->value
                                          (.getValuesList
                                           (.getArrayValue any-value)))
      AnyValue$ValueCase/KVLIST_VALUE (key-value-list->map
                                       (.getValuesList (.getKvlistValue any-value)))
      AnyValue$ValueCase/BYTES_VALUE (.toByteArray (.getBytesValue any-value))
      nil)))

(defn key-value-list->map
  [^java.util.List key-values]
  (doall
   (reduce
    (fn [state ^KeyValue key-value]
      (assoc state
             (keyword (.getKey key-value))
             (any-value->value (.getValue key-value))))
    {}
    key-values)))

(defn scope->map
  [^InstrumentationScope scope]
  {:name (.getName scope)
   :version (.getVersion scope)
   :attributes (key-value-list->map (.getAttributesList scope))
   :dropped-attributes-count (.getDroppedAttributesCount scope)})

(defn event->map
  [^Span$Event event]
  {:time (float (/ (.getTimeUnixNano event) 1000000000))
   :name (.getName event)
   :attributes (key-value-list->map (.getAttributesList event))
   :dropped-attributes-count (.getDroppedAttributesCount event)})

(defn link->map
  [^Span$Link link]
  {:trace-id (byte-string->string (.getTraceId link))
   :span-id (byte-string->string (.getSpanId link))
   :trace-state (.getTraceState link)
   :attributes (key-value-list->map (.getAttributesList link))
   :dropped-attributes-count (.getDroppedAttributesCount link)})

(defn span->event
  [^Span span scope resource schema-url]
  (let [start-time (.getStartTimeUnixNano span)
        end-time (.getEndTimeUnixNano span)
        duration (- end-time start-time)
        kind (condp = (.getKind span)
               Span$SpanKind/SPAN_KIND_UNSPECIFIED :unspecified
               Span$SpanKind/SPAN_KIND_INTERNAL :internal
               Span$SpanKind/SPAN_KIND_SERVER :server
               Span$SpanKind/SPAN_KIND_CLIENT :client
               Span$SpanKind/SPAN_KIND_PRODUCER :producer
               Span$SpanKind/SPAN_KIND_CONSUMER :consumer
               :unrecognized)
        status (.getStatus span)
        service (get-in resource [:attributes :service.name])
        status-code (condp = (.getCode status)
                      Status$StatusCode/STATUS_CODE_ERROR "error"
                      Status$StatusCode/STATUS_CODE_OK "ok"
                      Status$StatusCode/STATUS_CODE_UNSET "unset")]
    {:resource resource
     :service service
     :scope scope
     :schema-url schema-url
     :trace-id (byte-string->string (.getTraceId span))
     :span-id (byte-string->string (.getSpanId span))
     :state status-code
     :trace-state (.getTraceState span)
     :parent-span-id (byte-string->string (.getParentSpanId span))
     :name (.getName span)
     :kind kind
     :time (double (/ end-time 1000000000))
     :start-time start-time
     :end-time end-time
     :metric duration
     :attributes (key-value-list->map (.getAttributesList span))
     :dropped-attributes-count (.getDroppedAttributesCount span)
     :events (map event->map (.getEventsList span))
     :dropped-events-count (.getDroppedEventsCount span)
     :links (map link->map (.getLinksList span))
     :dropped-links-count (.getDroppedLinksCount span)
     :status {:message (.getMessage status)
              :status status-code}}))

(defn scope-span->events
  [^ScopeSpans scope-spans resource]
  (let [schema-url (.getSchemaUrl scope-spans)
        scope (scope->map (.getScope scope-spans))]
    (map #(span->event % scope resource schema-url) (.getSpansList scope-spans))))

(defn resource-span->events
  [^ResourceSpans resource-span]
  (let [^Resource resource (.getResource resource-span)
        scope-spans (.getScopeSpansList resource-span)
        resource-attributes (key-value-list->map (.getAttributesList resource))
        schema-url (.getSchemaUrl resource-span)]
    (map #(scope-span->events % {:attributes resource-attributes
                                 :schema-url schema-url})
         scope-spans)))

(defn service-request->events
  [^ExportTraceServiceRequest service-request]
  (let [^ResourceSpans span-list (.getResourceSpansList service-request)]
    (map resource-span->events span-list)))


(defn event->span
  [{:keys [trace-id
           span-id
           status
           trace-state
           parent-span-id
           name
           kind
           start-time
           end-time
           attributes
           events
           links
           status] :as event}]
  (let [builder (Span/newBuilder)]
    (when trace-id
      
      )
    (when start-time
      (.setStartTimeUnixNano builder start-time))
    (when end-time
      (.setEndTimeUnixNano builder end-time))
    
    )

  )



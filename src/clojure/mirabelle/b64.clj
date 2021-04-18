(ns mirabelle.b64
  (:import java.util.Base64))

(defn from-base64
  [s]
  (String. #^bytes (.decode (Base64/getDecoder) ^String s)))

(defn to-base64
  [s]
  (.encodeToString (Base64/getEncoder) (.getBytes ^String s)))

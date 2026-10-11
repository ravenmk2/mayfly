package packet

import "strings"

// validateTopicName checks that a topic name contains no wildcard characters
// [MQTT-3.3.2-2] and is within the length limit.
func validateTopicName(name string) error {
	if len(name) > maxFieldLen {
		return errMalformed("topic name exceeds two byte length limit")
	}
	if strings.ContainsAny(name, "+#") {
		// Wildcard characters MUST NOT appear in a topic name
		// [MQTT-3.3.2-2].
		return Code{Code: CodeTopicNameInvalid, Reason: "topic name contains wildcard characters"}
	}
	return nil
}

// validateTopicFilter checks the structure of a topic filter, including the
// wildcard placement rules [MQTT-4.7.1-1]..[MQTT-4.7.1-3] and the shared
// subscription prefix [MQTT-4.8.2].
func validateTopicFilter(filter string) error {
	// Topic filters MUST be at least one character long [MQTT-4.7.3-1].
	if filter == "" {
		return Code{Code: CodeTopicFilterInvalid, Reason: "topic filter is empty"}
	}
	if len(filter) > maxFieldLen {
		return errMalformed("topic filter exceeds two byte length limit")
	}
	f := filter
	if strings.HasPrefix(f, "$share/") {
		// Shared subscription: $share/{ShareName}/{filter}. The share name
		// and filter MUST be non-empty and the share name MUST NOT contain
		// wildcards [MQTT-4.8.2-1], [MQTT-4.8.2-2].
		rest := f[len("$share/"):]
		i := strings.IndexByte(rest, '/')
		if i <= 0 || i == len(rest)-1 {
			return Code{Code: CodeTopicFilterInvalid,
				Reason: "shared subscription requires $share/{name}/{filter}"}
		}
		if strings.ContainsAny(rest[:i], "+#") {
			return Code{Code: CodeTopicFilterInvalid,
				Reason: "shared subscription share name contains wildcards"}
		}
		f = rest[i+1:]
	}
	levels := strings.Split(f, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") {
			// The multi-level wildcard MUST occupy an entire level and be
			// the last level [MQTT-4.7.1-2].
			if level != "#" || i != len(levels)-1 {
				return Code{Code: CodeTopicFilterInvalid,
					Reason: "multi-level wildcard # must occupy the final level"}
			}
		}
		if strings.Contains(level, "+") {
			// The single-level wildcard MUST occupy an entire level
			// [MQTT-4.7.1-3].
			if level != "+" {
				return Code{Code: CodeTopicFilterInvalid,
					Reason: "single-level wildcard + must occupy an entire level"}
			}
		}
	}
	return nil
}

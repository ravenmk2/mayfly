package topics

import "strings"

// IsValidTopicName reports whether name is a valid topic name for
// publishing: non-empty and free of wildcard characters [MQTT-3.3.2-2],
// [MQTT-4.7.3-1].
func IsValidTopicName(name string) bool {
	return name != "" && !strings.ContainsAny(name, "+#")
}

// IsValidFilter reports whether filter is a structurally valid topic filter:
// the multi-level wildcard MUST occupy the final level on its own
// [MQTT-4.7.1-2], the single-level wildcard MUST occupy an entire level
// [MQTT-4.7.1-3], and a shared subscription prefix MUST have the form
// $share/{group}/{filter} with a wildcard-free group name [MQTT-4.8.2-1],
// [MQTT-4.8.2-2].
func IsValidFilter(filter string) bool {
	if filter == "" {
		// Topic filters MUST be at least one character long [MQTT-4.7.3-1].
		return false
	}
	f := filter
	if group, inner := splitSharedFilter(filter); group != "" {
		if strings.ContainsAny(group, "+#") {
			return false
		}
		f = inner
	} else if strings.HasPrefix(f, sharedPrefix) {
		return false // malformed shared form: missing group or inner filter
	}
	levels := strings.Split(f, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return false
		}
		if strings.Contains(level, "+") && level != "+" {
			return false
		}
	}
	return true
}

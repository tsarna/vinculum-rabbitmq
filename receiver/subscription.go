package receiver

import "strings"

// DefaultRoutingKeyTransform controls fallback behaviour when no Subscription
// matches the incoming AMQP routing key.
type DefaultRoutingKeyTransform int

const (
	// DefaultRKDotToSlash replaces "." with "/" in the routing key to form the
	// vinculum topic (e.g. "sensor.abc.reading" → "sensor/abc/reading").
	// This is the default.
	DefaultRKDotToSlash DefaultRoutingKeyTransform = iota

	// DefaultRKVerbatim uses the routing key unchanged as the vinculum topic.
	DefaultRKVerbatim

	// DefaultRKError logs an error, nacks without requeue, and increments
	// the error counter.
	DefaultRKError

	// DefaultRKIgnore acks and silently discards messages with no matching
	// Subscription.
	DefaultRKIgnore
)

// VinculumTopicFunc resolves the vinculum topic for an inbound AMQP delivery.
// routingKey is the AMQP routing key the message was published with; exchange
// is the exchange it was published to; fields is the AMQP headers (with W3C
// trace context stripped) merged with pattern-extracted captures; msg is the
// deserialized payload. Return "" to fall back to dot_to_slash on routingKey.
type VinculumTopicFunc func(routingKey, exchange string, fields map[string]string, msg any) (string, error)

// Subscription maps an AMQP routing-key pattern to a vinculum topic resolver.
type Subscription struct {
	// RoutingKeyPattern uses AMQP topic-exchange wildcards, optionally with
	// named captures:
	//   *       — matches exactly one word (dot-delimited segment)
	//   *name   — matches one word and captures it as "name" in fields
	//   #       — matches zero or more words
	//   #name   — matches zero or more words and captures the joined match
	//             (dot-separated) as "name" in fields
	RoutingKeyPattern string

	// VinculumTopicFunc resolves the vinculum topic per message.
	// nil means fall back to dot_to_slash on the routing key.
	VinculumTopicFunc VinculumTopicFunc
}

// match tests whether routingKey matches pattern. It returns the captured
// fields (from *name / #name segments) and ok=true on match. On no match,
// ok=false and fields is nil.
//
// AMQP pattern syntax (topic exchange):
//   *       — one word
//   *name   — one word, captured as fields[name]
//   #       — zero or more words
//   #name   — zero or more words, captured as fields[name] (dot-joined)
//   literal — exact match for one word
//
// Words are dot-delimited.
func match(pattern, routingKey string) (fields map[string]string, ok bool) {
	patternToks := splitDot(pattern)
	keyToks := splitDot(routingKey)
	return matchTokens(patternToks, keyToks)
}

func matchTokens(pattern, key []string) (map[string]string, bool) {
	if len(pattern) == 0 {
		if len(key) == 0 {
			return nil, true
		}
		return nil, false
	}

	p := pattern[0]
	rest := pattern[1:]

	if p == "#" || (len(p) > 1 && p[0] == '#') {
		// Multi-word wildcard: try matching zero, then one, then two, ... words.
		for i := 0; i <= len(key); i++ {
			if fields, ok := matchTokens(rest, key[i:]); ok {
				if len(p) > 1 {
					if fields == nil {
						fields = make(map[string]string)
					}
					fields[p[1:]] = strings.Join(key[:i], ".")
				}
				return fields, true
			}
		}
		return nil, false
	}

	if len(key) == 0 {
		return nil, false
	}
	k := key[0]

	if p == "*" || (len(p) > 1 && p[0] == '*') {
		fields, ok := matchTokens(rest, key[1:])
		if !ok {
			return nil, false
		}
		if len(p) > 1 {
			if fields == nil {
				fields = make(map[string]string)
			}
			fields[p[1:]] = k
		}
		return fields, true
	}

	if p == k {
		return matchTokens(rest, key[1:])
	}

	return nil, false
}

// stripFieldNames converts a pattern with named captures into a plain AMQP
// binding pattern (the broker doesn't understand the field-name prefix).
//   "sensor.*deviceId.reading" → "sensor.*.reading"
//   "alerts.#rest"             → "alerts.#"
//   "alerts.#"                 → "alerts.#"
func stripFieldNames(pattern string) string {
	toks := splitDot(pattern)
	for i, t := range toks {
		if len(t) > 1 && (t[0] == '*' || t[0] == '#') {
			toks[i] = string(t[0])
		}
	}
	return strings.Join(toks, ".")
}

// splitDot is a wrapper around strings.Split that returns an empty slice for
// the empty string (instead of []string{""}). This matters for the matcher:
// an empty pattern should match an empty routing key, not a single empty word.
func splitDot(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ".")
}

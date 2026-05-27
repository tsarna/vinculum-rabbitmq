package receiver

import (
	"reflect"
	"testing"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		name       string
		pattern    string
		routingKey string
		wantFields map[string]string
		wantOK     bool
	}{
		// Literal matches
		{"exact literal", "alerts", "alerts", nil, true},
		{"literal mismatch", "alerts", "events", nil, false},
		{"multi-segment literal", "sensor.temp", "sensor.temp", nil, true},
		{"multi-segment mismatch tail", "sensor.temp", "sensor.humidity", nil, false},

		// * wildcard
		{"star matches single word", "sensor.*.reading", "sensor.abc.reading", nil, true},
		{"star does not match dot", "sensor.*", "sensor.abc.def", nil, false},
		{"star matches empty segment", "sensor.*.reading", "sensor..reading", nil, true}, // empty segment is a (zero-length) word
		{"star at end", "sensor.*", "sensor.abc", nil, true},
		{"star at start", "*.reading", "sensor.reading", nil, true},

		// *name (named capture)
		{"named star captures", "sensor.*deviceId.reading", "sensor.abc.reading", map[string]string{"deviceId": "abc"}, true},
		{"named star captures multi", "*a.*b", "x.y", map[string]string{"a": "x", "b": "y"}, true},
		{"named star no match", "sensor.*deviceId.reading", "events.abc.reading", nil, false},

		// # wildcard
		{"hash matches zero words at end", "alerts.#", "alerts", nil, true},
		{"hash matches one word at end", "alerts.#", "alerts.cpu", nil, true},
		{"hash matches many words at end", "alerts.#", "alerts.cpu.high.warn", nil, true},
		{"hash matches zero in middle", "a.#.z", "a.z", nil, true},
		{"hash matches many in middle", "a.#.z", "a.b.c.d.z", nil, true},
		{"hash at start", "#.reading", "sensor.abc.reading", nil, true},
		{"hash alone matches anything", "#", "a.b.c", nil, true},
		{"hash alone matches empty", "#", "", nil, true},

		// Combinations
		{"star and hash", "sensor.*deviceId.#", "sensor.abc.x.y", map[string]string{"deviceId": "abc"}, true},
		{"star and hash no match", "sensor.*deviceId.#", "events.abc", nil, false},

		// #name (named multi-word capture)
		{"named hash captures one word at end", "alerts.#rest", "alerts.cpu", map[string]string{"rest": "cpu"}, true},
		{"named hash captures many words at end", "alerts.#rest", "alerts.cpu.high.warn", map[string]string{"rest": "cpu.high.warn"}, true},
		{"named hash captures zero words at end", "alerts.#rest", "alerts", map[string]string{"rest": ""}, true},
		{"named hash captures zero in middle", "a.#mid.z", "a.z", map[string]string{"mid": ""}, true},
		{"named hash captures one in middle", "a.#mid.z", "a.b.z", map[string]string{"mid": "b"}, true},
		{"named hash captures many in middle", "a.#mid.z", "a.b.c.d.z", map[string]string{"mid": "b.c.d"}, true},
		{"named hash alone captures everything", "#all", "a.b.c", map[string]string{"all": "a.b.c"}, true},
		{"named hash alone captures empty", "#all", "", map[string]string{"all": ""}, true},
		{"named hash no match", "alerts.#rest", "events.cpu", nil, false},

		// *name + #name combination
		{"named star then named hash", "sensor.*device.#rest", "sensor.abc.x.y", map[string]string{"device": "abc", "rest": "x.y"}, true},
		{"named star then named hash empty tail", "sensor.*device.#rest", "sensor.abc", map[string]string{"device": "abc", "rest": ""}, true},

		// Edge cases
		{"empty pattern matches empty", "", "", nil, true},
		{"empty pattern no match", "", "x", nil, false},
		{"non-empty pattern no match empty", "x", "", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, ok := match(tt.pattern, tt.routingKey)
			if ok != tt.wantOK {
				t.Fatalf("match(%q, %q) ok = %v, want %v", tt.pattern, tt.routingKey, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			// Normalize: nil and empty map both count as "no captures".
			if tt.wantFields == nil && len(fields) == 0 {
				return
			}
			if !reflect.DeepEqual(fields, tt.wantFields) {
				t.Errorf("match(%q, %q) fields = %v, want %v", tt.pattern, tt.routingKey, fields, tt.wantFields)
			}
		})
	}
}

func TestStripFieldNames(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"sensor.*deviceId.reading", "sensor.*.reading"},
		{"sensor.*.reading", "sensor.*.reading"},
		{"alerts.#", "alerts.#"},
		{"*a.#", "*.#"},
		{"alerts.#rest", "alerts.#"},
		{"#all", "#"},
		{"sensor.*device.#rest", "sensor.*.#"},
		{"", ""},
		{"plain", "plain"},
	}
	for _, tt := range tests {
		got := stripFieldNames(tt.in)
		if got != tt.want {
			t.Errorf("stripFieldNames(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

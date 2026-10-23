package cli

import "testing"

func TestParseSeedsIsStrictAndBounded(t *testing.T) {
	for _, input := range []string{"1x..2", "1..2tail", " 1..2", "1..2..3", "3..2", "-9223372036854775808..9223372036854775807", "1..1000001"} {
		if _, _, err := parseSeeds(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	for _, input := range []string{"0..0", "-2..2", "9223372036854775807..9223372036854775807"} {
		if _, _, err := parseSeeds(input); err != nil {
			t.Errorf("rejected %q: %v", input, err)
		}
	}
}

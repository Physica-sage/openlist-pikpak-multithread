package pikpak

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestLoadPikPakTransferConfig(t *testing.T) {
	tests := []struct {
		name            string
		environment     map[string]string
		wantConcurrency int
		wantPartSize    int
		wantWarnings    int
	}{
		{
			name:            "defaults",
			wantConcurrency: defaultPikPakTransferConcurrency,
			wantPartSize:    defaultPikPakTransferPartSizeMB * utils.MB,
		},
		{
			name: "configured",
			environment: map[string]string{
				pikPakTransferConcurrencyEnv: " 12 ",
				pikPakTransferPartSizeEnv:    "64",
			},
			wantConcurrency: 12,
			wantPartSize:    64 * utils.MB,
		},
		{
			name: "disabled ignores part size",
			environment: map[string]string{
				pikPakTransferConcurrencyEnv: "0",
				pikPakTransferPartSizeEnv:    "invalid",
			},
		},
		{
			name: "invalid concurrency falls back",
			environment: map[string]string{
				pikPakTransferConcurrencyEnv: "-1",
			},
			wantConcurrency: defaultPikPakTransferConcurrency,
			wantPartSize:    defaultPikPakTransferPartSizeMB * utils.MB,
			wantWarnings:    1,
		},
		{
			name: "overflowing concurrency falls back",
			environment: map[string]string{
				pikPakTransferConcurrencyEnv: "99999999999999999999",
			},
			wantConcurrency: defaultPikPakTransferConcurrency,
			wantPartSize:    defaultPikPakTransferPartSizeMB * utils.MB,
			wantWarnings:    1,
		},
		{
			name: "invalid part size falls back",
			environment: map[string]string{
				pikPakTransferPartSizeEnv: "3",
			},
			wantConcurrency: defaultPikPakTransferConcurrency,
			wantPartSize:    defaultPikPakTransferPartSizeMB * utils.MB,
			wantWarnings:    1,
		},
		{
			name: "combined buffer limit",
			environment: map[string]string{
				pikPakTransferConcurrencyEnv: "64",
				pikPakTransferPartSizeEnv:    "256",
			},
			wantConcurrency: 64,
			wantPartSize:    32 * utils.MB,
			wantWarnings:    1,
		},
		{
			name: "combined buffer boundary",
			environment: map[string]string{
				pikPakTransferConcurrencyEnv: "64",
				pikPakTransferPartSizeEnv:    "32",
			},
			wantConcurrency: 64,
			wantPartSize:    32 * utils.MB,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := func(key string) (string, bool) {
				value, ok := test.environment[key]
				return value, ok
			}
			got, warnings := loadPikPakTransferConfig(lookup)
			if got.concurrency != test.wantConcurrency {
				t.Fatalf("concurrency = %d, want %d", got.concurrency, test.wantConcurrency)
			}
			if got.partSize != test.wantPartSize {
				t.Fatalf("partSize = %d, want %d", got.partSize, test.wantPartSize)
			}
			if len(warnings) != test.wantWarnings {
				t.Fatalf("warnings = %d, want %d: %v", len(warnings), test.wantWarnings, warnings)
			}
		})
	}
}

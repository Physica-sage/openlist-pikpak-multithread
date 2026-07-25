package pikpak

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const (
	pikPakTransferConcurrencyEnv = "PIKPAK_TRANSFER_CONCURRENCY"
	pikPakTransferPartSizeEnv    = "PIKPAK_TRANSFER_PART_SIZE_MB"

	defaultPikPakTransferConcurrency = 10
	defaultPikPakTransferPartSizeMB  = 32
	maxPikPakTransferConcurrency     = 64
	minPikPakTransferPartSizeMB      = 4
	maxPikPakTransferPartSizeMB      = 256
	maxPikPakTransferBufferMB        = 2048
)

type pikPakTransferConfig struct {
	concurrency int
	partSize    int
}

var cachedPikPakTransferConfig struct {
	sync.Once
	value pikPakTransferConfig
}

func getPikPakTransferConfig() pikPakTransferConfig {
	cachedPikPakTransferConfig.Do(func() {
		config, warnings := loadPikPakTransferConfig(os.LookupEnv)
		for _, warning := range warnings {
			log.Warnf("[pikpak] %s", warning)
		}
		cachedPikPakTransferConfig.value = config
		if config.concurrency == 0 {
			log.Info("[pikpak] transfer multirange disabled")
			return
		}
		log.Infof(
			"[pikpak] transfer multirange configured: concurrency=%d, part_size=%d MiB",
			config.concurrency,
			config.partSize/utils.MB,
		)
	})
	return cachedPikPakTransferConfig.value
}

func loadPikPakTransferConfig(lookup func(string) (string, bool)) (pikPakTransferConfig, []string) {
	concurrency, warning := readBoundedEnvInt(
		lookup,
		pikPakTransferConcurrencyEnv,
		defaultPikPakTransferConcurrency,
		0,
		maxPikPakTransferConcurrency,
	)
	warnings := appendWarning(nil, warning)
	if concurrency == 0 {
		return pikPakTransferConfig{}, warnings
	}

	partSizeMB, warning := readBoundedEnvInt(
		lookup,
		pikPakTransferPartSizeEnv,
		defaultPikPakTransferPartSizeMB,
		minPikPakTransferPartSizeMB,
		maxPikPakTransferPartSizeMB,
	)
	warnings = appendWarning(warnings, warning)
	if concurrency*partSizeMB > maxPikPakTransferBufferMB {
		configuredPartSizeMB := partSizeMB
		partSizeMB = maxPikPakTransferBufferMB / concurrency
		warnings = append(warnings, fmt.Sprintf(
			"configured concurrency=%d and part_size=%d MiB exceed the %d MiB per-reader buffer limit; using part_size=%d MiB",
			concurrency,
			configuredPartSizeMB,
			maxPikPakTransferBufferMB,
			partSizeMB,
		))
	}
	return pikPakTransferConfig{
		concurrency: concurrency,
		partSize:    partSizeMB * utils.MB,
	}, warnings
}

func readBoundedEnvInt(
	lookup func(string) (string, bool),
	name string,
	fallback int,
	minimum int,
	maximum int,
) (int, string) {
	raw, exists := lookup(name)
	raw = strings.TrimSpace(raw)
	if !exists || raw == "" {
		return fallback, ""
	}

	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || parsed < int64(minimum) || parsed > int64(maximum) {
		return fallback, fmt.Sprintf(
			"%s=%q is invalid; expected an integer from %d to %d, using %d",
			name,
			raw,
			minimum,
			maximum,
			fallback,
		)
	}
	return int(parsed), ""
}

func appendWarning(warnings []string, warning string) []string {
	if warning == "" {
		return warnings
	}
	return append(warnings, warning)
}

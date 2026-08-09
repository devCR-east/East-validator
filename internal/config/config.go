package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr         string
	DataDir          string
	NodeID           string
	NodeName         string
	APISecret        string
	KeepRecentBlocks int
	GenesisPath      string
	ShutdownTimeout  time.Duration
	BlockInterval    time.Duration // auto-seal empty blocks every N (default 120s)
	AutoProduce      bool          // enable auto block producer
	EpochSeconds     int64         // uptime epoch length (default 7 days)
}

func Load() Config {
	keepBlocks, _ := strconv.Atoi(getEnv("KEEP_RECENT_BLOCKS", "3000"))

	intervalSec, _ := strconv.Atoi(getEnv("BLOCK_INTERVAL_SEC", "120"))
	if intervalSec <= 0 {
		intervalSec = 120
	}

	autoProduce := getEnv("AUTO_PRODUCE", "true") != "false"

	// Default epoch = 1 week
	epochSec, _ := strconv.ParseInt(getEnv("EPOCH_SECONDS", "604800"), 10, 64)
	if epochSec <= 0 {
		epochSec = 604800
	}

	httpAddr := getEnv("HTTP_ADDR", "")
	if httpAddr == "" {
		if port := os.Getenv("PORT"); port != "" {
			httpAddr = ":" + port
		} else {
			httpAddr = ":8080"
		}
	}

	return Config{
		HTTPAddr:         httpAddr,
		DataDir:          getEnv("DATA_DIR", "./data"),
		NodeID:           getEnv("NODE_ID", "validator-1"),
		NodeName:         getEnv("NODE_NAME", "east-validator"),
		APISecret:        getEnv("API_SECRET", ""),
		KeepRecentBlocks: keepBlocks,
		GenesisPath:      getEnv("GENESIS_PATH", "./genesis.json"),
		ShutdownTimeout:  10 * time.Second,
		BlockInterval:    time.Duration(intervalSec) * time.Second,
		AutoProduce:      autoProduce,
		EpochSeconds:     epochSec,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

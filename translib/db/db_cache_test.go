package db

import (
	"context"
	"testing"
)

func setCacheConfig(t *testing.T, config map[string]string) {
	t.Helper()
	rc := RedisClient(ConfigDB)
	defer CloseRedisClient(rc)
	if err := rc.HSet(context.Background(), CACHE_CONFIG_TABLE, config).Err(); err != nil {
		t.Fatalf("Failed to set cache config: %v", err)
	}
	ReconfigureCache()
}

func deleteCacheConfig(t *testing.T) {
	t.Helper()
	rc := RedisClient(ConfigDB)
	defer CloseRedisClient(rc)
	if err := rc.Del(context.Background(), CACHE_CONFIG_TABLE).Err(); err != nil {
		t.Fatalf("Failed to delete cache config: %v", err)
	}
	ReconfigureCache()
}

func TestInitCacheConfig(t *testing.T) {
	dbCacheConfig = nil
	initCacheConfig()
	if dbCacheConfig == nil {
		t.Errorf("Expected cache config to be initialized, got: %v", dbCacheConfig)
	}
}

func TestClearCache(t *testing.T) {
	if err := ClearCache(); err != nil {
		t.Errorf("ClearCache returned an error: %v", err)
	}
}

func TestGetDbCacheConfigDefault(t *testing.T) {
	if existingConfig, err := readRedis(CACHE_CONFIG_TABLE); err == nil && len(existingConfig) > 0 {
		defer setCacheConfig(t, existingConfig)
		deleteCacheConfig(t)
	}

	cacheConfig := getDBCacheConfig()
	if cacheConfig.PerConnection != defaultDBCacheConfig.PerConnection {
		t.Errorf("Expected PerConnection to be %v, got: %v", defaultDBCacheConfig.PerConnection, cacheConfig.PerConnection)
	}
	if cacheConfig.Global != defaultDBCacheConfig.Global {
		t.Errorf("Expected PerConnection to be %v, got: %v", defaultDBCacheConfig.PerConnection, cacheConfig.PerConnection)
	}
}

func TestGetDbCacheConfigCustom(t *testing.T) {
	if existingConfig, err := readRedis(CACHE_CONFIG_TABLE); err == nil && len(existingConfig) > 0 {
		defer setCacheConfig(t, existingConfig)
	} else {
		defer deleteCacheConfig(t)
	}
	setCacheConfig(t, map[string]string{
		"per_connection_cache": "False",
		"global_cache":         "True",
		"@tables_cache":        "test1",
		"@no_tables_cache":     "test2",
		"@maps_cache":          "test3",
		"@no_maps_cache":       "test4",
	})

	cacheConfig := getDBCacheConfig()
	if cacheConfig.PerConnection != false {
		t.Errorf("Expected PerConnection to be %v, got: %v", false, cacheConfig.PerConnection)
	}
	if cacheConfig.Global != true {
		t.Errorf("Expected PerConnection to be %v, got: %v", true, cacheConfig.PerConnection)
	}
	if _, ok := cacheConfig.CacheTables["test1"]; !ok {
		t.Errorf("Expected CacheTables to contain 'test1', got: %v", cacheConfig.CacheTables)
	}
	if _, ok := cacheConfig.NoCacheTables["test2"]; !ok {
		t.Errorf("Expected NoCacheTables to contain 'test2', got: %v", cacheConfig.NoCacheTables)
	}
	if _, ok := cacheConfig.CacheMaps["test3"]; !ok {
		t.Errorf("Expected CacheMaps to contain 'test3', got: %v", cacheConfig.CacheMaps)
	}
	if _, ok := cacheConfig.NoCacheMaps["test4"]; !ok {
		t.Errorf("Expected NoCacheMaps to contain 'test4', got: %v", cacheConfig.NoCacheMaps)
	}
}

func TestGetDbCacheConfigNil(t *testing.T) {
	dbCacheConfig = nil
	getDBCacheConfig()
	if dbCacheConfig == nil {
		t.Errorf("Expected cache config to be initialized, got: %v", dbCacheConfig)
	}
}

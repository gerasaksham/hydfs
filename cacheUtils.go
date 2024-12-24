package main

import "time"

type Cache struct {
	cacheMap          map[string]CacheValue
	numberOfBytesUsed int
	byteCapacity      int
}

type CacheValue struct {
	value     string
	timestamp int64
}

func (c *Cache) get(key string) (string, bool) {
	value, ok := c.cacheMap[key]
	if !ok {
		return "", false
	}
	value.timestamp = getCurrentTime()
	return value.value, true
}

func (c *Cache) removeLeastRecentlyUsed() {
	minTimestamp := getCurrentTime()
	minKey := ""
	for key, value := range c.cacheMap {
		if value.timestamp < minTimestamp {
			minTimestamp = value.timestamp
			minKey = key
		}
	}
	c.remove(minKey)
}

func (c *Cache) remove(key string) {
	delete(c.cacheMap, key)
}

func (c *Cache) put(key string, value string) {
	bytesUsed := len(value)
	if c.numberOfBytesUsed+bytesUsed > c.byteCapacity {
		c.removeLeastRecentlyUsed()
	}
	c.cacheMap[key] = CacheValue{
		value:     value,
		timestamp: getCurrentTime(),
	}
}

func getCurrentTime() int64 {
	return time.Now().Unix()
}

package main

import (
	"log"
	"time"
	"flag"
	"math/big"
	"crypto/rand"
	"github.com/adastreamer/db36/internal/storage"
)

var (
	zero = big.NewInt(0)
)

func main() {
	path := flag.String("path", "./test.bl", "Blob file path (uri)")
	capacity := flag.Uint("capacity", 0, "Blob capacity (2^capacity records in total)")
	keySize := flag.Uint64("key", 0, "Key size in bytes")
	valueSize := flag.Uint64("value", 0, "Value size in bytes")
	n := flag.Uint64("n", 0, "Number of random values")

	flag.Parse()

	blob := &storage.Blob{
		Path: *path,
		Capacity: uint8(*capacity),
		KeySize: *keySize,
		ValueSize: *valueSize,
	}

	if err := blob.Init(); err != nil {
		log.Fatal(err)
	}

	maxKey := new(big.Int)
	maxKey.Exp(big.NewInt(2), big.NewInt(int64(*keySize * 8)), nil).Sub(maxKey, big.NewInt(1))
	log.Printf(" [*] Max key: %d", maxKey)

	maxValue := new(big.Int)
	maxValue.Exp(big.NewInt(2), big.NewInt(int64(*valueSize * 8)), nil).Sub(maxValue, big.NewInt(1))
	log.Printf(" [*] Max value: %d", maxValue)


	var keys[]big.Int
	var values[]big.Int
	var valuesB[][]byte

	i := *n
	for i > uint64(0) {
		key, err := rand.Int(rand.Reader, maxKey)
		if err != nil {
			log.Fatal(err)
		}
		for {
			if key.Cmp(zero) != 0 {
				break
			}
			key, err = rand.Int(rand.Reader, maxKey)
			if err != nil {
				log.Fatal(err)
			}
		}
		keys = append(keys, *key)
		value, err := rand.Int(rand.Reader, maxValue)
		if err != nil {
			log.Fatal(err)
		}
		for {
			if value.Cmp(zero) != 0 {
				break
			}
			value, err = rand.Int(rand.Reader, maxKey)
			if err != nil {
				log.Fatal(err)
			}
		}
		values = append(values, *value)
		valuesB = append(valuesB, value.Bytes())
		i -= uint64(1)
	}
	log.Printf(" [*] Generated %d keys and %d values", len(keys), len(values))

	start := time.Now()
	complexity := 0
	progress := float64(0)
	for i, key := range keys {
		address, iters, err := blob.Set(&key, &valuesB[i])
		if err != nil {
			log.Printf(" [**] Unsuccessful put value with key %s @ %d with complexity of %d, error: %v", key.String(), address, iters, err)
		}
		complexity += int(iters)
		if (float64(i) / float64(*n)) * float64(100) - progress >= 1 {
			progress = (float64(i) / float64(*n)) * float64(100)
			log.Printf(" [**] %d%%", uint64(progress))
		}
	}
	duration := time.Since(start)
	log.Printf(" [**] Successful put values in: %s, total compexity: %d", duration, complexity)


	tmp := new(big.Int)

	start = time.Now()
	complexity = 0
	for i, key := range keys {
		data, address, iters, err := blob.Get(&key)
		if err != nil {
			log.Printf(" [**] Unsuccessful get value @ %d with complexity of %d, error: %v", address, iters, err)
		}
		tmp.SetBytes(data)
		if tmp.Cmp(&values[i]) != 0 {
			log.Printf(" [**] Inconsistent data for key %d", key)
		}
		complexity += int(iters)
	}
	duration = time.Since(start)
	log.Printf(" [**] Successful read values in: %s, total compexity: %d", duration, complexity)
}

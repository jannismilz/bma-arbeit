package generator

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"math"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"
)

// GOAL: 1e9 in under 5 seconds
var GOAL int = 1e8
var CHUNK_SIZE int = GOAL / 10

func simpleSieve(limit int) []int {
	potPrimes := make([]bool, limit+1)
	var precomputedPrimes []int

	for i := 2; i <= limit; i++ {
		if potPrimes[i] == false {
			precomputedPrimes = append(precomputedPrimes, i)

			// Mark all multiples of i as non-prime
			for j := i * i; j <= limit; j += i {
				potPrimes[j] = true
			}
		}
	}

	return precomputedPrimes
}

func segmentedSieve(min int, max int, precomputedPrimes []int) []int {
	potPrimes := make([]bool, max-min+1)

	for _, precompPrime := range precomputedPrimes {
		firstMultiple := min / precompPrime

		if firstMultiple <= 1 {
			firstMultiple = precompPrime + precompPrime
		} else if (min % precompPrime) != 0 {
			firstMultiple = (firstMultiple * precompPrime) + precompPrime
		} else {
			firstMultiple = firstMultiple * precompPrime
		}

		for i := firstMultiple; i <= max; i += precompPrime {
			potPrimes[i-min] = true
		}
	}

	var finalPrimes []int
	for i := min; i <= max; i++ {
		if potPrimes[i-min] == false {
			finalPrimes = append(finalPrimes, i)
		}
	}

	return finalPrimes
}

func isPrime(prime int, primeList []int) bool {
	if prime <= 1 {
		return false
	}
	if prime == 2 || prime == 3 {
		return true
	}
	if prime%2 == 0 || prime%3 == 0 {
		return false
	}

	if slices.Contains(primeList, prime) {
		return true
	}

	sqrtPrime := int(math.Sqrt(float64(prime)))
	for _, precompPrime := range primeList {
		if precompPrime > sqrtPrime {
			break // No need to check beyond sqrt(prime)
		}
		if prime%precompPrime == 0 {
			return false
		}
	}

	if len(primeList) > 0 && primeList[len(primeList)-1] < sqrtPrime {
		for i := primeList[len(primeList)-1] + 2; i <= sqrtPrime; i += 2 {
			if prime%i == 0 {
				return false
			}
		}
	}

	return true
}

func getNextTwinPrime(prime int, primeList []int) int {
	if isPrime(prime+2, primeList) {
		return prime + 2
	}

	return 0
}

func isSafePrime(prime int, primeList []int) bool {
	safeCandidate := (prime - 1) / 2

	if safeCandidate <= 1 {
		return false
	}

	if slices.Contains(primeList, safeCandidate) {
		return true
	}

	return isPrime(safeCandidate, primeList)
}

func getNForMersenne(prime int) int {
	log := math.Log2(float64(prime + 1))
	if log == math.Floor(log) {
		return int(log)
	} else {
		return 0
	}
}

func getDigitSum(prime, base int) int {
	if base < 2 {
		panic("base must be >= 2")
	}

	sum := 0
	for prime > 0 {
		sum += prime % base
		prime /= base
	}
	return sum
}

type PrimeData struct {
	Index          int32 `parquet:"name=index, type=INT32"`
	Prime          int64 `parquet:"name=prime, type=INT64"`
	GapToPrevious  int32 `parquet:"name=gap_to_previous, type=INT32"`
	TwinPrime      int32 `parquet:"name=twin_prime, type=INT32"`
	IsSafePrime    bool  `parquet:"name=is_safe_prime, type=BOOLEAN"`
	MersenneK      int32 `parquet:"name=mersenne_k, type=INT32"`
	DigitSumBase10 int32 `parquet:"name=digit_sum_base10, type=INT32"`
	DigitSumBase2  int32 `parquet:"name=digit_sum_base2, type=INT32"`
	DigitSumBase16 int32 `parquet:"name=digit_sum_base16, type=INT32"`
}

func CreateParquetWriter(outputFile string) (*writer.ParquetWriter, error) {
	pf, err := local.NewLocalFileWriter(outputFile)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewParquetWriter(pf, new(PrimeData), 4)
	if err != nil {
		return nil, err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	return pw, nil
}

func WriteParquetBatch(pw *writer.ParquetWriter, data []PrimeData) error {
	for _, item := range data {
		if err := pw.Write(item); err != nil {
			return err
		}
	}

	return pw.Flush(true)
}

func CloseParquetWriter(pw *writer.ParquetWriter) error {
	return pw.WriteStop()
}

func GeneratePrimes() {
	start := time.Now()

	fmt.Printf("Starting data generation with a goal of %d...\n", GOAL)

	upToSqrt := int(math.Sqrt(float64(GOAL))) + 1

	fmt.Printf("Precomputing primes up to square root of goal: %d...\n", upToSqrt)

	precomputedPrimes := simpleSieve(upToSqrt)

	fmt.Println("Split goal into chunks of ranges...")

	chunks := getChunks(GOAL)

	fmt.Printf("%d chunks of ranges computed...\n", len(chunks))

	outputFile := "primes_data.parquet"
	parquetWriter, err := CreateParquetWriter(outputFile)
	if err != nil {
		fmt.Printf("Error creating parquet writer: %v\n", err)
		return
	}
	defer CloseParquetWriter(parquetWriter)

	// Create a mutex for synchronizing writes to the parquet file
	var writerMutex sync.Mutex

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	maxConcurrent := runtime.NumCPU() // Adjust based on the number of CPU cores
	semaphore := make(chan struct{}, maxConcurrent)

	total := 0
	var totalMutex sync.Mutex

	// Process chunks in parallel
	for chunkIndex, chunk := range chunks {
		wg.Add(1)

		// Acquire semaphore
		semaphore <- struct{}{}

		// Launch a goroutine to process this chunk
		go func(index int, chunkRange [2]int) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore when done

			min, max := chunkRange[0], chunkRange[1]
			fmt.Printf("Processing chunk %d/%d: [%d, %d]...\n", index+1, len(chunks), min, max)

			// Generate primes for this chunk
			primes := segmentedSieve(min, max, precomputedPrimes)
			chunkSize := len(primes)

			// Update total count (thread-safe)
			totalMutex.Lock()
			total += chunkSize
			totalMutex.Unlock()

			fmt.Printf("Mapping properties for %d primes in chunk %d...\n", chunkSize, index+1)
			primeData := mapPrimeProperties(primes, precomputedPrimes, 0) // Index doesn't matter for now

			fmt.Printf("Writing batch from chunk %d to parquet file...\n", index+1)
			writerMutex.Lock()
			err := WriteParquetBatch(parquetWriter, primeData)
			writerMutex.Unlock()

			if err != nil {
				fmt.Printf("Error writing batch from chunk %d: %v\n", index+1, err)
				return
			}

			elapsed := time.Since(start).Seconds()
			fmt.Printf("Chunk %d completed: %d primes processed in %.2fs\n", index+1, chunkSize, elapsed)
		}(chunkIndex, chunk)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	fmt.Printf("Completed! %d primes written to %s in %.2fs (%.2f primes/s)\n",
		total, outputFile, elapsed, float64(total)/elapsed)
}

func getChunks(limit int) [][2]int {
	var ranges [][2]int
	size := CHUNK_SIZE
	start := 2

	for start < limit {
		end := int(math.Min(float64(start+size), float64(limit)))
		ranges = append(ranges, [2]int{start, end})
		start += size
	}

	return ranges
}

func mapPrimeProperties(primes []int, precomputedPrimes []int, startIndex int) []PrimeData {
	data := make([]PrimeData, len(primes))

	// Set the previous prime for gap calculation
	prevPrime := 2
	if startIndex > 0 && len(precomputedPrimes) > 0 {
		prevPrime = precomputedPrimes[len(precomputedPrimes)-1]
	}

	for i, prime := range primes {
		pd := &data[i]
		pd.Index = int32(startIndex + i)
		pd.Prime = int64(prime)
		pd.GapToPrevious = int32(prime - prevPrime)

		prevPrime = prime

		twinPrime := getNextTwinPrime(prime, precomputedPrimes)
		if twinPrime != 0 {
			pd.TwinPrime = int32(twinPrime)
		}

		pd.IsSafePrime = isSafePrime(prime, precomputedPrimes)

		n := getNForMersenne(prime)
		if (1<<n)-1 == prime {
			pd.MersenneK = int32(n)
		}

		pd.DigitSumBase10 = int32(getDigitSumBase10(prime))
		pd.DigitSumBase2 = int32(getDigitSumBase2(prime))
		pd.DigitSumBase16 = int32(getDigitSumBase16(prime))
	}

	return data
}

func getDigitSumBase10(n int) int {
	sum := 0
	for n > 0 {
		sum += n % 10
		n /= 10
	}
	return sum
}

func getDigitSumBase2(n int) int {
	return getDigitSum(n, 2)
}

func getDigitSumBase16(n int) int {
	sum := 0
	hexStr := strconv.FormatInt(int64(n), 16)

	for _, ch := range hexStr {
		if ch >= '0' && ch <= '9' {
			sum += int(ch - '0')
		} else if ch >= 'a' && ch <= 'f' {
			sum += int(ch - 'a' + 10)
		} else if ch >= 'A' && ch <= 'F' {
			sum += int(ch - 'A' + 10)
		}
	}

	return sum
}

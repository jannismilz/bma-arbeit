package generator

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"math"
	"slices"
	"strconv"
	"time"
)

var GOAL int = 1e8
var CHUNK_SIZE int = GOAL / 10

// Sieve of Eratosthenes
func simpleSieve(limit int) []int {
	potPrimes := make([]bool, limit+1)
	var precomputedPrimes []int

	for i := 2; i <= limit; i++ {
		if potPrimes[i] == false {
			// This is a prime number, add it to our list
			precomputedPrimes = append(precomputedPrimes, i)

			// Mark all multiples of i as non-prime
			for j := i * i; j <= limit; j += i {
				potPrimes[j] = true
			}
		}
	}

	return precomputedPrimes
}

// Segmented Sieve of Eratosthenes
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

// Parallelize

func isPrime(prime int, primeList []int) bool {
	// Handle edge cases
	if prime <= 1 {
		return false
	}
	if prime == 2 || prime == 3 {
		return true
	}
	if prime%2 == 0 || prime%3 == 0 {
		return false
	}

	// Check if the number is in the precomputed list
	if slices.Contains(primeList, prime) {
		return true
	}

	// Check if the number is divisible by any precomputed prime
	sqrtPrime := int(math.Sqrt(float64(prime)))
	for _, precompPrime := range primeList {
		if precompPrime > sqrtPrime {
			break // No need to check beyond sqrt(prime)
		}
		if prime%precompPrime == 0 {
			return false
		}
	}

	// If we don't have enough precomputed primes, check up to sqrt(prime)
	if len(primeList) > 0 && primeList[len(primeList)-1] < sqrtPrime {
		for i := primeList[len(primeList)-1] + 2; i <= sqrtPrime; i += 2 {
			if prime%i == 0 {
				return false
			}
		}
	}

	return true
}

// Check if twin prime
func getNextTwinPrime(prime int, primeList []int) int {
	// Check if prime+2 is a twin prime
	if isPrime(prime+2, primeList) {
		return prime + 2
	}

	return 0
}

// Check if safe prime
func isSafePrime(prime int, primeList []int) bool {
	// A safe prime is a prime p where (p-1)/2 is also prime
	// This second prime is called a Sophie Germain prime
	safeCandidate := (prime - 1) / 2

	// Quick check for obvious non-primes
	if safeCandidate <= 1 {
		return false
	}

	// Check if the candidate is in the precomputed list
	if slices.Contains(primeList, safeCandidate) {
		return true
	}

	// Otherwise, check if it's prime
	return isPrime(safeCandidate, primeList)
}

// Check if Mersenne prime
func getNForMersenne(prime int) int {
	return int(math.Log2(float64(prime + 1)))
}

// Check digit sum in base
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

// Continuously write to parquet

// PrimeData represents the structure for prime number data
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

// CreateParquetWriter initializes a new Parquet writer for the given file path
// and returns the writer that can be used for multiple batch writes
func CreateParquetWriter(outputFile string) (*writer.ParquetWriter, error) {
	// Convert os.File to source.ParquetFile
	pf, err := local.NewLocalFileWriter(outputFile)
	if err != nil {
		return nil, err
	}

	// Create parquet writer with the PrimeData schema
	pw, err := writer.NewParquetWriter(pf, new(PrimeData), 4)
	if err != nil {
		return nil, err
	}

	// Set compression and encoding
	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	return pw, nil
}

// WriteParquetBatch writes a batch of prime data to the Parquet file
// while keeping the writer open for future batches
func WriteParquetBatch(pw *writer.ParquetWriter, data []PrimeData) error {
	for _, item := range data {
		if err := pw.Write(item); err != nil {
			return err
		}
	}

	// Optionally flush after each batch to ensure data is written
	return pw.Flush(true)
}

// CloseParquetWriter closes the Parquet writer and flushes any remaining data
func CloseParquetWriter(pw *writer.ParquetWriter) error {
	return pw.WriteStop()
}

// GOAL: 1e9 in under 5 seconds

// GeneratePrimes generates prime numbers and their properties up to GOAL and writes them to a Parquet file
func GeneratePrimes() {
	start := time.Now()

	fmt.Printf("Starting data generation with a goal of %d...\n", GOAL)

	// Calculate the square root of GOAL for precomputation
	upToSqrt := int(math.Sqrt(float64(GOAL))) + 1

	fmt.Printf("Precomputing primes up to square root of goal: %d...\n", upToSqrt)

	// Compute primes up to sqrt(GOAL) with simple sieve
	precomputedPrimes := simpleSieve(upToSqrt)

	fmt.Println("Split goal into chunks of ranges...")

	// Create chunks for processing
	chunks := getChunks(GOAL)

	fmt.Printf("%d chunks of ranges computed...\n", len(chunks))

	// Initialize the parquet writer
	outputFile := "primes_data.parquet"
	parquetWriter, err := CreateParquetWriter(outputFile)
	if err != nil {
		fmt.Printf("Error creating parquet writer: %v\n", err)
		return
	}
	defer CloseParquetWriter(parquetWriter)

	total := 0
	currentIndex := 0

	for chunkIndex, chunk := range chunks {
		min, max := chunk[0], chunk[1]
		fmt.Printf("Processing chunk %d/%d: [%d, %d]...\n", chunkIndex+1, len(chunks), min, max)

		// Generate primes for this chunk
		primes := segmentedSieve(min, max, precomputedPrimes)
		chunkSize := len(primes)
		total += chunkSize

		// Map primes to their properties
		fmt.Printf("Mapping properties for %d primes...\n", chunkSize)
		primeData := mapPrimeProperties(primes, precomputedPrimes, currentIndex)

		// Write this batch to the parquet file
		fmt.Println("Writing batch to parquet file...")
		err := WriteParquetBatch(parquetWriter, primeData)
		if err != nil {
			fmt.Printf("Error writing batch: %v\n", err)
			return
		}

		// Update the current index for the next chunk
		currentIndex += chunkSize

		// Print progress
		elapsed := time.Since(start).Seconds()
		fmt.Printf("Progress: %d primes processed in %.2fs (%.2f primes/s)\n", total, elapsed, float64(total)/elapsed)
	}

	fmt.Printf("Completed! %d primes written to %s in %.2fs\n", total, outputFile, time.Since(start).Seconds())
}

// Helper function to get chunks for processing
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

// mapPrimeProperties maps each prime number to its properties
func mapPrimeProperties(primes []int, precomputedPrimes []int, startIndex int) []PrimeData {
	data := make([]PrimeData, 0, len(primes))

	// Set the previous prime for gap calculation
	prevPrime := 2
	if startIndex > 0 && len(precomputedPrimes) > 0 {
		prevPrime = precomputedPrimes[len(precomputedPrimes)-1]
	}

	for i, prime := range primes {
		// Create a new PrimeData instance
		pd := PrimeData{
			Index:         int32(startIndex + i),
			Prime:         int64(prime),
			GapToPrevious: int32(prime - prevPrime),
		}

		// Update previous prime for next iteration
		prevPrime = prime

		// Check for twin prime
		twinPrime := getNextTwinPrime(prime, precomputedPrimes)
		if twinPrime != 0 {
			pd.TwinPrime = int32(twinPrime)
		}

		// Check if safe prime
		pd.IsSafePrime = isSafePrime(prime, precomputedPrimes)

		// Check if Mersenne prime
		n := getNForMersenne(prime)
		if (1<<n)-1 == prime {
			pd.MersenneK = int32(n)
		}

		// Calculate digit sums
		pd.DigitSumBase10 = int32(getDigitSumBase10(prime))
		pd.DigitSumBase2 = int32(getDigitSumBase2(prime))
		pd.DigitSumBase16 = int32(getDigitSumBase16(prime))

		// Add to data slice
		data = append(data, pd)
	}

	return data
}

// getDigitSumBase10 calculates the sum of digits in base 10
func getDigitSumBase10(n int) int {
	sum := 0
	for n > 0 {
		sum += n % 10
		n /= 10
	}
	return sum
}

// getDigitSumBase2 calculates the sum of digits in base 2 (binary)
func getDigitSumBase2(n int) int {
	return getDigitSum(n, 2)
}

// getDigitSumBase16 calculates the sum of digits in base 16 (hex)
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

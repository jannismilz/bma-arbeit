package generator

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	// "github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"math"
	"slices"
)

var GOAL int = 1e7
var CHUNK_SIZE int = GOAL / 100

// Sieve of Eratosthenes
func simpleSieve(limit int) []int {
	potPrimes := make([]bool, limit+1)

	for i := 2; i <= limit; i++ {
		if potPrimes[i] == false {
			// Schliesse alle Vielfachen von i aus
			for j := i * i; j <= limit; j += i {
				potPrimes[j] = true
			}
		}
	}

	var precomputedPrimes []int
	for i := 2; i <= limit; i++ {
		if potPrimes[i] == false {
			precomputedPrimes = append(precomputedPrimes, i)
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
	for _, precompPrime := range primeList {
		if prime%precompPrime == 0 {
			return false
		}
	}

	return true
}

// // Check if twin prime
func getNextTwinPrime(prime int, primeList []int) int {
	if isPrime(prime+2, primeList) {
		return prime + 2
	}

	return 0
}

// // Check if safe prime
func isSafePrime(prime int, primeList []int) bool {
	safeCandidate := (prime - 1) / 2

	if safeCandidate%2 == 0 || safeCandidate%5 == 0 {
		return false
	} else if slices.Contains(primeList, safeCandidate) {
		return false
	} else if isPrime(safeCandidate, primeList) {
		return false
	}

	return true
}

// // Check if Mersenne prime
func getNForMersenne(prime int) int {
	return int(math.Log2(float64(prime + 1)))
}

// // Check digit sum in base
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
	TwinPrime      int32 `parquet:"name=twin_prime, type=INT32, nullable=true"`
	IsSafePrime    bool  `parquet:"name=is_safe_prime, type=BOOLEAN"`
	MersenneK      int32 `parquet:"name=mersenne_k, type=INT32, nullable=true"`
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

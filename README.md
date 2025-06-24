# Code meiner BMA Arbeit

In diesem Repository findest du den nötigen Code für meine BMA Arbeit **"Mustererkennung in Primzahllücken mittels Informationstheorie"**.

## Prime Number Generator

This repository contains a high-performance prime number generator that can generate prime numbers up to 1 billion (10^9) and compute various prime-related properties:

- Gap to previous prime
- Twin primes
- Safe primes
- Mersenne primes
- Digit sums in base 10, 2, and 16

The generator uses parallel processing to efficiently generate and analyze prime numbers, and outputs the results to a Parquet file for further analysis.

## Requirements

- Go 1.18 or higher
- At least 16GB of RAM for generating primes up to 10^9
- Approximately 20GB of free disk space for the final Parquet file

## Installation

1. Install Go from [golang.org](https://golang.org/dl/)
2. Clone this repository:
   ```
   git clone https://github.com/jannismilz/bma-arbeit.git
   cd bma-arbeit
   ```
3. Install dependencies:
   ```
   go mod tidy
   ```

## Usage

To generate prime numbers up to the default goal (currently set to 10^9):

```
go run main.go
```

The program will:
1. Precompute primes up to the square root of the goal
2. Split the range into chunks for parallel processing
3. Generate primes in each chunk using a segmented sieve
4. Calculate properties for each prime
5. Write the results to an unsorted Parquet file
6. Sort the final Parquet file using the embedded DuckDB library
7. Remove the unsorted temporary file

## Configuration

You can modify the following constants in `generator/generator.go`:

- `GOAL`: The upper limit for prime generation (default: 10^9)
- `CHUNK_SIZE`: The size of each chunk for parallel processing (default: GOAL/100)

## Performance

The generator is optimized for performance with:
- Parallel chunk processing using goroutines
- Efficient prime checking algorithms
- Batch processing for better cache locality
- Optimized type conversions
- Efficient Parquet file writing
- Embedded DuckDB for fast sorting of the final dataset

On a modern multi-core system, it can generate and analyze primes up to 10^9 in a few minutes.

## Output

The generator produces a Parquet file named `primes_data.parquet` containing the following columns:

- `index`: Sequential index of the prime
- `prime`: The prime number
- `gap_to_previous`: Gap to the previous prime
- `twin_prime`: The twin prime (if exists, otherwise 0)
- `is_safe_prime`: Whether the prime is a safe prime
- `mersenne_k`: The k value if the prime is a Mersenne prime (2^k - 1)
- `digit_sum_base10`: Sum of digits in base 10
- `digit_sum_base2`: Sum of digits in base 2 (popcount)
- `digit_sum_base16`: Sum of digits in base 16

## Development

The code is structured as follows:

- `main.go`: Entry point with profiling setup
- `generator/generator.go`: Core prime generation and property calculation logic
- `generator/generator.py`: Python reference implementation

To profile the code:

```
go run main.go
go tool pprof -http=:8080 cpu.prof
```

## Dependencies

- [github.com/xitongsys/parquet-go](https://github.com/xitongsys/parquet-go): For Parquet file writing
- [github.com/marcboeker/go-duckdb](https://github.com/marcboeker/go-duckdb): Embedded DuckDB for sorting the final dataset

## License

This project is licensed under the MIT License - see the LICENSE file for details.

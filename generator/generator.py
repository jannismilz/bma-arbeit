import time
import math
from math import sqrt, floor
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 1 Milliarde
GOAL = 1e8

# 168

precomputed_primes = []

# Simple Sieve of Erat.
def simpleSieve(limit):
    mark = [True] * (limit + 1)
    p = 2

    while (p * p <= limit):
        if (mark[p] == True): 
            for i in range(p * p, limit + 1, p): 
                mark[i] = False  

        p += 1
        
    for p in range(2, limit + 1): 
        if mark[p]:
            precomputed_primes.append(p)


def get_chunks(limit):
    ranges = []
    # Chunk size
    size = int(GOAL / 100)
    start = 2

    while start < limit:
        end = int(min(start + size, limit))
        ranges.append((start, end))
        start += size
    
    return ranges

def segmentedSieve(min, max):
    primes = [True] * (max - min + 1)

    for precomp in precomputed_primes:
        first_multiple = (min // precomp)

        if first_multiple <= 1:
            first_multiple = precomp + precomp
        elif (min % precomp) != 0:
            first_multiple = (first_multiple * precomp) + precomp
        else:
            first_multiple = first_multiple * precomp

        for j in range(first_multiple, max + 1, precomp):
            primes[j - min] = False
            
    final_primes = []
    for k in range(min, max+ 1):
            if primes[k - min]:
                final_primes.append(k)

    return final_primes

def is_prime(prime):
    if prime in precomputed_primes:
        return True

    for precomp in precomputed_primes:
        if prime % precomp == 0:
            return False
    return True


def get_twin_prime(prime, prime_list):
    """Check if prime is part of a twin prime pair and return the twin if it exists"""
    if prime - 2 in precomputed_primes or prime - 2 in prime_list:
        return prime - 2
    elif prime + 2 in prime_list:
        return prime + 2
    return None


def check_safe_prime(prime, prime_list):
    """Check if (p-1)/2 is also prime"""
    safe_candidate = (prime - 1) // 2
    return safe_candidate in precomputed_primes or safe_candidate in prime_list


def get_mersenne_k(prime):
    """Check if prime is a Mersenne prime candidate (2^k - 1) and return k if true"""
    # A more efficient check: if prime = 2^k - 1, then prime + 1 = 2^k
    # So prime + 1 must be a power of 2, which means (prime + 1) & prime == 0
    if (prime + 1) & prime == 0:
        return int(math.log2(prime + 1))
    return None


# Process each prime to its other formats and properties
def map_prime_properties(prime_list, start_index=0):
    """
    Maps each prime number to its properties as specified in the requirements.
    
    Args:
        prime_list: List of prime numbers to process
        start_index: Starting index for the prime numbers in the sequence
        
    Returns:
        DataFrame with all the requested properties
    """
    # Initialize lists for each property
    data = {
        'index': [],
        'prime': [],
        'gap_to_previous': [],
        'twin_prime': [],
        'is_safe_prime': [],
        'mersenne_k': [],
        'bit_length': [],
        'digit_sum_base10': [],
        'digit_sum_base2': [],
        'is_palindrome_base10': [],
        'div_mod_10': []
    }
    
    # Set the previous prime for gap calculation
    prev_prime = 2 if start_index == 0 else precomputed_primes[start_index - 1]
    
    for i, prime in enumerate(prime_list):
        # index - position in the sequence
        index = start_index + i
        data['index'].append(index)
        
        # prime - the prime itself
        data['prime'].append(prime)
        
        # gap_to_previous - difference from the previous prime
        gap = prime - prev_prime
        data['gap_to_previous'].append(gap)
        prev_prime = prime
        
        # twin_prime - twin prime if one exists
        twin = get_twin_prime(prime, prime_list)
        data['twin_prime'].append(twin)
        
        # is_safe_prime - Is (p-1)/2 also prime?
        is_safe = check_safe_prime(prime, prime_list)
        data['is_safe_prime'].append(is_safe)
        
        # mersenne_k - If Mersenne candidate: which k? (else NULL)
        mersenne_k = get_mersenne_k(prime)
        data['mersenne_k'].append(mersenne_k)
        
        # bit_length - Number of bits needed to represent prime
        bit_length = prime.bit_length()
        data['bit_length'].append(bit_length)
        
        # digit_sum_base10 - Sum of digits in base 10
        digit_sum_base10 = sum(int(digit) for digit in str(prime))
        data['digit_sum_base10'].append(digit_sum_base10)
        
        # digit_sum_base2 - Sum of digits in base 2
        digit_sum_base2 = bin(prime).count('1')
        data['digit_sum_base2'].append(digit_sum_base2)
        
        # is_palindrome_base10 - Is the prime a palindrome?
        prime_str = str(prime)
        is_palindrome = prime_str == prime_str[::-1]
        data['is_palindrome_base10'].append(is_palindrome)
        
        # div_mod_10 - For exploratory analysis (e.g., does it end with 1, 3, 7, 9?)
        div_mod_10 = prime % 10
        data['div_mod_10'].append(div_mod_10)
    
    return data

# Define the schema for our parquet file
def create_parquet_schema():
    return pa.schema([
        ('index', pa.int32()),
        ('prime', pa.int64()),
        ('gap_to_previous', pa.int32()),
        ('twin_prime', pa.int32()),
        ('is_safe_prime', pa.bool_()),
        ('mersenne_k', pa.int32()),
        ('bit_length', pa.int32()),
        ('digit_sum_base10', pa.int32()),
        ('digit_sum_base2', pa.int32()),
        ('is_palindrome_base10', pa.bool_()),
        ('div_mod_10', pa.int32())
    ])

# Initialize the parquet writer
def initialize_parquet_writer(output_file):
    schema = create_parquet_schema()
    return pq.ParquetWriter(output_file, schema)

# Write a batch of data to the parquet file
def write_batch_to_parquet(writer, data):
    # Convert the dictionary to a PyArrow Table
    table = pa.Table.from_pydict(data, schema=writer.schema)
    writer.write_table(table)

start = time.time()

print(f"Starting data generation with a goal of {int(GOAL):_}...")

up_to_sqrt = floor(sqrt(GOAL)) + 1

print(f"Precomputing primes up to square root of goal: {up_to_sqrt:_}...")

# Compute up to sqrt(limit) with simple sieve
simpleSieve(up_to_sqrt)

print(f"Split goal into chunks of ranges...")

# Chunk the limit
chunks = get_chunks(GOAL)

print(f"{len(chunks)} chunks of ranges computed...")

print(chunks)

# Initialize the parquet writer
output_file = "primes_data.parquet"
parquet_writer = initialize_parquet_writer(output_file)

total = 0
current_index = 0

for chunk_index, chunk in enumerate(chunks):
    print(f"Processing chunk {chunk_index+1}/{len(chunks)}: {chunk}...")

    # Generate primes for this chunk
    primes = segmentedSieve(*chunk)
    chunk_size = len(primes)
    total += chunk_size
    
    # Map primes to their properties
    print(f"Mapping properties for {chunk_size} primes...")
    prime_data = map_prime_properties(primes, current_index)
    
    # Write this batch to the parquet file
    print(f"Writing batch to parquet file...")
    write_batch_to_parquet(parquet_writer, prime_data)
    
    # Update the current index for the next chunk
    current_index += chunk_size
    
    # Print progress
    elapsed = time.time() - start
    print(f"Progress: {total:_} primes processed in {elapsed:.2f}s ({total/elapsed:.2f} primes/s)")

# Close the parquet writer
parquet_writer.close()

print(f"Completed! {total:_} primes written to {output_file} in {time.time() - start:.2f}s")

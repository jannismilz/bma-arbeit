import time
from math import sqrt, floor

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

# Create a sieve for the range
# Use precomputed simple sieve to mark all non-primes

# Process each prime to it's other formats and properties

# Write that batch to a parquet file

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

total = 0

for chunk in chunks:
    print(f"Use segemented sieve on chunk {chunk}...")

    total += len(segmentedSieve(*chunk))

end = time.time()

print(f"{end - start}s in total")

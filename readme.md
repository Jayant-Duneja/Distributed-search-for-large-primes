# DS Project: Finding Large Prime Numbers

## How to run

```
usage: main.py [-h] [--starting_number STARTING_NUMBER] [--k K] [--n_procs N_PROCS]

optional arguments:
  -h, --help            show this help message and exit
  --starting_number STARTING_NUMBER
                        starting number to look for primes
  --k K                 interations count of rabin-miller. Approx probability of failing = 1/(2^(2k))
  --n_procs N_PROCS     number of worker processes
```
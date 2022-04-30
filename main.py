import os
import multiprocessing
import random
import argparse


def primality_test_worker(job_q: multiprocessing.Queue, result_q: multiprocessing.Queue, k: int):
    """ A worker function to be launched in a separate process. 
        Takes jobs from job_q - each job a list of numbers to be test for primality.
        The result (n if n is prime else -1) is placed into result_q.
        Stops with keyboardInterrupt or SystemExit
    """

    def rabin_miller_test(n):
        # find r and s
        s = 0
        r = n - 1
        while r & 1 == 0:
            s += 1
            r //= 2

        # do k tests (each test has the chance of 75% of finding if composite)
        for _ in range(k):
            a = random.randrange(2, n - 1)
            x = pow(a, r, n)
            if x != 1 and x != n - 1:
                j = 1
                while j < s and x != n - 1:
                    x = pow(x, 2, n)
                    if x == 1:
                        return False
                    j += 1
                if x != n - 1:
                    return False
        return True

    # Run until keyboard interrupt
    while True:

        try:
            number_to_test = job_q.get(True)  # Blocking statement
            is_prime = rabin_miller_test(number_to_test)
            result_q.put(number_to_test if is_prime else -1, True)

        except (KeyboardInterrupt, SystemExit):
            print(f"Exiting Worker with pid: {os.getpid()}")
            break


def run_sieve(limit):
    """ Sieve of Eratosthenes
    """

    primes = set()
    is_prime = [True for _ in range(0, limit+1)]
    max_prime = 2

    for i in range(2, limit):
        if is_prime[i]:
            primes.add(i)
            max_prime = i

            multiplier = 2
            while i*multiplier < limit:
                is_prime[i*multiplier] = False
                multiplier += 1

    return primes, max_prime


def get_possible_number(bits, primes):
    """ Get n-bits random number to be tested by rabin-miller
        Performs basic pre-checks of primality
    """

    while True:
        number = random.getrandbits(bits) + 1

        # apply a mask to set MSB 1 to get bits lengtgh
        number |= (1 << bits - 1)

        # apply a mask to set LSB 1 to get odd number
        number |= 1

        # check if initial primes divide the number
        if not any([number % prime == 0 for prime in primes]):
            break

    return number


def run_server(starting_number, k, n_procs, bit_step,excess_numbers=3, sieve_limit=1000):
    """ Server process to initiate the workers and queues.
        starting_number -> start testing for primes from this number
        k -> number of times to run rabin-miller
        n_procs -> number of workers processes to initiate
    """

    # initial primes and max_prime initialization
    initial_primes, max_prime = run_sieve(sieve_limit)
    max_prime = max(max_prime, starting_number)

    # start manager process and initiate queues
    manager = multiprocessing.Manager()
    job_queue = manager.Queue()
    result_queue = manager.Queue()

    # fill the job queue with (nprocs + excess_numbers) numbers to test
    bit_length = max_prime.bit_length() + 1
    for _ in range(n_procs + excess_numbers):
        job_queue.put(get_possible_number(bit_length, initial_primes))

    # initialize nprocs Processes
    procs = []
    for _ in range(n_procs):
        procs.append(multiprocessing.Process(target=primality_test_worker, args=(job_queue, result_queue, k)))
        procs[-1].start()

    # continously check for bigger primes and end with keyboard interrupt
    while True:

        try:
            result = result_queue.get(True)

            # if found new prime, increase the bits of numbers to test
            if result != -1 and result > max_prime:
                max_prime = result
                bit_length = max_prime.bit_length() + bit_step
                print(f"Found bigger prime: {max_prime}")

            job_queue.put(get_possible_number(bit_length, initial_primes))

        except (KeyboardInterrupt, SystemExit):

            for proc in procs:
                proc.join()

            print(f"Max Prime Found Till Now: {max_prime}")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--starting_number', type=int, default=1, help="starting number to look for primes")
    parser.add_argument('--k', type=int, default=128, help="interations count of rabin-miller. Approx probability of failing = 1/(2^(2k))")
    parser.add_argument('--bit_step', type=int, default=1, help="Bit steps to take after successfully finding one prime")
    parser.add_argument('--n_procs', type=int, default=multiprocessing.cpu_count()-1, help='number of worker processes')
    args = parser.parse_args()

    run_server(args.starting_number, args.k, args.n_procs, args.bit_step)

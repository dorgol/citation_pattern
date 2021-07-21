import shortest_path
import concurrent.futures
import time

start = time.perf_counter()

def do_something(end_year):
    a = shortest_path.main_op(end_year)
    return a


if __name__ == '__main__':

    with concurrent.futures.ProcessPoolExecutor() as executor:
        end_year = [1990, 1991, 1992]
        results = executor.map(do_something, end_year)

        # for result in results:
        #     print(result)


finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')


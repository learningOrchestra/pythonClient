from time import time


class Timer:
    def __init__(self):
        self.time = None
        self.elapsed_time = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        print(f'elapsed time: {self.elapsed_time} seconds')

    def start(self):
        self.time = time()
        print('time started !')
        return self

    def stop(self):
        self.elapsed_time = time() - self.time

    def partial(self, name):
        print(f'elapsed time until {name}: {time() - self.time} seconds')

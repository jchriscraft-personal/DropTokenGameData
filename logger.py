import datetime


default_log_file = './log.txt'

class Log:

    def __init__(self, log_file: str = default_log_file):
        self.file_handle = open(log_file, 'a')

    
    def __del__(self):
        if self.file_handle:
            self.file_handle.close()


    def write_info(self, message: str):
        self.file_handle.write(f'INFO: {datetime.datetime.now()} {message}\n')
        self.file_handle.flush()


    def write_warning(self, message: str):
        self.file_handle.write(f'WARNING: {datetime.datetime.now()} {message}\n')
        self.file_handle.flush()


    def write_error(self, message: str):
        self.file_handle.write(f'ERROR: {datetime.datetime.now()} {message}\n')
        self.file_handle.flush()


    def write_metric(self, metric_name: str, value: float):
        self.file_handle.write(f'METRIC: {datetime.datetime.now()} {metric_name}: {value}\n')
        self.file_handle.flush()





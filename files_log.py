from os.path import isfile


class FilesLog:
    def __init__(self, log):
        self.log = log
    def add_entry(self,file_basename):
        if file_basename not in self.read_entries():
            with open(self.log, 'a+') as file:
                file.write(file_basename+'\n')

    def read_entries(self):
        if not isfile(self.log):
            return []
        else:
            with open(self.log) as file:
                return file.read().splitlines()

class RemoteFileSizes:
    def __init__(self, filename):
        sizes = {}
        with open(filename) as file:
            for line in file.read().splitlines():
                name, size = line.split()
                sizes[name] = int(size)
        self.sizes = sizes
    def getSizes(self):
        return self.sizes
    def getSumOfDaily(self, monthly):
        sum = 0
        for filename, size in self.sizes.items():
            if (monthly+'-') in filename:
                sum += size
        return sum

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

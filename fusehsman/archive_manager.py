import tarfile


def make_tarfile(name):
    with tarfile.open(name, "w:gz"):
        pass

def add_file(name, )
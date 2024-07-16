import os

lock_file = "lock_file"
with open(
        os.open(path=lock_file, flags=(os.O_WRONLY | os.O_CREAT), mode=0o666),
        "w") as f:
    f.write("This is autogen lock file\n")

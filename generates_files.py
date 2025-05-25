import os

sizes_mb = [10, 50, 100]

for size in sizes_mb:
    filename = f"testfile_{size}MB.dat"
    print(f"Creating {filename} ({size}MB)...")
    with open(filename, 'wb') as f:
        f.write(os.urandom(size * 1024 * 1024)) 

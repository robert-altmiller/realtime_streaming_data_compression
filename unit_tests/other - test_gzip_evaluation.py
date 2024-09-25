import os, gzip

# Path to your original JSON file
original_file = './event-hub/data/event-hub-capture/output_file_20240925_121029.json'

# Read the original file
with open(original_file, 'r') as f:
    json_data = f.read()

# Further compress the file using gzip
gzip_file = 'output_file_20240925_121029.json.gz'
with gzip.open(gzip_file, 'wt', encoding='utf-8') as gz_file:
    gz_file.write(json_data)

# Get file sizes
original_size = os.path.getsize(original_file)
gzip_size = os.path.getsize(gzip_file)

# Calculate compression ratio
compression_ratio = 100 * (original_size - gzip_size) / original_size

print(f"Original Size: {original_size / 1024:.2f} KB")
print(f"Gzip Compressed Size: {gzip_size / 1024:.2f} KB")
print(f"Compression Savings: {compression_ratio:.2f}%")

import time
import sys

print("Starting the test...")
for i in range(1, 6):
    print(f"Log message {i} - waiting...")
    # We sleep to show that the text DOES NOT appear immediately
    time.sleep(1) 
print("Finished!")
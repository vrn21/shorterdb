import random
import string
import csv

def generate_random_word(length=20):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

def generate_random_number(min_val=1000, max_val=9999):
    return random.randint(min_val, max_val)

def generate_dataset(size=1000000000, filename='data.csv'):
    # Open the CSV file for writing
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Key', 'Value'])  # Write header

        dataset_keys = set()  # To keep track of unique keys

        for _ in range(size):
            data_type = random.choice(['num_word', 'word_num', 'word_word', 'num_num'])

            if data_type == 'num_word':
                key = generate_random_number()
                value = generate_random_word()
            elif data_type == 'word_num':
                key = generate_random_word()
                value = generate_random_number()
            elif data_type == 'word_word':
                key = generate_random_word()
                value = generate_random_word()
            else:  # num_num
                key = generate_random_number()
                value = generate_random_number()

            # Ensure key is unique
            # while key in dataset_keys:
            #     if isinstance(key, int):
            #         key = generate_random_number()
            #     else:
            #         key = generate_random_word()

            # Add the unique key to the set and write to CSV immediately
            # dataset_keys.add(key)
            writer.writerow([key, value])

# Generate the dataset and write it to a CSV file
generate_dataset(1000000000)

print(f"Dataset has been written to data.csv")

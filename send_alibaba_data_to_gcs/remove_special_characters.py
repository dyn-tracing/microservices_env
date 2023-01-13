import csv
import sys
import re

def process(filename, newfilename):
    with open(newfilename, 'w') as new_file:
        writer = csv.writer(new_file, delimiter=' ')
        with open(filename, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in reader:
                new_row = []
                for obj in row:
                    cleaned_obj = ""
                    for c in obj:
                        if (ord(c) >= ord('a') and ord(c) <= ord('z')) or \
                            (ord(c) >= ord('A') and ord(c) <= ord('Z')) or \
                            c == ' ':
                            cleaned_obj += c
                        else:
                            cleaned_obj += 'a'
                    new_row.append(cleaned_obj)
                writer.writerow(new_row)

def main():
    if len(sys.argv) != 3:
        print("usage: python3 remove_special_characters.py original_file new_file")
    process(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    main()

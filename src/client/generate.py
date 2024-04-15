import csv
import random
from typing import List

from dataclasses import dataclass

fnames: List[str] = []
lnames: List[str] = []

with open('data/fnames.txt', 'r') as file:
    for line in file:
        fnames.append(line.strip())

with open('data/lnames.txt', 'r') as file:
    for line in file:
        lnames.append(line.strip())

all_names = [f'{f} {l}' for f in fnames for l in lnames]

@dataclass
class Person:
    id: int
    name: str
    marks: int


def generate_people(n = len(all_names)) -> List[Person]:
    people = []
    names = random.sample(all_names, n)
    for i, name in zip(range(n), names):
        marks = random.randint(0, 100)
        people.append(Person(i, name, marks))
    return people


all_data = generate_people()

# Write to CSV
with open('data/people.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['id', 'name', 'marks'])
    for person in all_data:
        writer.writerow([person.id, person.name, person.marks])


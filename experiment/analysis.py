import os
import csv

name = "result"
project_root = os.path.dirname(os.path.dirname(__file__))
experiment_dir = os.path.dirname(__file__)
result_dir = os.path.join(project_root, name)


def parse(filename):
    args = filename.split("-")
    data = args[0]
    algorithm = args[1]
    server = args[2]
    replica = args[3]
    node = args[4]

    with open(os.path.join(result_dir, filename)) as f:
        reader = csv.reader(f)
        result = []
        for row in reader:
            result = row
        if result:
            cost = str(row[0])
            time = str(float(row[1]) / 1000.)
        return [data, algorithm, server, replica, node, cost, time]


def main():
    header = ["data", "algorithm", "server", "replica", "node", "cost", "time"]
    with open(os.path.join(experiment_dir, name + ".csv"), "w") as f:
        f.write(",".join(header) + "\n")
        for filename in sorted(os.listdir(result_dir)):
            row = parse(filename)
            f.write(",".join(row) + "\n")


if __name__ == '__main__':
    main()

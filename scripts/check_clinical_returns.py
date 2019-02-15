import argparse
import csv
import sys
import os


def compare_files(ukrr, uktr):
    with open(ukrr) as r, open(uktr) as t:
        csv_r = csv.reader(r)
        next(csv_r)
        ukrr_ids = set()
        for r in csv_r:
            ukrr_ids.add(r[0])
        csv_t = csv.reader(t)
        next(csv_t)
        uktr_ids = set()
        for r in csv_t:
            if r[1] != "":
                uktr_ids.add(r[1])
        diff = uktr_ids - ukrr_ids
        print(len(diff))
        print(sorted(diff))
        diff = ukrr_ids - uktr_ids
        print("==========================")
        print(len(diff))
        print(sorted(diff))


def main():
    parser = argparse.ArgumentParser(description='compare matched to clinical returns')
    parser.add_argument(
        '--ukrr',
        type=str,
        help="Specify UKRR returns file",
        required=True)
    parser.add_argument(
        '--uktr',
        type=str,
        help="Specify UKTR clinical returns file",
        required=True)
    if not len(sys.argv) > 1:
        parser.print_help()
        return
    args = parser.parse_args()
    if not os.path.exists(args.ukrr):
        print(f"File {args.ukrr} must exist")
        sys.exit()
    if not os.path.exists(args.uktr):
        print(f"File {args.uktr} must exist")
        sys.exit()

    compare_files(args.ukrr, args.uktr)


if __name__ == '__main__':
    main()

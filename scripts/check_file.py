fp = 'Q:\\NHSBT\\2018-07-30\\UKTR_DATA_27JUL2018.csv'

with open(fp) as fh:
    header = next(fh)
    c = header.count(',')
    for x, l in enumerate(fh):
        c2 = l.count(',')
        if c != c2:
            print(f"check line {x} {c} {c2}")
            print(l)

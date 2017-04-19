from datetime import datetime


def convert_datetime_string_to_datetime(d):
    date_formats = ["%d/%m/%y", "%d-%m-%Y", "%d%b%Y"]
    for format in date_formats:
        try:
            return datetime.strptime(d, format)
        except:
            continue
    print("No datetime formats found for {0}".format(d))
    return None

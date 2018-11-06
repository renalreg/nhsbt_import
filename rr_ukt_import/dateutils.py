from datetime import datetime
import logging


def convert_datetime_string_to_datetime(d):
    log = logging.getLogger('ukt_match')
    # check for two digit years
    try:
        dt = datetime.strptime(d, '%d-%b-%y')
        if dt is not None:
            if dt.year > 2000:
                dt = dt.replace(year=dt.year-100)
            return dt
    except Exception:
        pass
    date_formats = ["%d/%m/%y", "%d-%m-%Y", "%d%b%Y", "%d/%m/%Y"]
    for format in date_formats:
        try:
            return datetime.strptime(d, format)
        except Exception:
            continue
    log.warning("No datetime formats found for %s", d)
    return None

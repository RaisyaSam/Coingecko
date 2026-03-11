from datetime import date, timedelta

def last_n_days_range(n_days: int, end: date | None = None) -> tuple[date, date]:
    end_d = end or date.today()
    start_d = end_d - timedelta(days=n_days - 1)
    return start_d, end_d
from datetime import datetime, timedelta

class TimeUtil:

    @classmethod
    def add_time_to_date(cls,date_str: str, time_str: str = "07:00:00") -> str:
        """
        Add time to date
        """
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.strftime('%Y-%m-%d')} {time_str}"

    @classmethod
    def generate_dates(cls,start_date_str, end_date_str, time_str="07:00:00"):
        start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        delta = (end - start).days

        return [
            f"{(start + timedelta(days=i)).strftime('%Y-%m-%d')} {time_str}"
            for i in range(delta + 1)
        ]
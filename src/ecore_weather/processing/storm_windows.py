"""UTC comparison windows shared by the forecast audit and demo."""
from datetime import timedelta

from ecore_weather.core.common import utc

STORMS = {
    'fred-2021': ('Fred', '2021-08-10T12:00Z', '2021-08-11T16:00Z'),
    'grace-2021': ('Grace', '2021-08-15T06:00Z', '2021-08-16T06:00Z'),
    'earl-2022': ('Earl', '2022-09-03T18:00Z', '2022-09-05T12:00Z'),
    'fiona-2022': ('Fiona', '2022-09-17T18:00Z', '2022-09-20T00:00Z'),
    'philippe-2023': ('Philippe', '2023-10-03T06:00Z', '2023-10-04T14:00Z'),
    'ernesto-2024': ('Ernesto', '2024-08-13T16:00Z', '2024-08-14T18:00Z'),
    'erin-2025': ('Erin', '2025-08-16T16:00Z', '2025-08-17T18:00Z'),
}


def storm_hours(start, end):
    current = utc(start)
    while current < utc(end):
        yield current
        current += timedelta(hours=1)


def flash_windows(hour):
    hour = utc(hour)
    return [
        (hour + timedelta(minutes=offset),
         tuple(hour + timedelta(minutes=offset + lead) for lead in range(-60, 0, 10)))
        for offset in (-10, 0, 10)
    ]

def create_pid(row: dict) -> str:
    if saon := row["saon"]:
        return f'{row["postcode"]}, {row["paon"]}, {saon}'
    else:
        return f'{row["postcode"]}, {row["paon"]}'


def add_pid(row: dict) -> str:
    row["pid"] = create_pid(row)
    return row

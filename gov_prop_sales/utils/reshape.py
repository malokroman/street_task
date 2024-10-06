def reshape(row) -> dict:
    pid, transactions = row
    property_fields = (
        "postcode",
        "property_type",
        "paon",
        "saon",
        "street",
        "locality",
        "town_or_city",
        "district",
        "county",
    )
    transaction_fields = (
        "tid",
        "price",
        "date",
        "is_new",
        "freehold_or_leasehold",
        "ppd_category_type",
        "record_status",
    )
    property_info = {field: transactions[0][field] for field in property_fields}

    property_info["transactions"] = [
        {field: transaction[field] for field in transaction_fields}
        for transaction in transactions
    ]

    return property_info

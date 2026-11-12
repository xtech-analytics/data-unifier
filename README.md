# Unifier

Unifier is a Python package that provides a simple interface to interact with the Unifier API, allowing for Point-In-Time and streaming data access.

## Features

- Query the Unifier API for data.
- Stream query results row by row as they arrive.
- Retrieve data in JSON format.
- Convert API responses into pandas DataFrames for easy data manipulation and analysis.
- Efficiently replicate large datasets to local storage using rclone integration.
- List and retrieve detailed metadata for datasets in the catalog.
- Check which datasets you are entitled to, now or as of any past date or range.

## Installation

You can install the package via pip:

```bash
pip install unifier
```

Requires Python 3.10 or later.

To use the **Data Replication** feature, it is recommended to install [rclone](https://rclone.org/downloads/) for better performance. If `rclone` is not available, the package will automatically fall back to a native Python implementation for downloads.

## Usage

Here's a basic example of how to use the unifier package:

```python
from unifier import unifier

# Set your user and token
unifier.user = 'your_username'
unifier.token = 'your_api_token'

# Query the API. Arguments are passed by name and sent to the server as
# given; unifier.help() prints the parameters the server accepts.
response = unifier.query(name='your_dataset_name', limit=10)

# Stream rows as they arrive
for row in unifier.stream(name='your_dataset_name'):
    print(row)

# Convert the response to a DataFrame
df = unifier.get_dataframe(name='your_dataset_name')

print(df.head())

# Get list asof_date available for a dataset
dates_df = unifier.get_asof_dates(name='dataset_name')
print(dates_df.head())

# List the datasets in the catalog you are entitled to
datasets = unifier.list_data_catalog()
print(datasets)

# Include publicly available datasets alongside your entitled ones
all_datasets = unifier.list_data_catalog(show_all_datasets=True)

# Optionally limit the public datasets to a single product
product_datasets = unifier.list_data_catalog(
    show_all_datasets=True,
    product_type='product_name'
)

# Get detailed metadata for a specific dataset
details = unifier.get_dataset_details(name='dataset_name')
print(details)

# Look up a dataset you have no entitlement for, if it is publicly displayed
public_details = unifier.get_dataset_details(
    name='dataset_name',
    show_all_datasets=True
)

# List the datasets you are entitled to right now
entitled = unifier.list_entitled_datasets()
print(entitled)

# Entitlements as of a past date, or at any point across a range
entitled_then = unifier.list_entitled_datasets(date='2024-03-15')
entitled_range = unifier.list_entitled_datasets(
    from_date='2024-01-01',
    to_date='2024-06-30'
)

# Replicate a large dataset to a local folder
# Note: uses rclone if installed, otherwise uses native python download
unifier.replicate(
    name="large_dataset_name",
    target_location="./data/downloads",
    back_to="2023-01-01",    # Optional: fetch history back to this date
    up_to="2024-12-16",    # Optional: fetch history up to this date
    bandwidth_limit=20       # Optional: limit download speed to 20MB/s
)
```

## Configuration

Before using the package, ensure you set your `user` and `token` attributes in the `unifier` class to authenticate with the API.

All arguments are keyword-only. Positional arguments still work for now but are deprecated and print a warning naming the equivalent keyword call.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) for more information.

## Support

For support, please contact [support@exponential-tech.ai](mailto:support@exponential-tech.ai).

## Acknowledgments

- Thanks to the team at Exponential Tech for their support and contributions.

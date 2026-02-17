# Unifier

Unifier is a Python package that provides a simple interface to interact with the Unifier API, allowing for Point-In-Time and streaming data access.

## Features

- Query the Unifier API for data.
- Retrieve data in JSON format.
- Convert API responses into pandas DataFrames for easy data manipulation and analysis.
- Efficiently replicate large datasets to local storage using rclone integration.
- List and retrieve detailed metadata for data catalogs.

## Installation

You can install the package via pip:

```bash
pip install unifier
```

To use the **Data Replication** feature, it is recommended to install [rclone](https://rclone.org/downloads/) for better performance. If `rclone` is not available, the package will automatically fall back to a native Python implementation for downloads.

## Usage

Here's a basic example of how to use the unifier package:

```python
from unifier import unifier

# Set your user and token
unifier.user = 'your_username'
unifier.token = 'your_api_token'

# Query the API
response = unifier.query(name='your_dataset_name')

# Convert the response to a DataFrame
df = unifier.get_dataframe(name='your_dataset_name')

print(df.head())

# Get list asof_date available for a dataset
dates_df = unifier.get_asof_dates(name='dataset_name')
print(dates_df.head())

# List all available data catalogs
catalogs = unifier.list_data_catalogs()
print(catalogs)

# Get detailed metadata for a specific catalog
catalog = unifier.get_data_catalog(name='dataset_name')
print(catalog)

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

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) for more information.

## Support

For support, please contact [support@exponential-tech.ai](mailto:support@exponential-tech.ai).

## Acknowledgments

- Thanks to the team at Exponential Tech for their support and contributions.

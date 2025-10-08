# Unifier

Unifier is a Python package that provides a simple interface to interact with the Unifier API, allowing for Point-In-Time and streaming data access.

## Features

- Query the Unifier API for data.
- Retrieve data in JSON format.
- Convert API responses into pandas DataFrames for easy data manipulation and analysis.

## Installation

You can install the package via pip:

```bash
pip install unifier
```

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
df = unifier.get_asof_dates(name='dataset_name')
print(df.head())
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

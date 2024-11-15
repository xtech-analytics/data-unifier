import requests
import pandas as pd

class Unifier:
    """A class to interact with the Unifier API."""
    
    url = 'https://unifier.exponential-tech.ai/unifier'
    user = ''
    token = ''

    @classmethod
    def query(cls, name, user=None, token=None, key=None, as_of=None, back_to=None, up_to=None, asof_date=None, limit=None):
        """Query the Unifier API and return the response data."""
        
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'name': name,
            'user': cls.user,
            'token': cls.token,
        }

        if limit:
            payload['limit'] = limit
        
        if key is not None:
            payload['key'] = key
        if token is not None:
            payload['token'] = token
        if user is not None:
            payload['user'] = user
        
        # as_of param is deprecated and will be removed in future
        if as_of is not None:
            payload['up_to'] = as_of
        if back_to is not None:
            payload['back_to'] = back_to
        if up_to is not None:
            payload["up_to"] = up_to
        if asof_date is not None:
            payload['asof_date'] = asof_date

        try:
            response = requests.post(cls.url, headers=headers, json=payload)
            response.raise_for_status()
            response_data = response.json()
            if 'error' in response_data:
                print("Error:", response_data['error'])
                return {}
            return response_data
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {}

    @classmethod
    def get_dataframe(cls, name, user=None, token=None, key=None, as_of=None, back_to=None, up_to=None, asof_date=None, limit=None):
        """Get the query result as a pandas DataFrame."""
        
        json_result = cls.query(name, user, token, key, as_of, back_to, up_to, asof_date, limit)
        return pd.DataFrame.from_dict(json_result)

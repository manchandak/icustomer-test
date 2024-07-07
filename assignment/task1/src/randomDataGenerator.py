import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Constants
num_records = 1000
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
actions = ['view', 'click', 'add_to_cart', 'purchase']

# Helper functions
def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

# Generating sample data
data = {
    'interaction_id': range(1, num_records + 1),
    'user_id': np.random.randint(1000, 1100, size=num_records),
    'product_id': np.random.randint(200, 300, size=num_records),
    'action': np.random.choice(actions, size=num_records),
    'timestamp': [random_date(start_date, end_date) for _ in range(num_records)]
}

# Creating DataFrame
df = pd.DataFrame(data)

# Converting timestamp to string
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Saving to CSV
csv_path = '../input/interaction_data.csv'
df.to_csv(csv_path, index=False)

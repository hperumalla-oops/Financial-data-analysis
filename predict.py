import torch
import torch.nn as nn
import mysql.connector
import json
import pytz
import pickle
import time
import logging
import numpy as np
from google.cloud import pubsub_v1
from config import pubsub_topics, time_zone, gcp_project_id
from datetime import datetime, timedelta
from mysql.connector import errorcode
from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name
from create_database import join_statement as db_x_query
from biGRU_model import BiGRU

subscriber = pubsub_v1.SubscriberClient()
subscription_path = pubsub_topics['predict_timestamp'].replace('topics', 'subscriptions') + '-sub'

# Specify target labels
y_fields = "up1, up2, down1, down2".split(", ")

publisher = pubsub_v1.PublisherClient()
print("✅✅✅✅✅✅✅✅✅✅ line 25")

# Connect to MySQL server
cnx = mysql.connector.connect(host=mysql_hostname, user=mysql_user, password=mysql_password)

try:
    cnx = mysql.connector.connect(host=mysql_hostname, user=mysql_user, password=mysql_password)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("User name or password incorrect")
    else:
        print(err)
    cnx.close()
    print('Connection closed')

# Instantiate cursor object
cursor = cnx.cursor()

# Use given database
cursor.execute("USE {};".format(mysql_database_name))
print("✅✅✅✅✅✅✅✅✅✅ line 45")

# Extract x_fields from db_query
# To use different set of columns modify SQL db_x_query
db_x_query = [w.strip(",") for w in db_x_query.split()]
fields_start_idx = db_x_query.index("SELECT")
fields_end_idx = db_x_query.index("FROM")
x_fields = ", ".join(db_x_query[fields_start_idx + 1: fields_end_idx]).strip(", ")

# Extract FROM table statement
from_start_idx = db_x_query.index("FROM")
from_statement = " ".join(db_x_query[from_start_idx:]).strip(";")

# Load Pytorch model for Inference
# Initialize parameters (use the same parameters except batch_size as during training)
window = 5
batch_size = 1
hidden_size = 8
n_features = len(x_fields.split(", "))
output_size = len(y_fields)
n_layers = 1
clip = 50
dropout = 0.2
learning_rate = 0.001
epochs = 20
spatial_dropout = False
prob_threshold = 0.5

# Check whether system supports CUDA
CUDA = torch.cuda.is_available()

model = BiGRU(hidden_size, n_features, output_size, n_layers, clip, dropout,
              spatial_dropout, bidirectional=True)

# Move the model to GPU if possible
if CUDA:
    model.cuda()

model.add_loss_fn(nn.MultiLabelSoftMarginLoss())

optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
model.add_optimizer(optimizer)

use_device = torch.device('cuda' if CUDA else 'cpu')

model.add_device(use_device)

# Load model's trained parameters
model.load_state_dict(torch.load('model_params.pt'))

# Set model to evaluation mode
model.eval()

# Load normalization parameters
with open('norm_params', 'rb') as file:
    norm_params = pickle.load(file)

x_min = []
x_max = []

for name in norm_params.keys():
    x_min.append(norm_params[name]["MIN"])
    x_max.append(norm_params[name]["MAX"])

# Shape of x_min and x_max tensors: n_features
x_min = torch.Tensor(x_min)
x_max = torch.Tensor(x_max)

print("✅✅✅✅✅✅✅✅✅✅ line 113")


def callback(message):
    message.ack()
    timestamp_dict = json.loads(message.data.decode('utf-8'))
    timestamp_str = timestamp_dict['Timestamp']

    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    timestamp = timestamp.replace(tzinfo=None)
    timestamp = time_zone['EST'].localize(timestamp)

    current_timestamp = pytz.utc.localize(datetime.utcnow()).astimezone(time_zone['EST'])

    # Filter out old messages
    if timestamp > current_timestamp - (timedelta(minutes=4)):
        # continue
    # else:
        timestamp_str = datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

        # Give time to MySQL to load new data
        time.sleep(15)

        # Fetch current timestamp data ID if available
        cursor.execute("SELECT ID FROM {} WHERE Timestamp = '{}';".format(mysql_table_name, timestamp_str))
        current_id = cursor.fetchall()

        if not current_id:
            logging.warning("The current data ({}) is not yet available in MySQL.".format(timestamp_str))
            logging.warning("Next attempt to retrieve data in 15 seconds.")

            time.sleep(15)

            cursor.execute("SELECT ID FROM {} WHERE Timestamp = '{}';".format(mysql_table_name, timestamp_str))
            current_id = cursor.fetchall()

        if not current_id:
            logging.warning("The current data is not available.")
        else:
            current_id = current_id[0][0]

            # Generate window data indices
            indices = tuple(range(current_id - window + 1, current_id + 1 ))

            # Fetch data of entire window (starting from current timestamp)
            cursor.execute("SELECT {} {} WHERE sd.ID IN {};".format(x_fields, from_statement, indices))
            input_data = cursor.fetchall()

            # input_data shape: window, n_features
            input_data = torch.Tensor(input_data)

            # Reshape input to batch_size, window, n_features
            input_data = input_data.unsqueeze(0)

            # Perform input data normalization
            input_data = (input_data - x_min)/(x_max - x_min)

            # Forward propagate through network
            pred = model.forward(input_data)

            # Map logits to probabilities
            pred = torch.sigmoid(pred)

            pred = pred.squeeze(0)

            # Extract indices of predictions that have probability above a threshold
            pred_idx = np.where(pred > prob_threshold)[0]

            pred_labels = [y_fields[i] for i in pred_idx]

            print("Timestamp: {}, Probabilities: {}. Indices of labels above {} threshold: {}. Predicted labels: {}"\
                .format(timestamp_str, pred.numpy(), prob_threshold, pred_idx, pred_labels))

            pred_dict = {"timestamp": timestamp_str, "probabilities": pred, "prob_threshold": prob_threshold,
                "pred_indices": pred_idx, "pred_labels": pred_labels}

            # Send predictions to Kafka topic ('predict')
            publisher.publish(pubsub_topics['prediction'],
                      data=json.dumps({
                          "timestamp": timestamp_str,
                          "probabilities": pred.tolist(),
                          "prob_threshold": prob_threshold,
                          "pred_indices": pred_idx.tolist(),
                          "pred_labels": pred_labels
                      }).encode())
            
            streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

            print("Listening for messages on {}".format(subscription_path))

            try:
                streaming_pull.result()
            except KeyboardInterrupt:
                streaming_pull.cancel()

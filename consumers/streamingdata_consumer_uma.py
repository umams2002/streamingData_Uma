"""
streamingdata_consumer_uma.py

Consume json messages from a Kafka topic and visualize author counts in real-time.
Example Kafka message format:
{'timestamp': '2025-02-23T02:28:34.746539', 
'Food': 'Apple, commercial, 2 crust (23cm diam)', 
'Calories': '296', 
'Protein': '2', 
'Fat': '14', 
'Carbs': '43',
'Fibre': '2.0'}
"""


#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    return temp_variation


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Set up data structures (empty lists)
#####################################

foods = []  # To store food for the x-axis
proteins = []  # To store proteins for the y-axis

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()




#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart(rolling_window, window_size):
    """
    Update the live chart with new data.
    """
    # Clear the previous chart
    ax.clear()  

    # Create a line chart using the plot() method
    # Use the timestamps for the x-axis and temperatures for the y-axis
    # Use the label parameter to add a legend entry
    # Use the color parameter to set the line color
    ax.plot(foods, proteins, label="Temperature", color="blue")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Food")
    ax.set_ylabel("Proteins")
    ax.set_title("Food Smoker: Food vs. Proteins Uma Subramanian")

    

    # Regardless of whether a stall is detected, we want to show the legend

    # Use the legend() method to display the legend
    ax.legend()

    # Use the autofmt_xdate() method to automatically format the x-axis labels as dates
    fig.autofmt_xdate()

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)  


#####################################
# Function to process a single message
# #####################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a single JSON message from Kafka.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        food_list = data.get("Food")
        food = food_list.split(",")[0]
        protein = data.get("Protein")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if food is None or protein is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the message to the rolling window
        rolling_window.append(food)

        # Append the message to the rolling window
        foods.append(food)
        proteins.append(protein)

        # Update chart after processing this message
        update_chart(rolling_window=rolling_window, window_size=window_size)

       

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # Clear previous run's data
    foods.clear()
    proteins.clear()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")
    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
    plt.ioff()  # Turn off interactive mode after completion
    plt.show()
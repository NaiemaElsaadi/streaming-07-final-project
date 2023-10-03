"""
    This program sends a message to a queue on the RabbitMQ server.
    Messages come from a csv file of Housing since the messages are sent every 5 seconds.
    This program is designed to handle housing data using RabbitMQ as the message broker. 
    It consists of both producer and consumer components for processing and distributing
    housing-related information to different queues based on specific criteria.
    

    
    Author: Naiema Elsaadi
    Date: September 30, 2023

"""


import pika
import webbrowser
import csv
import time

def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

# RabbitMQ server settings
HOST = 'localhost'  # Change to your RabbitMQ server address
QUEUE_NAME_1 = 'housing_data'  # Name of the first queue for data_houseing with alert Excessively priced property 
QUEUE_NAME_2 = 'housing_queue1'  # Name of the second queue for houses with high prices
QUEUE_NAME_3 = 'housing_queue2'  # Name of the third queue for houses with low prices
QUEUE_NAME_4 = 'housing_queue3'  # Name of the forth queue for houses with 5 bedrooms


# Function to send a message to a RabbitMQ queue
def send_message(channel, queue_name, message):
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )
    print(f"Sent to {queue_name}: {message}")


# Function to process the housing data CSV file

def process_housing_data(channel):
    dataset_path = 'Housing.csv'

    with open(dataset_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)  # Skip the header row
        for row in csv_reader:
            price = float(row[0])
            area = float(row[1])
            bedrooms = int(row[2])

            # Check for excessively priced properties
            if price >= 300000:
                alert_message = f"ALERT: Excessively priced property - Price: ${price}, Area: {area} sqft"
                send_message(channel, QUEUE_NAME_1, alert_message)

            message = f"Price: ${price}, Area: {area} sqft"
            send_message(channel, QUEUE_NAME_1, message)


        # Send houses with price >= $3,000,000 to Queue 2
            if price >= 3000000:
                message = f"Price: ${price}, Bedrooms: {bedrooms}"
                send_message(channel, QUEUE_NAME_2, message)

            # Send houses with price < $3,000,000 to Queue 3
            if price < 3000000:
                message = f"Price: ${price}, Bedrooms: {bedrooms}"
                send_message(channel, QUEUE_NAME_3, message)

            # Send houses with 5 bedrooms to Queue 4
            if bedrooms == 5:
                message = f"Price: ${price}, Bedrooms: {bedrooms}"
                send_message(channel, QUEUE_NAME_4, message)

                time.sleep(5) ## sleep for 5 minutes

if __name__ == "__main__":
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = True
    offer_rabbitmq_admin_site(show_offer)

    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()

    # Declare the queue as durable
    channel.queue_declare(queue=QUEUE_NAME_1, durable=True)
    # Declare the queues as durable
    channel.queue_declare(queue= QUEUE_NAME_2, durable=True)
    channel.queue_declare(queue= QUEUE_NAME_3, durable=True)
    channel.queue_declare(queue= QUEUE_NAME_4, durable=True)
    

    process_housing_data(channel)  # Process the data once

    # Close the connection after processing
    connection.close()

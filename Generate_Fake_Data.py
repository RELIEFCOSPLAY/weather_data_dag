import csv
import random
from faker import Faker

fake = Faker()

num_records = 1000

header = ['Order_id', 'Customer_id', 'Customer_Name', 'Invoice_Amount']

with open('Order.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    
    for order_id in range(1, num_records + 1):
        customer_id = fake.random_int(min=1000, max=9999)
        customer_name = fake.name()
        invoice_amount = round(random.uniform(1000, 10000), 2) 
        writer.writerow([order_id, customer_id, customer_name, invoice_amount])

print("Order.csv generated successfully.")

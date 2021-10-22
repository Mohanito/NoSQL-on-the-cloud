'''
This python script uploads the CSV data (Master CSV and other CSVs) to one NoSQL DynamoDB.
- Create DynamoDB table.
- Read the master CSV for Experiments
- Read the data for each experiment based on the location stored in the master CSV
- Upload the BLOB data to your NoSQL DB and Fill your NoSQL DB with experiment data
'''

import boto3
import csv


def main():
    s3 = boto3.resource('s3',
                        aws_access_key_id='replaceMe',
                        aws_secret_access_key='replaceMe'
                        )
    try:
        s3.create_bucket(Bucket='bucket14848', CreateBucketConfiguration={
            'LocationConstraint': 'us-west-2'})
    except Exception as e:
        print(e)

    # make bucket publicly readable
    bucket = s3.Bucket("bucket14848")
    bucket.Acl().put(ACL='public-read')

    # upload a new object into the bucket
    body = open('./experiments.csv', 'rb')
    o = s3.Object('bucket14848', 'experiments').put(Body=body)
    s3.Object('bucket14848', 'experiments').Acl().put(ACL='public-read')

    '''
        Next we will create the DynamoDB table. Note that creating the resource
        does not create the table. The following try-block creates the table. We
        need to provide a Key schema. One element is hashed to produce a
        partition that stores a row while the second key is RowKey. The pair
        (PartitionKey, RowKey) is a unique identifier for the row in the table.
    '''
    dyndb = boto3.resource('dynamodb',
                           region_name='us-west-2',
                           aws_access_key_id='replaceMe',
                           aws_secret_access_key='replaceMe'
                           )
    try:
        table = dyndb.create_table(
            TableName='DataTable',
            KeySchema=[
                {
                    'AttributeName': 'PartitionKey',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'RowKey',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'PartitionKey',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'RowKey',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        print(e)
        # if there is an exception, the table may already exist. if so...
        table = dyndb.Table("DataTable")

    # wait for the table to be created
    table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
    print(table.item_count)

    # Reading the csv file, uploading the blobs and creating the table
    with open('./experiments.csv', 'r') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(csvf)
        for item in csvf:
            print(item)
            # ['3', '-2.93', '57.1', '3.7', 'exp3.csv']
            body = open('./' + item[4], 'rb')
            s3.Object('bucket14848', item[4]).put(Body=body)
            md = s3.Object('bucket14848', item[4]).Acl().put(ACL='public-read')

            url = " https://s3-us-west-2.amazonaws.com/bucket14848/"+item[4]
            metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                             'Conductivity': item[2], 'Concentration': item[3], 'url': url}
            try:
                table.put_item(Item=metadata_item)
            except:
                print("item may already be there or another failure")

    response = table.get_item(
        Key={
            'PartitionKey': '1',
            'RowKey': '-1'
        }
    )
    item = response['Item']
    print(item)
    # print(response)

    response = table.get_item(
        Key={
            'PartitionKey': '2',
            'RowKey': '-2'
        }
    )
    item = response['Item']
    print(item)
    # print(response)

    response = table.get_item(
        Key={
            'PartitionKey': '3',
            'RowKey': '-2.93'
        }
    )
    item = response['Item']
    print(item)
    # print(response)
    return


if __name__ == "__main__":
    main()

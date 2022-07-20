import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

logging.basicConfig(
level=logging.INFO,
format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()

def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True
    
def list_sns_topics(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)

def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []),subscriptions.get('NextToken', None)
    
def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
    'TopicArn': topic_arn,
    'Protocol': 'sms',
    'Endpoint': mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    return True
def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Message': message,
    }
    res = sns.publish(**params)
    print(res)
    return True
def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {
        'SubscriptionArn': subscription_arn,
    }
    res = sns.unsubscribe(**params)
    print(res)
    return True
    
def delete_sns_topic(topic_arn):
    # This will delete the topic and all it's subscriptions.
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True

if __name__ =='__main__':
    
    """sns_list_topics = list_sns_topics()
    log.info(f'sns topics : {sns_list_topics}')
    
    create_topic=create_sns_topic('price_updates_soyoppa')
    log.info(create_topic)
    
    log.info(list_sns_subscriptions())
    
    
    sns_topic_subs=subscribe_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_soyoppa','+639176885267')
    log.info(f"{sns_topic_subs}")
    """
    send=send_sns_message('arn:aws:sns:ap-southeast-1:337008671328:price_updates_soyoppa', 'Woo Hoodies are no 50% off!')
    log.info(send)

        
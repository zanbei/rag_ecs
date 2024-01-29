
import sys
sys.path.append(r"/home/ec2-user/guidance-for-custom-search-of-an-enterprise-knowledge-base-on-aws/lambda/langchain_processor_layer/python/")
from lambda_function import *
import time

event2={'requestContext': {'routeKey': 'search', 'messageId': 'QFfd0dRnIAMCFow=', 'eventType': 'MESSAGE', 'extendedRequestId': 'QFfd0FRoIAMFfjg=', 'requestTime': '17/Dec/2023:11:46:19 +0000', 'messageDirection': 'IN', 'stage': 'prod', 'connectedAt': 1702813569932, 'requestTimeEpoch': 1702813579371, 'identity': {'userAgent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'sourceIp': '183.220.42.61'}, 'requestId': 'QFfd0FRoIAMFfjg=',  'connectionId': 'RiGWXf6ePHcCE-Q=', 'apiId': 'p6paizcwkh'}, 'body': '{"action":"search","configs":{"name":"smart_search_qa_test","searchEngine":"opensearch","embedding_type":"ecs","llmData":{"type":"xxx","modelType":"ecs","modelName":"xxx","recordId":"pytorch-inference-llm-v1-68684"},"role":"","language":"chinese","taskDefinition":"","outputFormat":"","isCheckedGenerateReport":false,"isCheckedContext":false,"isCheckedKnowledgeBase":true,"indexName":"smart_search_qa_ecs","topK":3,"searchMethod":"vector","txtDocsNum":0,"vecDocsScoreThresholds":0,"txtDocsScoreThresholds":0,"isCheckedScoreQA":true,"isCheckedScoreQD":true,"isCheckedScoreAD":true,"prompt":"","tokenContentCheck":"","responseIfNoDocsFound":"Cannot find the answer","sessionId":"123"},"query":"什么猫最好看?"}', 'isBase64Encoded': False}
lambda_handler(event2,None)

'''
export api_gw=460e8z8jle
export dynamodb_table_name=LambdaStack-ChatSessionRecord189120C3-6XJ2IVJ5EALJ
export host=search-smartsearch-cfjxhevyorwyyyart6xh2zjgr4.us-west-2.es.amazonaws.com
export index=smart_search_qa_ecs
export language=chinese
export llm_endpoint_name=pytorch-inference-llm-v1
export search_engine_kendra=False
export search_engine_opensearch=True
export search_engine_zilliz=False
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=us-west-2
export PYTHONPATH=/Users/anbei/Desktop/rag_stream/guidance-for-custom-search-of-an-enterprise-knowledge-base-on-aws/lambda/langchain_processor_layer/python/
'''
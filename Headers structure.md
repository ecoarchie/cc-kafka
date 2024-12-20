Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER 
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING

# ONLY FOR APIVERSIONSREQUEST 
Response Header v0 => correlation_id 
  correlation_id => INT32

# For the rest apis
Response Header v1 => correlation_id TAG_BUFFER 
  correlation_id => INT32
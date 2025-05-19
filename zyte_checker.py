import requests

def get_article_list(source: str) -> str:
    """
    Returns URL_CODE:
      - 'ZYTE' = The API call succeeds and 'articleList' and 'articles' keys exist in the response and articles is non-empty
      - 'ZYTE_NO_ARTICLES' = articles is empty

    Retries automatically if the API returns a 52x status code (e.g., 521), up to MAX_RETRIES times.
    Returns URL_CODE error condition if:
      - Retries are exhausted
      - Any other error/exception occurs
      - 'articleList' / 'articles' keys are missing (not checked in the code below)
    """
    MAX_RETRIES = 3
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            api_response = requests.post(
                "https://api.zyte.com/v1/extract",
                auth=("40eb739606e748089cd0a447e1c3f88d", ""),
                json={
                    "url": source,
                    "httpResponseBody": True,
                    "articleList": True,
                    "articleListOptions": {"extractFrom": "httpResponseBody"},
                },
            )

            status_code = api_response.status_code
            URL_code = str(status_code)

            # Check for 52x status codes that warrant a retry
            if status_code in [520, 521, 522, 523, 524]:
                attempt += 1
                if attempt < MAX_RETRIES:
                    # print(f"Received {status_code}. Retrying '{source}' (attempt {attempt}/{MAX_RETRIES})")
                    continue
                else:
                    # print(f"Exhausted retries for '{source}' with status code {status_code}")
                    return URL_code

            # We have a non-52x status or a 52x that was successfully retried
            json_data = api_response.json()
            article_list = json_data["articleList"]
            articles = article_list["articles"]

            # Check if articles is empty
            if len(articles) > 0:
                URL_code = 'ZYTE'
                return URL_code
            else:
                URL_code = 'ZYTE_NO_ARTICLES'
                return URL_code
        except Exception as e:
            URL_code = 'ERROR'
            return URL_code
    return URL_code
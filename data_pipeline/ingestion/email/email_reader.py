def fetch_messages(service, query="", max_results=10):
    response = service.users().messages().list(
        userId="me",
        q=query,
        maxResults=max_results
    ).execute()

    return response.get("messages", [])

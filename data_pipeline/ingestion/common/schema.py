from typing import Dict, Any

def raw_transaction(
    source: str,
    sender: str,
    timestamp: str,
    raw_text: str,
    metadata: Dict[str, Any] = None
):
    return {
        "source": source,         
        "sender": sender,          
        "timestamp": timestamp,    
        "raw_text": raw_text,      
        "currency": "INR",
        "metadata": metadata or {}
    }

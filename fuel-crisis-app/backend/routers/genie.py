"""Genie router — proxy to Databricks Genie Conversation API with follow-up support."""
import os
import time
import logging
import requests as req
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from backend.database import get_databricks_db

logger = logging.getLogger("uvicorn.error")
router = APIRouter()


class GenieQuestion(BaseModel):
    question: str
    conversation_id: Optional[str] = None


@router.post("/ask")
def ask_genie(body: GenieQuestion):
    space_id = os.getenv("GENIE_SPACE_ID", "")
    logger.info(f"Genie ask: '{body.question}' conv={body.conversation_id}")
    if not space_id:
        raise HTTPException(503, "GENIE_SPACE_ID not configured")

    db = get_databricks_db()
    sdk = db.sdk
    host = sdk.config.host.rstrip("/")
    headers = sdk.config.authenticate()
    headers["Content-Type"] = "application/json"

    conversation_id = body.conversation_id or ""
    message_id = ""

    # Follow-up in existing conversation
    if conversation_id:
        try:
            resp = req.post(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                json={"content": body.question}, headers=headers, timeout=30)
            resp.raise_for_status()
            msg_obj = resp.json()
            message_id = msg_obj.get("id", "")
            logger.info(f"Genie follow-up: conv={conversation_id} msg={message_id}")
        except Exception as e:
            logger.warning(f"Follow-up failed ({e}), starting new conversation")
            conversation_id = ""

    # New conversation
    if not conversation_id:
        try:
            resp = req.post(f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
                            json={"content": body.question}, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            conv_obj = data.get("conversation", {})
            msg_obj = data.get("message", {})
            conversation_id = msg_obj.get("conversation_id") or conv_obj.get("id", "")
            message_id = msg_obj.get("id", "")
            logger.info(f"Genie new: conv={conversation_id} msg={message_id}")
        except Exception as e:
            raise HTTPException(502, f"Genie failed: {e}")

    if not conversation_id or not message_id:
        return {"answer": None, "sql": None, "error": "No conversation created"}

    # Poll for result
    for i in range(30):
        time.sleep(2)
        try:
            poll_headers = sdk.config.authenticate()
            poll_resp = req.get(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                headers=poll_headers, timeout=15)
            msg = poll_resp.json()
            status = msg.get("status", "")
            if i % 5 == 0:
                logger.info(f"Genie poll {i}: status={status}")
        except Exception:
            continue

        if status in ("COMPLETED", "COMPLETE"):
            sql_text, answer_text = None, None
            for att in msg.get("attachments", []):
                if att.get("query", {}).get("query"):
                    sql_text = att["query"]["query"]
                if att.get("text", {}).get("content"):
                    answer_text = att["text"]["content"]
            if not answer_text:
                answer_text = msg.get("content", "")
            return {"answer": answer_text, "sql": sql_text, "status": status,
                    "conversation_id": conversation_id}

        if status in ("FAILED", "CANCELLED"):
            error = msg.get("error", {}).get("message", "Failed")
            return {"answer": None, "sql": None, "error": error, "status": status,
                    "conversation_id": conversation_id}

    return {"answer": None, "sql": None, "error": "Timeout", "status": "TIMEOUT",
            "conversation_id": conversation_id}

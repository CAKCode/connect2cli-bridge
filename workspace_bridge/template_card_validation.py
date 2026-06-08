from __future__ import annotations

import re


INTERACTIVE_CARD_TYPES = {"button_interaction", "vote_interaction", "multiple_interaction"}
SUBTITLE_OWNER_SAFE_CARD_TYPES = {"text_notice", "news_notice", "button_interaction"}
TASK_ID_ALLOWED_RE = re.compile(r"^[A-Za-z0-9_@-]+$")


def require_non_empty_string(value, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} required")
    return text


def validate_feedback_id(value, field: str = "feedback.id") -> str:
    text = require_non_empty_string(value, field)
    if len(text.encode("utf-8")) > 256:
        raise ValueError(f"{field} exceeds 256 bytes")
    return text


def validate_task_id(value, field: str) -> str:
    text = require_non_empty_string(value, field)
    if len(text.encode("utf-8")) > 128:
        raise ValueError(f"{field} exceeds 128 bytes")
    if not TASK_ID_ALLOWED_RE.fullmatch(text):
        raise ValueError(f"{field} contains unsupported characters")
    return text


def validate_button_list(value, field: str = "button_interaction.button_list") -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} required")
    if len(value) > 6:
        raise ValueError(f"{field} exceeds 6 items")
    normalized = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{field}[{index}] must be an object")
        require_non_empty_string(item.get("text"), f"{field}[{index}].text")
        require_non_empty_string(item.get("key"), f"{field}[{index}].key")
        normalized.append(dict(item))
    return normalized


def validate_submit_button(value, field: str) -> dict:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{field} required")
    require_non_empty_string(value.get("text"), f"{field}.text")
    require_non_empty_string(value.get("key"), f"{field}.key")
    return dict(value)


def validate_checkbox(value) -> dict:
    if not isinstance(value, dict) or not value:
        raise ValueError("vote_interaction.checkbox required")
    require_non_empty_string(value.get("question_key"), "vote_interaction.checkbox.question_key")
    option_list = value.get("option_list")
    if not isinstance(option_list, list) or not option_list:
        raise ValueError("vote_interaction.checkbox.option_list required")
    if len(option_list) > 20:
        raise ValueError("vote_interaction.checkbox.option_list exceeds 20 items")
    for index, item in enumerate(option_list, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"vote_interaction.checkbox.option_list[{index}] must be an object")
        require_non_empty_string(item.get("id"), f"vote_interaction.checkbox.option_list[{index}].id")
        require_non_empty_string(item.get("text"), f"vote_interaction.checkbox.option_list[{index}].text")
    return dict(value)


def validate_select_list(value) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValueError("multiple_interaction.select_list required")
    if len(value) > 3:
        raise ValueError("multiple_interaction.select_list exceeds 3 items")
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"multiple_interaction.select_list[{index}] must be an object")
        require_non_empty_string(item.get("question_key"), f"multiple_interaction.select_list[{index}].question_key")
        option_list = item.get("option_list")
        if not isinstance(option_list, list) or not option_list:
            raise ValueError(f"multiple_interaction.select_list[{index}].option_list required")
        for option_index, option in enumerate(option_list, start=1):
            if not isinstance(option, dict):
                raise ValueError(
                    f"multiple_interaction.select_list[{index}].option_list[{option_index}] must be an object"
                )
            require_non_empty_string(
                option.get("id"),
                f"multiple_interaction.select_list[{index}].option_list[{option_index}].id",
            )
            require_non_empty_string(
                option.get("text"),
                f"multiple_interaction.select_list[{index}].option_list[{option_index}].text",
            )
    return list(value)


def validate_template_card_payload(template_card: dict, *, require_interaction_task_id: bool = True) -> dict:
    if not isinstance(template_card, dict):
        raise ValueError("templateCard must be a JSON object")
    card = dict(template_card)
    card_type = require_non_empty_string(card.get("card_type"), "templateCard.card_type")

    if card_type == "text_notice":
        has_title = bool(str(((card.get("main_title") or {}).get("title")) or "").strip())
        has_sub_title = bool(str(card.get("sub_title_text") or "").strip())
        if not has_title and not has_sub_title:
            raise ValueError("text_notice requires main_title.title or sub_title_text")
        if not isinstance(card.get("card_action"), dict) or not card.get("card_action"):
            raise ValueError("text_notice requires card_action")
        if card.get("action_menu"):
            validate_task_id(card.get("task_id"), "text_notice.task_id")

    elif card_type == "news_notice":
        if not isinstance(card.get("main_title"), dict) or not str((card.get("main_title") or {}).get("title") or "").strip():
            raise ValueError("news_notice requires main_title.title")
        if not card.get("card_image") and not card.get("image_text_area"):
            raise ValueError("news_notice requires card_image or image_text_area")
        if not isinstance(card.get("card_action"), dict) or not card.get("card_action"):
            raise ValueError("news_notice requires card_action")
        if card.get("action_menu"):
            validate_task_id(card.get("task_id"), "news_notice.task_id")

    elif card_type == "button_interaction":
        if not isinstance(card.get("main_title"), dict) or not str((card.get("main_title") or {}).get("title") or "").strip():
            raise ValueError("button_interaction requires main_title.title")
        card["button_list"] = validate_button_list(card.get("button_list"))
        if require_interaction_task_id:
            validate_task_id(card.get("task_id"), "button_interaction.task_id")

    elif card_type == "vote_interaction":
        if not isinstance(card.get("main_title"), dict) or not str((card.get("main_title") or {}).get("title") or "").strip():
            raise ValueError("vote_interaction requires main_title.title")
        card["checkbox"] = validate_checkbox(card.get("checkbox"))
        card["submit_button"] = validate_submit_button(card.get("submit_button"), "vote_interaction.submit_button")
        if require_interaction_task_id:
            validate_task_id(card.get("task_id"), "vote_interaction.task_id")

    elif card_type == "multiple_interaction":
        if not isinstance(card.get("main_title"), dict) or not str((card.get("main_title") or {}).get("title") or "").strip():
            raise ValueError("multiple_interaction requires main_title.title")
        card["select_list"] = validate_select_list(card.get("select_list"))
        card["submit_button"] = validate_submit_button(card.get("submit_button"), "multiple_interaction.submit_button")
        if require_interaction_task_id and card.get("task_id"):
            validate_task_id(card.get("task_id"), "multiple_interaction.task_id")

    return card


def validate_template_card_update_payload(template_card: dict) -> dict:
    return validate_template_card_payload(template_card, require_interaction_task_id=False)


def normalize_template_card_task_id_seed(value: str, fallback: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_@-]+", "-", str(value or "").strip()).strip("-")
    return text[:48] or fallback


def build_group_owner_template_card_task_id(base_task_id: str, owner_user_id: str, unique_suffix: str) -> str:
    normalized_base = normalize_template_card_task_id_seed(base_task_id, "codex-card")
    normalized_owner = normalize_template_card_task_id_seed(owner_user_id, "owner")
    normalized_unique = normalize_template_card_task_id_seed(unique_suffix, "uniq")[:12]
    return f"{normalized_base}--owner--{normalized_owner}--uniq--{normalized_unique}"[:128]


def extract_template_card_owner_user_id(task_id: str) -> str | None:
    text = str(task_id or "").strip()
    if "--owner--" in text:
        owner_fragment = text.rsplit("--owner--", 1)[1].strip()
        return owner_fragment.split("--uniq--", 1)[0].strip() or None
    if "__owner__" in text:
        owner_fragment = text.rsplit("__owner__", 1)[1].strip()
        return owner_fragment.split("__uniq__", 1)[0].strip() or None
    return None


def strip_template_card_owner_suffix(task_id: str) -> str:
    text = str(task_id or "").strip()
    if "--owner--" in text:
        return text.split("--owner--", 1)[0].strip()
    if "__owner__" in text:
        return text.split("__owner__", 1)[0].strip()
    return text


def enrich_template_card_for_delivery(chat_key: str, template_card: dict, *, unique_token: str) -> dict:
    card = dict(template_card or {})
    card_type = str(card.get("card_type") or "").strip()
    task_id = str(card.get("task_id") or "").strip()
    if card_type in INTERACTIVE_CARD_TYPES and not task_id:
        task_id = f"codex-card-{normalize_template_card_task_id_seed(unique_token, 'uniq')}"
    if str(chat_key or "").startswith("group-user:"):
        parts = str(chat_key).split(":", 2)
        owner_user_id = parts[2] if len(parts) == 3 else ""
        if owner_user_id:
            if card_type in SUBTITLE_OWNER_SAFE_CARD_TYPES:
                owner_hint = f"此卡片归属：{owner_user_id}"
                existing_sub_title = str(card.get("sub_title_text") or "").strip()
                card["sub_title_text"] = f"{owner_hint}\n{existing_sub_title}" if existing_sub_title else owner_hint
            if task_id:
                task_id = build_group_owner_template_card_task_id(
                    strip_template_card_owner_suffix(task_id),
                    owner_user_id,
                    unique_token,
                )
    if task_id:
        card["task_id"] = task_id
    return card

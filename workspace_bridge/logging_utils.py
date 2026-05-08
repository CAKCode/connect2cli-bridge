from __future__ import annotations

import logging


def get_logger(name: str = "workspace_bridge") -> logging.Logger:
    return logging.getLogger(name)

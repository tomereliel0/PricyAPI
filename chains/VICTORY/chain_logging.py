#!/usr/bin/env python3
"""Shared logging setup for VICTORY scripts."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

CHAIN_NAME = "victory"


def _build_file_handler(path: Path, level: int) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(level)
    return handler


def configure_chain_logger(script_name: str, debug: bool = False) -> logging.Logger:
    """Create or reuse logger with chain-wide and script-specific file handlers."""
    level = logging.DEBUG if debug else logging.INFO
    logger_name = f"{CHAIN_NAME}.{script_name}"
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        logger.setLevel(level)
        for handler in logger.handlers:
            handler.setLevel(level)
        return logger

    script_dir = Path(__file__).resolve().parent
    log_dir = script_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    chain_file = log_dir / f"{CHAIN_NAME}.log"
    script_file = log_dir / f"{script_name}.log"

    chain_handler = _build_file_handler(chain_file, level)
    chain_handler.setFormatter(formatter)

    script_handler = _build_file_handler(script_file, level)
    script_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(formatter)

    logger.addHandler(chain_handler)
    logger.addHandler(script_handler)
    logger.addHandler(stderr_handler)

    return logger


def get_log_paths(script_name: str) -> tuple[Path, Path]:
    script_dir = Path(__file__).resolve().parent
    log_dir = script_dir / "logs"
    return log_dir / f"{CHAIN_NAME}.log", log_dir / f"{script_name}.log"

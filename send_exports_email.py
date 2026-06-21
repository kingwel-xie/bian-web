#!/usr/bin/env python3
"""Send generated leaderboard export files by SMTP."""

from __future__ import annotations

import argparse
import mimetypes
import os
import smtplib
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path


DEFAULT_EXPORT_DIR = Path(os.environ.get("DATA_ROOT", "/data")) / "exports"


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Email leaderboard XLSX exports.")
    parser.add_argument("--env-file", help="Load SMTP settings from a KEY=VALUE env file.")
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR), help="Directory containing .xlsx exports.")
    parser.add_argument("--file", action="append", default=[], help="Attach a specific file. Can be repeated.")
    parser.add_argument("--subject", default=env("EMAIL_SUBJECT", "Binance leaderboard exports"), help="Email subject.")
    parser.add_argument("--body", default=env("EMAIL_BODY", "Leaderboard export files are attached."), help="Email body.")
    parser.add_argument("--to", default=env("SMTP_TO"), help="Comma-separated recipient list.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be sent without connecting to SMTP.")
    return parser.parse_args()


def load_env_file(path_value: str | None) -> None:
    if not path_value:
        return
    path = Path(path_value).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Env file not found: {path}")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def required_env(name: str) -> str:
    value = env(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def recipient_list(value: str | None) -> list[str]:
    recipients = [item.strip() for item in (value or "").replace(";", ",").split(",") if item.strip()]
    if not recipients:
        raise SystemExit("No recipients configured. Set SMTP_TO or pass --to.")
    return recipients


def attachment_paths(args: argparse.Namespace) -> list[Path]:
    if args.file:
        paths = [Path(item).expanduser().resolve() for item in args.file]
    else:
        export_dir = Path(args.export_dir).expanduser().resolve()
        paths = sorted(export_dir.glob("*.xlsx"))
    missing = [str(path) for path in paths if not path.exists() or not path.is_file()]
    if missing:
        raise SystemExit("Attachment not found: " + ", ".join(missing))
    if not paths:
        raise SystemExit("No .xlsx attachments found.")
    return paths


def build_message(args: argparse.Namespace, attachments: list[Path]) -> EmailMessage:
    sender = env("SMTP_FROM") or env("SMTP_USER")
    if not sender:
        raise SystemExit("Missing sender. Set SMTP_FROM or SMTP_USER.")
    recipients = recipient_list(args.to)

    message = EmailMessage()
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message["Date"] = formatdate(localtime=True)
    message["Message-ID"] = make_msgid()
    message["Subject"] = args.subject
    message.set_content(args.body)

    for path in attachments:
        ctype, _ = mimetypes.guess_type(path.name)
        if not ctype:
            ctype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        maintype, subtype = ctype.split("/", 1)
        message.add_attachment(path.read_bytes(), maintype=maintype, subtype=subtype, filename=path.name)
    return message


def send_message(message: EmailMessage) -> None:
    host = required_env("SMTP_HOST")
    port = int(env("SMTP_PORT", "465") or "465")
    user = env("SMTP_USER")
    password = env("SMTP_PASSWORD")
    mode = (env("SMTP_TLS", "ssl") or "ssl").lower()

    if mode == "ssl":
        server: smtplib.SMTP = smtplib.SMTP_SSL(host, port, timeout=30)
    else:
        server = smtplib.SMTP(host, port, timeout=30)
    try:
        server.ehlo()
        if mode in {"starttls", "tls"}:
            server.starttls()
            server.ehlo()
        if user and password:
            server.login(user, password)
        server.send_message(message)
    finally:
        server.quit()


def main() -> int:
    args = parse_args()
    load_env_file(args.env_file)
    if not args.to:
        args.to = env("SMTP_TO")
    attachments = attachment_paths(args)
    message = build_message(args, attachments)
    if args.dry_run:
        print(f"from={message['From']}")
        print(f"to={message['To']}")
        print(f"subject={message['Subject']}")
        for path in attachments:
            print(f"attach={path} size={path.stat().st_size}")
        return 0
    send_message(message)
    print(f"sent {len(attachments)} attachment(s) to {message['To']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

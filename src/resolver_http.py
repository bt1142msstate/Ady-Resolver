#!/usr/bin/env python3
"""HTTP request handling for the local resolver app."""
from __future__ import annotations

import json
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from typing import Dict, Optional
from urllib.parse import unquote, urlparse

from resolver_app_ui import HTML, STATIC_DIR
from resolver_batch_io import inspect_batch_columns
from resolver_service import ResolverService

class ResolverRequestHandler(BaseHTTPRequestHandler):
    service: ResolverService

    def log_message(self, format: str, *args) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        route = urlparse(self.path).path
        if route == "/":
            self.send_html(HTML)
            return
        if route.startswith("/static/"):
            self.send_static_asset(route)
            return
        if route == "/api/health":
            self.send_json(self.service.health())
            return
        if route == "/api/training":
            self.send_json(self.service.training_status())
            return
        self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        route = urlparse(self.path).path
        if route == "/api/add-address":
            self.add_verified_address()
            return
        if route == "/api/add-addresses":
            self.import_verified_addresses()
            return
        if route == "/api/feedback":
            self.record_feedback()
            return
        if route == "/api/training/start":
            self.start_training()
            return
        if route == "/api/batch-columns":
            self.batch_columns()
            return
        if route == "/api/batch-resolve":
            self.batch_resolve()
            return
        if route != "/api/resolve":
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            if not raw_address:
                self.send_json({"error": "Address is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(self.service.resolve(raw_address))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def parse_multipart_form(self) -> Dict[str, object]:
        content_type = self.headers.get("Content-Type", "")
        marker = "boundary="
        if marker not in content_type:
            raise ValueError("Expected multipart form data.")
        boundary = content_type.split(marker, 1)[1].split(";", 1)[0].strip().strip('"')
        if not boundary:
            raise ValueError("Multipart boundary is missing.")
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            raise ValueError("Request body is empty.")
        if length > 32 * 1024 * 1024:
            raise ValueError("Upload is too large. Use a file under 32 MB.")

        body = self.rfile.read(length)
        delimiter = b"--" + boundary.encode("utf-8")
        form: Dict[str, object] = {}
        for raw_part in body.split(delimiter):
            part = raw_part
            if part.startswith(b"\r\n"):
                part = part[2:]
            if part.endswith(b"--"):
                part = part[:-2]
            if part.endswith(b"\r\n"):
                part = part[:-2]
            if not part or part == b"--":
                continue
            if b"\r\n\r\n" not in part:
                continue
            raw_headers, content = part.split(b"\r\n\r\n", 1)
            headers = raw_headers.decode("utf-8", errors="replace").split("\r\n")
            disposition = ""
            for header in headers:
                name, _, value = header.partition(":")
                if name.lower() == "content-disposition":
                    disposition = value.strip()
                    break
            if not disposition:
                continue
            fields: Dict[str, str] = {}
            for segment in disposition.split(";"):
                key, separator, value = segment.strip().partition("=")
                if separator:
                    fields[key] = value.strip().strip('"')
            name = fields.get("name", "")
            filename = fields.get("filename")
            if not name:
                continue
            if filename is not None:
                form[name] = {"filename": filename, "content": content}
            else:
                form[name] = content.decode("utf-8", errors="replace").strip()
        return form

    def start_training(self) -> None:
        try:
            self.read_json_body()
            self.send_json(self.service.start_training())
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def batch_columns(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(inspect_batch_columns(filename, content))
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def batch_resolve(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            address_column = str(form.get("address_column") or "")
            id_column = str(form.get("id_column") or "")
            has_header_value = str(form.get("has_header") or "").strip().lower()
            has_header = None
            if has_header_value in {"1", "true", "yes"}:
                has_header = True
            elif has_header_value in {"0", "false", "no"}:
                has_header = False
            output_filename, workbook, row_count = self.service.resolve_batch(
                filename,
                content,
                address_column,
                id_column,
                has_header,
            )
            self.send_bytes(
                workbook,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=output_filename,
                extra_headers={"X-Ady-Resolved-Rows": str(row_count)},
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def import_verified_addresses(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "verified_addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            address_column = str(form.get("address_column") or "")
            source_note = str(form.get("source_note") or "")
            has_header_value = str(form.get("has_header") or "").strip().lower()
            has_header = None
            if has_header_value in {"1", "true", "yes"}:
                has_header = True
            elif has_header_value in {"0", "false", "no"}:
                has_header = False
            self.send_json(
                self.service.import_verified_addresses(
                    filename,
                    content,
                    address_column,
                    source_note,
                    has_header,
                )
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def record_feedback(self) -> None:
        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            feedback_type = str(payload.get("feedback_type", "")).strip()
            correct_address = str(payload.get("correct_address", "")).strip()
            self.send_json(self.service.record_feedback(raw_address, feedback_type, correct_address))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def add_verified_address(self) -> None:
        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            source_note = str(payload.get("source_note", "")).strip()
            if not raw_address:
                self.send_json({"error": "Address is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(self.service.add_verified_address(raw_address, source_note))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def read_json_body(self) -> Dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def send_html(self, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_json(self, payload: Dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_static_asset(self, route: str) -> None:
        relative_path = unquote(route.removeprefix("/static/")).lstrip("/")
        if not relative_path or "\x00" in relative_path:
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return
        static_root = STATIC_DIR.resolve()
        asset_path = (STATIC_DIR / relative_path).resolve()
        try:
            asset_path.relative_to(static_root)
        except ValueError:
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return
        if not asset_path.is_file():
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return
        content_types = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".html": "text/html; charset=utf-8",
        }
        payload = asset_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_types.get(asset_path.suffix.lower(), "application/octet-stream"))
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(payload)

    def send_bytes(
        self,
        payload: bytes,
        content_type: str,
        filename: str,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)

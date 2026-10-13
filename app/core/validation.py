"""Input validation helpers shared by the API and node processes."""

from __future__ import annotations

import re
import unicodedata

NODE_ID_PATTERN = r"^[A-Za-z0-9_.-]{1,64}$"
FILE_ID_PATTERN = r"^[0-9a-f]{32}$"
_NODE_ID_RE = re.compile(NODE_ID_PATTERN)
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._ ()\-+@,=]")
MAX_FILENAME_LENGTH = 255


def is_valid_node_id(value: str) -> bool:
    return bool(_NODE_ID_RE.match(value or ""))


def sanitize_filename(name: str | None) -> str:
    """Return a display-safe filename.

    Filenames are metadata only and are never used to build filesystem paths;
    sanitisation removes directory components, control characters and
    characters that are awkward in Content-Disposition headers.
    """
    if not name:
        return "unnamed"
    name = unicodedata.normalize("NFKC", name)
    name = name.replace("\\", "/").split("/")[-1]
    name = "".join(ch for ch in name if unicodedata.category(ch)[0] != "C")
    name = _UNSAFE_CHARS.sub("_", name).strip(" .")
    if name in {"", ".", ".."}:
        return "unnamed"
    if len(name) > MAX_FILENAME_LENGTH:
        stem, dot, ext = name.rpartition(".")
        if dot and 0 < len(ext) <= 16:
            name = stem[: MAX_FILENAME_LENGTH - len(ext) - 1] + "." + ext
        else:
            name = name[:MAX_FILENAME_LENGTH]
    return name

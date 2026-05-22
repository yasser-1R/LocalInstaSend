"""
localInstaSend — LAN File & Message Transfer
=============================================
Devices join by name → pick a destination → send files or chat messages.
Files are relayed through the server and auto-downloaded on the target device.
Messages are broadcast to everyone in real time via SSE.

Run:
    python main.py
"""

import asyncio
import json
import socket
import time
import uuid
from pathlib import Path
from typing import Dict, List

import uvicorn
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

# ── Config ────────────────────────────────────────────────────────
PORT     = 8000
TEMP_DIR = Path("temp_transfers")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

app       = FastAPI(title="localInstaSend")
templates = Jinja2Templates(directory="templates")

# ── Device registry  { session_id: {name, ip, last_seen} } ───────
registry: Dict[str, dict] = {}
DEVICE_TIMEOUT = 35

# ── Pending transfers  { file_id: {name, path, sender, created_at} } ──
pending: Dict[str, dict] = {}
FILE_TTL = 7200   # auto-delete after 2 hours

# ── SSE queues ────────────────────────────────────────────────────
_all_qs:     List[asyncio.Queue]            = []
_session_qs: Dict[str, List[asyncio.Queue]] = {}


# ── Helpers ───────────────────────────────────────────────────────
def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0];  s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def active_devices() -> list:
    now   = time.time()
    stale = [s for s, d in registry.items()
             if now - d["last_seen"] > DEVICE_TIMEOUT]
    for s in stale:
        registry.pop(s, None)
        _session_qs.pop(s, None)
    return [{"id": s, "name": d["name"], "ip": d["ip"]}
            for s, d in registry.items()]


def safe_name(s: str) -> str:
    return (
        "".join(c if c.isalnum() or c in "._- " else "_"
                for c in Path(s).name)
        or "file"
    )


def human_size(b: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"


async def _push(qs: list, event: dict) -> None:
    msg  = json.dumps(event, ensure_ascii=False)
    dead = []
    for q in qs:
        try:
            q.put_nowait(msg)
        except Exception:
            dead.append(q)
    for q in dead:
        if q in qs:
            qs.remove(q)


async def broadcast(ev: dict)        -> None: await _push(_all_qs, ev)
async def notify(sid: str, ev: dict) -> None: await _push(_session_qs.get(sid, []), ev)
async def push_devices()             -> None: await broadcast({"type": "devices", "list": active_devices()})


# ── Background cleanup ────────────────────────────────────────────
@app.on_event("startup")
async def on_startup() -> None:
    asyncio.create_task(_cleanup_loop())


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(10)
        before = set(registry)
        active_devices()
        if set(registry) != before:
            await push_devices()
        now     = time.time()
        expired = [fid for fid, m in list(pending.items())
                   if now - m["created_at"] > FILE_TTL]
        for fid in expired:
            try:
                pending[fid]["path"].unlink(missing_ok=True)
            except Exception:
                pass
            pending.pop(fid, None)


# ── Routes ────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/register")
async def register(request: Request, name: str = Form(...)):
    name = name.strip()[:32]
    if not name:
        return JSONResponse({"error": "Name cannot be empty"}, status_code=400)
    sid = str(uuid.uuid4())
    registry[sid] = {"name": name, "ip": request.client.host,
                     "last_seen": time.time()}
    await push_devices()
    return JSONResponse({"session_id": sid, "name": name})


@app.post("/heartbeat")
async def heartbeat(session_id: str = Form(...)):
    if session_id in registry:
        registry[session_id]["last_seen"] = time.time()
    return JSONResponse({"ok": True})


@app.post("/leave")
async def leave(session_id: str = Form(...)):
    registry.pop(session_id, None)
    _session_qs.pop(session_id, None)
    await push_devices()
    return JSONResponse({"ok": True})


@app.get("/events")
async def sse(request: Request, sid: str = ""):
    q: asyncio.Queue = asyncio.Queue(maxsize=256)
    _all_qs.append(q)
    if sid:
        _session_qs.setdefault(sid, []).append(q)

    async def stream():
        try:
            yield f"data: {json.dumps({'type':'devices','list':active_devices()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=20)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            if q in _all_qs:
                _all_qs.remove(q)
            ql = _session_qs.get(sid, [])
            if q in ql:
                ql.remove(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/upload")
async def upload(
    session_id: str              = Form(...),
    target_id:  str              = Form(...),
    files:      List[UploadFile] = File(...),
):
    if session_id not in registry:
        return JSONResponse({"error": "Unknown sender — please rejoin"}, status_code=401)
    if target_id not in registry:
        return JSONResponse({"error": "Target device is no longer online"}, status_code=404)

    sender = registry[session_id]["name"]
    relayed, errors = [], []

    for f in files:
        try:
            data    = await f.read()
            file_id = str(uuid.uuid4())
            name    = safe_name(f.filename or "file")
            path    = TEMP_DIR / file_id
            path.write_bytes(data)
            pending[file_id] = {
                "name": name, "path": path,
                "sender": sender, "created_at": time.time(),
            }
            relayed.append({"id": file_id, "name": name,
                            "size": human_size(len(data))})
        except Exception as exc:
            errors.append({"file": f.filename or "?", "error": str(exc)})

    if relayed:
        await notify(target_id, {
            "type": "incoming", "from": sender,
            "files": relayed, "count": len(relayed),
        })

    return JSONResponse({
        "relayed": relayed, "errors": errors,
        "total_relayed": len(relayed),
        "target": registry[target_id]["name"],
    })


@app.get("/download/{file_id}")
async def download(file_id: str):
    meta = pending.get(file_id)
    if not meta or not meta["path"].exists():
        return JSONResponse({"error": "File not found or expired"}, status_code=404)
    return FileResponse(
        str(meta["path"]),
        filename=meta["name"],
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{meta["name"]}"'},
    )


@app.post("/message")
async def send_message(
    session_id: str = Form(...),
    text:       str = Form(...),
):
    if session_id not in registry:
        return JSONResponse({"error": "Unknown sender"}, status_code=401)
    text = text.strip()[:2000]
    if not text:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    await broadcast({
        "type":    "message",
        "from":    registry[session_id]["name"],
        "from_id": session_id,
        "text":    text,
        "ts":      int(time.time()),
    })
    return JSONResponse({"ok": True})


# ── Entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    ip = get_local_ip()
    print(f"\n{'='*54}")
    print("  ⚡  localInstaSend — ready!")
    print(f"{'='*54}")
    print(f"  Local   →  http://127.0.0.1:{PORT}")
    print(f"  Network →  http://{ip}:{PORT}   ← open on every device")
    print(f"  Relay   →  {TEMP_DIR.resolve()}")
    print(f"{'='*54}\n")
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)

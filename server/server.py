#!/usr/bin/env python3
"""
Turbo Photo Sync - Dual-Stack High-Throughput Streaming Server
Engineered by Nilkanth Desai

Provides high-speed sequential media streaming from macOS to iOS over:
- Apple CoreDevice USB Cable Tunnel (IPv6 MTU 16000)
- 5GHz Wi-Fi / Local Area Network (IPv4)

Features:
- Dual-stack socket binding (IPv4 + IPv6 simultaneously on one port)
- 2.0 GB LRU RAM Read-Ahead Prefetcher (zero disk seek overhead during sync)
- Automatic batch slicing (1,000 items per batch) for both folder batches and flat directories
- Thread-safe resume state tracking and album metadata mapping
- Zero external dependencies (Python 3 standard library only)
"""

import argparse
import http.server
import json
import mimetypes
import os
import re
import socket
import socketserver
import struct
import subprocess
import threading
import time
from collections import OrderedDict
from urllib.parse import unquote, urlparse

# CLI Configuration
PARSER = argparse.ArgumentParser(description="Turbo Photo Sync High-Performance Streaming Server")
PARSER.add_argument("--dir", "-d", default=os.environ.get("PHOTOS_DIR", "./Batches"), help="Directory containing photo batches or photos")
PARSER.add_argument("--port", "-p", type=int, default=int(os.environ.get("PORT", "8765")), help="Port to listen on (default: 8765)")
PARSER.add_argument("--cache-size", "-c", type=int, default=int(os.environ.get("CACHE_SIZE_MB", "2048")), help="RAM cache size limit in MB (default: 2048)")
PARSER.add_argument("--manifest", "-m", default=os.environ.get("MANIFEST_PATH", None), help="Optional path to album mapping restore_manifest.json")
PARSER.add_argument("--state", "-s", default=os.environ.get("SYNCED_STATE_PATH", "./synced_state.json"), help="State file tracking synced keys")

ARGS, _ = PARSER.parse_known_args()

BATCHES_ROOT = os.path.abspath(ARGS.dir)
PORT = ARGS.port
MAX_RAM_CACHE_BYTES = ARGS.cache_size * 1024 * 1024
MANIFEST_PATH = os.path.abspath(ARGS.manifest) if ARGS.manifest else None
SYNCED_STATE_PATH = os.path.abspath(ARGS.state)

MEDIA_EXTS = {
    ".jpg", ".jpeg", ".heic", ".png", ".gif", ".webp", ".tif", ".tiff", ".bmp", ".dng",
    ".mp4", ".mov", ".m4v", ".avi", ".mkv"
}
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi", ".mkv"}

# In-Memory Indexes & Caches
STATE_LOCK = threading.Lock()
SYNCED_FILES = set()
ALBUM_LOOKUP = {}  # original_filename -> album_name
BATCH_CACHE = OrderedDict()  # batch_name -> { "index": int, "files": [...] }

# RAM Read-Ahead Cache
RAM_CACHE_LOCK = threading.Lock()
RAM_CACHE = OrderedDict()
RAM_CACHE_USAGE = 0

PREFETCH_QUEUE_LOCK = threading.Lock()
PREFETCH_QUEUE = []


def ram_cache_get(key):
    with RAM_CACHE_LOCK:
        if key in RAM_CACHE:
            RAM_CACHE.move_to_end(key)
            return RAM_CACHE[key]
    return None


def ram_cache_put(key, data):
    global RAM_CACHE_USAGE
    dlen = len(data)
    if dlen > MAX_RAM_CACHE_BYTES:
        return
    with RAM_CACHE_LOCK:
        if key in RAM_CACHE:
            RAM_CACHE_USAGE -= len(RAM_CACHE[key])
            del RAM_CACHE[key]
        while RAM_CACHE_USAGE + dlen > MAX_RAM_CACHE_BYTES and RAM_CACHE:
            k, v = RAM_CACHE.popitem(last=False)
            RAM_CACHE_USAGE -= len(v)
        RAM_CACHE[key] = data
        RAM_CACHE_USAGE += dlen


def request_prefetch_batch(batch_name):
    with PREFETCH_QUEUE_LOCK:
        if batch_name not in PREFETCH_QUEUE:
            PREFETCH_QUEUE.append(batch_name)


def prefetch_worker():
    while True:
        target_batch = None
        with PREFETCH_QUEUE_LOCK:
            if PREFETCH_QUEUE:
                target_batch = PREFETCH_QUEUE.pop(0)
        if not target_batch:
            time.sleep(0.5)
            continue

        binfo = BATCH_CACHE.get(target_batch)
        if not binfo:
            continue

        for f in binfo["files"]:
            key = f["key"]
            if key in SYNCED_FILES:
                continue
            if ram_cache_get(key) is not None:
                continue
            fpath = os.path.join(BATCHES_ROOT, f["relative_path"])
            try:
                with open(fpath, "rb") as fp:
                    raw = fp.read()
                ram_cache_put(key, raw)
            except Exception:
                pass


def get_wifi_ip():
    for iface in ["en0", "en1"]:
        try:
            res = subprocess.run(["ipconfig", "getifaddr", iface], capture_output=True, text=True, timeout=2)
            if res.returncode == 0 and res.stdout.strip():
                return res.stdout.strip()
        except Exception:
            pass
    return "127.0.0.1"


def get_usb_tunnel_ipv6():
    try:
        res = subprocess.run(["ifconfig"], capture_output=True, text=True, timeout=2)
        matches = re.findall(r"inet6 (fd[0-9a-fA-F:]+::2)\s+prefixlen", res.stdout)
        if matches:
            return matches[-1]
    except Exception:
        pass
    return None


def load_synced_state():
    global SYNCED_FILES
    if os.path.exists(SYNCED_STATE_PATH):
        try:
            with open(SYNCED_STATE_PATH, "r") as f:
                data = json.load(f)
                SYNCED_FILES = set(data.get("synced", []))
        except Exception as e:
            print(f"[WARN] Could not load synced state: {e}")


def save_synced_state():
    os.makedirs(os.path.dirname(os.path.abspath(SYNCED_STATE_PATH)), exist_ok=True)
    tmp = SYNCED_STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"synced": list(SYNCED_FILES), "updated_at": time.time()}, f)
    os.replace(tmp, SYNCED_STATE_PATH)


def load_album_lookup():
    global ALBUM_LOOKUP
    manifest_candidates = []
    if MANIFEST_PATH and os.path.exists(MANIFEST_PATH):
        manifest_candidates.append(MANIFEST_PATH)
    manifest_candidates.append(os.path.join(BATCHES_ROOT, "restore_manifest.json"))

    for mfile in manifest_candidates:
        if os.path.exists(mfile):
            try:
                with open(mfile, "r") as f:
                    entries = json.load(f)
                for entry in entries:
                    alb = entry.get("album")
                    if not alb:
                        continue
                    if "flat_filename" in entry:
                        ALBUM_LOOKUP[entry["flat_filename"]] = alb
                    if "original_filename" in entry:
                        ALBUM_LOOKUP[entry["original_filename"]] = alb
                    if "source_path" in entry:
                        ALBUM_LOOKUP[os.path.basename(entry["source_path"])] = alb
                print(f"[OK] Loaded {len(ALBUM_LOOKUP)} album mappings from {os.path.basename(mfile)}")
                return
            except Exception as e:
                print(f"[WARN] Could not parse manifest {mfile}: {e}")


def build_batch_index():
    global BATCH_CACHE
    BATCH_CACHE.clear()

    if not os.path.exists(BATCHES_ROOT):
        os.makedirs(BATCHES_ROOT, exist_ok=True)
        print(f"[INFO] Created empty batches directory: {BATCHES_ROOT}")
        return

    # Check if directory contains existing Batch subfolders
    entries = sorted(os.listdir(BATCHES_ROOT))
    batch_dirs = [e for e in entries if os.path.isdir(os.path.join(BATCHES_ROOT, e)) and e.lower().startswith("batch")]

    if batch_dirs:
        for idx, bname in enumerate(batch_dirs, 1):
            bpath = os.path.join(BATCHES_ROOT, bname)
            files = []
            for fname in sorted(os.listdir(bpath)):
                ext = os.path.splitext(fname)[1].lower()
                if ext not in MEDIA_EXTS:
                    continue
                fpath = os.path.join(bpath, fname)
                try:
                    st = os.stat(fpath)
                except OSError:
                    continue
                album = ALBUM_LOOKUP.get(fname)
                if not album:
                    clean_name = re.sub(r"__\d+(\.[^.]+)$", r"\1", fname)
                    album = ALBUM_LOOKUP.get(clean_name)
                files.append({
                    "key": f"{bname}/{fname}",
                    "filename": fname,
                    "relative_path": os.path.join(bname, fname),
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                    "isVideo": ext in VIDEO_EXTS,
                    "album": album,
                    "url": f"/files/{bname}/{fname}"
                })
            BATCH_CACHE[bname] = {"index": idx, "files": files}
    else:
        # Flat directory of files: automatically partition into 1,000-file virtual batches
        flat_files = []
        for root, _, fnames in os.walk(BATCHES_ROOT):
            for fname in sorted(fnames):
                ext = os.path.splitext(fname)[1].lower()
                if ext not in MEDIA_EXTS:
                    continue
                fpath = os.path.join(root, fname)
                try:
                    st = os.stat(fpath)
                except OSError:
                    continue
                rel_path = os.path.relpath(fpath, BATCHES_ROOT)
                album = ALBUM_LOOKUP.get(fname)
                flat_files.append({
                    "filename": fname,
                    "relative_path": rel_path,
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                    "isVideo": ext in VIDEO_EXTS,
                    "album": album
                })

        chunk_size = 1000
        for i in range(0, len(flat_files), chunk_size):
            chunk = flat_files[i:i + chunk_size]
            bidx = (i // chunk_size) + 1
            bname = f"Batch_{bidx:02d}"
            formatted_files = []
            for item in chunk:
                key = f"{bname}/{item['filename']}"
                formatted_files.append({
                    "key": key,
                    "filename": item["filename"],
                    "relative_path": item["relative_path"],
                    "size": item["size"],
                    "mtime": item["mtime"],
                    "isVideo": item["isVideo"],
                    "album": item["album"],
                    "url": f"/files/{key}"
                })
            BATCH_CACHE[bname] = {"index": bidx, "files": formatted_files}

    total_f = sum(len(b["files"]) for b in BATCH_CACHE.values())
    total_b = sum(sum(f["size"] for f in b["files"]) for b in BATCH_CACHE.values())
    print(f"[OK] Indexed {len(BATCH_CACHE)} batches with {total_f:,} media files ({total_b / (1024**3):.2f} GB)")


class DualStackPhotoSyncServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    address_family = socket.AF_INET6
    daemon_threads = True

    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except (AttributeError, OSError):
            pass
        super().server_bind()


class PhotoSyncHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass  # Suppress individual request logging for maximum wire speed

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)

        if path == "/api/status":
            wifi_ip = get_wifi_ip()
            usb_ip = get_usb_tunnel_ipv6()
            with STATE_LOCK:
                synced_count = len(SYNCED_FILES)
            total_f = sum(len(b["files"]) for b in BATCH_CACHE.values())
            total_bytes = sum(sum(f["size"] for f in b["files"]) for b in BATCH_CACHE.values())
            self._send_json({
                "status": "ok",
                "serverIP": wifi_ip,
                "usbTunnelIP": usb_ip,
                "usbURL": f"http://[{usb_ip}]:{PORT}" if usb_ip else None,
                "wifiURL": f"http://{wifi_ip}:{PORT}",
                "port": PORT,
                "totalBatches": len(BATCH_CACHE),
                "totalFiles": total_f,
                "totalBytes": total_bytes,
                "syncedFiles": synced_count
            })
            return

        if path == "/api/batches":
            with STATE_LOCK:
                synced_snap = set(SYNCED_FILES)
            summaries = []
            for bname, binfo in BATCH_CACHE.items():
                bfiles = binfo["files"]
                photos = sum(1 for f in bfiles if not f["isVideo"])
                videos = sum(1 for f in bfiles if f["isVideo"])
                total_sz = sum(f["size"] for f in bfiles)
                synced = sum(1 for f in bfiles if f["key"] in synced_snap)
                albs = sorted(list(set(f["album"] for f in bfiles if f["album"])))
                summaries.append({
                    "id": bname,
                    "index": binfo["index"],
                    "totalFiles": len(bfiles),
                    "photosCount": photos,
                    "videosCount": videos,
                    "totalBytes": total_sz,
                    "syncedFiles": synced,
                    "albums": albs
                })
            self._send_json({"batches": summaries})
            return

        if path.startswith("/api/batches/"):
            batch_id = path.split("/api/batches/")[1].strip("/")
            if batch_id not in BATCH_CACHE:
                self._send_json({"error": "Batch not found"}, status=404)
                return
            binfo = BATCH_CACHE[batch_id]
            with STATE_LOCK:
                synced_snap = set(SYNCED_FILES)
            albs = sorted(list(set(f["album"] for f in binfo["files"] if f["album"])))
            total_sz = sum(f["size"] for f in binfo["files"])
            files_out = []
            for f in binfo["files"]:
                files_out.append({
                    "key": f["key"],
                    "filename": f["filename"],
                    "size": f["size"],
                    "mtime": f["mtime"],
                    "isVideo": f["isVideo"],
                    "album": f["album"],
                    "url": f["url"],
                    "synced": f["key"] in synced_snap
                })
            request_prefetch_batch(batch_id)
            self._send_json({
                "id": batch_id,
                "index": binfo["index"],
                "totalFiles": len(files_out),
                "totalBytes": total_sz,
                "albums": albs,
                "files": files_out
            })
            return

        if path.startswith("/files/"):
            rel_req = path[len("/files/"):].lstrip("/")
            key = rel_req
            cached_bytes = ram_cache_get(key)
            if cached_bytes is not None:
                fname = os.path.basename(rel_req)
                ctype, _ = mimetypes.guess_type(fname)
                self.send_response(200)
                self.send_header("Content-Type", ctype or "application/octet-stream")
                self.send_header("Content-Length", str(len(cached_bytes)))
                self.send_header("X-Cache", "RAM-HIT")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                try:
                    self.wfile.write(cached_bytes)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            fpath = os.path.join(BATCHES_ROOT, rel_req)
            if not os.path.isfile(fpath):
                self.send_error(404, "File not found")
                return

            ctype, _ = mimetypes.guess_type(fpath)
            fsize = os.path.getsize(fpath)
            self.send_response(200)
            self.send_header("Content-Type", ctype or "application/octet-stream")
            self.send_header("Content-Length", str(fsize))
            self.send_header("X-Cache", "DISK")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            with open(fpath, "rb") as f:
                while True:
                    buf = f.read(4 * 1024 * 1024)  # 4 MB sequential buffer
                    if not buf:
                        break
                    try:
                        self.wfile.write(buf)
                    except (BrokenPipeError, ConnectionResetError):
                        break
            return

        self._send_json({"message": "Turbo Photo Sync Streaming Server Running"}, status=200)

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/pack":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
                keys = payload.get("keys", [])
            except Exception as e:
                self._send_json({"error": str(e)}, status=400)
                return

            try:
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass

            chunks_to_write = []
            total_len = 4
            for key in keys:
                data = ram_cache_get(key)
                if data is None:
                    fpath = os.path.join(BATCHES_ROOT, key)
                    if not os.path.isfile(fpath):
                        continue
                    try:
                        with open(fpath, "rb") as fp:
                            data = fp.read()
                    except Exception:
                        continue
                meta_json = json.dumps({"key": key, "size": len(data)}).encode("utf-8")
                hdr = struct.pack(">I", len(meta_json))
                chunks_to_write.append((hdr, meta_json, data))
                total_len += len(hdr) + len(meta_json) + len(data)

            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(total_len))
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            try:
                for hdr, meta_bytes, data in chunks_to_write:
                    self.wfile.write(hdr)
                    self.wfile.write(meta_bytes)
                    self.wfile.write(data)
                self.wfile.write(struct.pack(">I", 0))
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        if parsed.path == "/api/mark_synced":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
                keys = payload.get("keys", [])
                if isinstance(keys, list) and keys:
                    with STATE_LOCK:
                        SYNCED_FILES.update(keys)
                        save_synced_state()
                self._send_json({"ok": True, "syncedTotal": len(SYNCED_FILES)})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            return

        self._send_json({"error": "Not found"}, status=404)


if __name__ == "__main__":
    load_synced_state()
    load_album_lookup()
    build_batch_index()

    t = threading.Thread(target=prefetch_worker, daemon=True)
    t.start()

    with STATE_LOCK:
        synced_snap = set(SYNCED_FILES)
    for bname, binfo in sorted(BATCH_CACHE.items()):
        if any(f["key"] not in synced_snap for f in binfo["files"]):
            request_prefetch_batch(bname)
            break

    wifi_ip = get_wifi_ip()
    usb_ip = get_usb_tunnel_ipv6()
    print("\n==============================================================")
    print(" 🚀 Turbo Photo Sync Streaming Server")
    print(f" 📂 Media Source         : {BATCHES_ROOT}")
    if usb_ip:
        print(f" ⚡ USB Cable Tunnel URL : http://[{usb_ip}]:{PORT}")
    print(f" 📶 5GHz Wi-Fi URL       : http://{wifi_ip}:{PORT}")
    print(f" 💾 RAM Read-Ahead Cache : {ARGS.cache_size} MB Active")
    print(f" 📦 Total Batches        : {len(BATCH_CACHE)} | Synced: {len(SYNCED_FILES):,}")
    print("==============================================================\n")
    server = DualStackPhotoSyncServer(("::", PORT), PhotoSyncHandler)
    server.serve_forever()

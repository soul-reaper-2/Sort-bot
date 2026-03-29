import asyncio
import re
import os
import json
import time
from collections import defaultdict
import aiosqlite
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

# ---------------- CONFIGURATION ---------------- #
BOT_TOKEN = "YOUR_BOT_TOKEN"  # <-- Set your bot token here
OWNER_USER_ID = 123456789     # <-- Set your Telegram user ID

DB_FILE = "sortbot.db"
VIDEO_EXTS = [".mkv", ".mp4", ".avi", ".mov"]
ALLOWED_FORM_FIELDS = ["season", "quality", "episode", "title", "audio"]
SKIP_EP_LABELS = {"OVA", "SP", "SPECIAL", "MOVIE"}
HELP_CATEGORIES = [
    ("Queue Commands", "queue"),
    ("Admin Commands", "admin"),
    ("Channel Commands", "channel"),
    ("Form / Sorting", "form"),
    ("Header / Footer", "headerfooter"),
    ("Other", "other"),
]

# ---------------- DATABASE INIT ---------------- #
DB_INIT_SCRIPT = """
CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS channels (
    user_id INTEGER,
    channel_id INTEGER,
    PRIMARY KEY (user_id, channel_id)
);

CREATE TABLE IF NOT EXISTS user_settings (
    user_id INTEGER PRIMARY KEY,
    sort_form TEXT DEFAULT 'season-quality-episode'
);

CREATE TABLE IF NOT EXISTS user_queue (
    user_id INTEGER,
    file_id TEXT,
    chat_id INTEGER,
    msg_id INTEGER,
    file_type TEXT,
    filename TEXT,
    title TEXT,
    season TEXT,
    episode TEXT,
    quality TEXT,
    audio TEXT,
    ts INTEGER
);

CREATE TABLE IF NOT EXISTS header_footer (
    user_id INTEGER PRIMARY KEY,
    header_type TEXT DEFAULT NULL,
    header_value TEXT DEFAULT NULL,
    footer_type TEXT DEFAULT NULL,
    footer_value TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS resend_state (
    user_id INTEGER PRIMARY KEY,
    data TEXT
);
"""

async def db_exec(query, args=(), fetchone=False, fetchall=False, commit=False):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(query, args)
        ret = None
        if fetchone:
            ret = await cur.fetchone()
        elif fetchall:
            ret = await cur.fetchall()
        if commit:
            await db.commit()
        return ret

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.executescript(DB_INIT_SCRIPT)
        await db.commit()

# -------------- ADMIN & USER HELPERS ------------ #
async def is_admin(user_id):
    if user_id == OWNER_USER_ID:
        return True
    row = await db_exec("SELECT 1 FROM admins WHERE user_id = ?", (user_id,), fetchone=True)
    return row is not None

async def add_admin(user_id):
    await db_exec("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (user_id,), commit=True)

async def remove_admin(user_id):
    await db_exec("DELETE FROM admins WHERE user_id = ?", (user_id,), commit=True)

async def list_admins():
    rows = await db_exec("SELECT user_id FROM admins", fetchall=True)
    return [OWNER_USER_ID] + [r[0] for r in rows if r[0] != OWNER_USER_ID]

# --------------- CHANNEL HELPERS ---------------- #
async def add_channel(user_id, channel_id):
    await db_exec("INSERT OR IGNORE INTO channels (user_id, channel_id) VALUES (?,?)", (user_id, channel_id), commit=True)
async def remove_channel(user_id, channel_id):
    await db_exec("DELETE FROM channels WHERE user_id=? AND channel_id=?", (user_id, channel_id), commit=True)
async def list_channels(user_id):
    rows = await db_exec("SELECT channel_id FROM channels WHERE user_id=?", (user_id,), fetchall=True)
    return [r[0] for r in rows]

# -------------- USER SETTINGS/FORM -------------- #
async def set_form(user_id, form_str):
    await db_exec("INSERT OR REPLACE INTO user_settings (user_id, sort_form) VALUES (?,?)", (user_id, form_str), commit=True)
async def get_form(user_id):
    row = await db_exec("SELECT sort_form FROM user_settings WHERE user_id=?", (user_id,), fetchone=True)
    return (row[0] if row else "season-quality-episode")

# -------------- HEADER/FOOTER SYSTEM ------------ #
async def set_header(user_id, htype, hval):
    row = await db_exec("SELECT 1 FROM header_footer WHERE user_id = ?", (user_id,), fetchone=True)
    if not row:
        await db_exec("INSERT INTO header_footer (user_id, header_type, header_value) VALUES (?,?,?)",
            (user_id, htype, hval), commit=True)
    else:
        await db_exec("UPDATE header_footer SET header_type=?, header_value=? WHERE user_id=?",
            (htype, hval, user_id), commit=True)
async def set_footer(user_id, ftype, fval):
    row = await db_exec("SELECT 1 FROM header_footer WHERE user_id = ?", (user_id,), fetchone=True)
    if not row:
        await db_exec("INSERT INTO header_footer (user_id, footer_type, footer_value) VALUES (?,?,?)",
            (user_id, ftype, fval), commit=True)
    else:
        await db_exec("UPDATE header_footer SET footer_type=?, footer_value=? WHERE user_id=?",
            (ftype, fval, user_id), commit=True)
async def clear_header(user_id):
    await db_exec("UPDATE header_footer SET header_type=NULL, header_value=NULL WHERE user_id=?", (user_id,), commit=True)
async def clear_footer(user_id):
    await db_exec("UPDATE header_footer SET footer_type=NULL, footer_value=NULL WHERE user_id=?", (user_id,), commit=True)
async def get_header(user_id):
    row = await db_exec("SELECT header_type, header_value FROM header_footer WHERE user_id=?", (user_id,), fetchone=True)
    return row if row and row[0] else (None, None)
async def get_footer(user_id):
    row = await db_exec("SELECT footer_type, footer_value FROM header_footer WHERE user_id=?", (user_id,), fetchone=True)
    return row if row and row[0] else (None, None)

# --------------- QUEUE MANAGEMENT --------------- #
async def add_to_queue(user_id, file_id, chat_id, msg_id, file_type, filename, parsed):
    await db_exec(
        """INSERT INTO user_queue (user_id, file_id, chat_id, msg_id, file_type, filename, title, season, episode, quality, audio, ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (user_id, file_id, chat_id, msg_id, file_type, filename, parsed["title"], parsed["season"], parsed["episode"], parsed["quality"], parsed["audio"], int(time.time())),
        commit=True)
async def get_queue(user_id):
    rows = await db_exec("""
        SELECT file_id, chat_id, msg_id, file_type, filename, title, season, episode, quality, audio
        FROM user_queue WHERE user_id=?
        ORDER BY ts ASC
    """, (user_id,), fetchall=True)
    return [{
        "file_id": r[0], "chat_id": r[1], "msg_id": r[2], "file_type": r[3], "filename": r[4],
        "title": r[5], "season": r[6], "episode": r[7], "quality": r[8], "audio": r[9]
    } for r in rows]
async def clear_queue(user_id):
    await db_exec("DELETE FROM user_queue WHERE user_id=?", (user_id,), commit=True)

# --------------- RESEND STATE ------------------- #
async def set_resend(user_id, data):
    await db_exec("INSERT OR REPLACE INTO resend_state (user_id, data) VALUES (?,?)", (user_id, json.dumps(data)), commit=True)
async def get_resend(user_id):
    row = await db_exec("SELECT data FROM resend_state WHERE user_id=?", (user_id,), fetchone=True)
    return json.loads(row[0]) if row else None
async def clear_resend(user_id):
    await db_exec("DELETE FROM resend_state WHERE user_id=?", (user_id,), commit=True)

# --------------- PARSING FUNCTION --------------- #
def parse_filename(filename: str):
    name = re.sub(r"\.[^.]+$", "", filename)
    name = re.sub(r"^\[.*?\]\s*", "", name)
    name = re.sub(r"@\w+", "", name)
    season = None; episode = None; quality = None; audio = None
    # Season
    sm = re.search(r"S(?:eason)?[ .-]?(\d+)", name, re.I)
    if not sm:
        sm = re.search(r"\bS(\d+)\b", name, re.I)
    if sm:
        season = str(int(sm.group(1)))
    # Episode
    em = re.search(r"\bE[pisode\s\-]*(\d+(\.\d+)?)", name, re.I)
    if em:
        episode = em.group(1)
    else:
        em = re.search(r"\bEp[\s\-]*(\d+(\.\d+)?)", name, re.I)
        if em:
            episode = em.group(1)
        else:
            em = re.search(r"(\d+\.\d+|\d+)\b", name)
            if em:
                episode = em.group(1)
    # Quality
    qm = re.search(r"(2160p|1080p|720p|480p|360p)", name)
    if qm:
        quality = qm.group(1)
    # Audio
    am = re.search(r"(Dual Audio|Sub|Dub)", name, re.I)
    if am:
        audio = am.group(1)
    # Title extraction
    tmp = name
    tmp = re.sub(r"S(?:eason)?[ .-]?\d+", "", tmp, flags=re.I)
    tmp = re.sub(r"Ep(isode)?[-\s]*\d+(\.\d+)?", "", tmp, flags=re.I)
    tmp = re.sub(r"(Dual Audio|Sub|Dub)", "", tmp, flags=re.I)
    tmp = re.sub(r"(2160p|1080p|720p|480p|360p)", "", tmp)
    tmp = re.sub(r"[\-\[\]\(\)]", "", tmp)
    tmp = re.sub(r"\s+", " ", tmp)
    title = tmp.strip()
    # Special check for SKIP_EP_LABELS (for episode)
    if episode and any(lbl.lower() in str(episode).lower() for lbl in SKIP_EP_LABELS):
        episode = None
    return {
        "filename": filename,
        "title": title or None,
        "season": season,
        "episode": episode,
        "quality": quality,
        "audio": audio,
    }

def valid_video_file(filename: str):
    return any(filename.lower().endswith(ext) for ext in VIDEO_EXTS)

def sort_files(files, form_fields):
    # Remove any form fields that ALL files lack (auto-fallback)
    fields_to_keep = []
    for f in form_fields:
        if any(x.get(f) for x in files):
            fields_to_keep.append(f)
    form_fields = fields_to_keep if fields_to_keep else form_fields
    skipped = []
    segments = defaultdict(list)
    for f in files:
        missing = [fld for fld in form_fields if not f.get(fld)]
        if missing:
            skipped.append((f, f"Missing {', '.join(missing)}"))
            continue
        key = tuple(f.get(k) for k in form_fields[:-1]) if len(form_fields) > 1 else ("all",)
        segments[key].append(f)
    # Sort in each segment
    for seg in segments:
        epkey = form_fields[-1] if form_fields else "episode"
        try:
            segments[seg].sort(key=lambda x: float(x.get(epkey) or 0))
        except Exception:
            pass
    return segments, skipped, form_fields

def format_segments(segments, form_fields):
    # Return readable names for buttons/etc.
    def fmt(seg):
        out = []
        for k, v in zip(form_fields[:-1], seg):
            if v: out.append(f"{k.capitalize()} {v}")
        return " ".join(out) if out else "All"
    return [fmt(x) for x in segments.keys()]

def render_placeholder(txt, group_data):
    def rep(m):
        key = m.group(1)
        return str(group_data.get(key, ""))
    return re.sub(r"\{(\w+)\}", rep, txt or "")

# --------------- TELEGRAM BOT SETUP ------------- #
app = Client("sortbot", bot_token=BOT_TOKEN)

# --------------- MISC HELPERS ------------------- #
async def check_admin_rights(client, channel_id):
    try:
        member = await client.get_chat_member(channel_id, "me")
        if member.status in ["administrator", "creator"]:
            return True
    except Exception:
        return False
    return False

async def username_from_id(client, user_id):
    try:
        u = await client.get_users(user_id)
        return u.username or ""
    except Exception:
        return ""
async def chat_name_from_id(client, chat_id):
    try:
        c = await client.get_chat(chat_id)
        return c.username or c.title
    except Exception:
        return str(chat_id)

# --------------- COMMAND HANDLERS ---------------- #

@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply("Welcome to Sort-bot!\nUse /help to see command categories.")

@app.on_message(filters.command("help"))
async def help_cmd(client, message):
    kbs = [
        [InlineKeyboardButton(x[0], callback_data=f"help_{x[1]}")] for x in HELP_CATEGORIES
    ] + [[InlineKeyboardButton("Cancel", callback_data="help_cancel")]]
    await message.reply("Sort-bot Help:\nChoose a category.", reply_markup=InlineKeyboardMarkup(kbs))

@app.on_callback_query(filters.regex(r"^help_"))
async def help_cb(client, cq):
    cat = cq.data.split("_", 1)[1]
    if cat == "queue":
        text = (
            "**Queue Commands**\n"
            "`/sort` - Sort and send current queue\n"
            "`/clear` - Clear queue\n"
            "Send a video/document to add to queue."
        )
    elif cat == "admin":
        text = (
            "**Admin Commands**\n"
            "`/auser user_id` - Add admin\n"
            "`/ruser user_id` - Remove admin\n"
            "`/luser` - List admins"
        )
    elif cat == "channel":
        text = (
            "**Channel Commands**\n"
            "`/acha channel_id` - Add channel\n"
            "`/rcha channel_id` - Remove channel\n"
            "`/lcha` - List channels"
        )
    elif cat == "form":
        text = (
            "**Form/Sorting**\n"
            "`/form fields` - Set sorting form, e.g. `/form season-quality-episode`\n"
            "Allowed: season, quality, episode, title, audio"
        )
    elif cat == "headerfooter":
        text = (
            "**Header/Footer**\n"
            "`/sh text` - Set header\n"
            "`/sf text` - Set footer\n"
            "`/rh` - Remove header\n"
            "`/rf` - Remove footer\n"
            "`/h` or `/f` alone to view\n"
            "Reply /h or /f to media to set image/sticker"
        )
    elif cat == "other":
        text = (
            "**Other**\n"
            "- Only direct uploads, no downloads/renames/uploads\n"
            "- Per-user queue/settings: no shared forms"
        )
    else:
        await cq.message.delete()
        return
    kbs = [[InlineKeyboardButton("Back", callback_data="help_back")]] + ([[InlineKeyboardButton("Cancel", callback_data="help_cancel")]] if cat!="main" else [])
    await cq.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kbs))

@app.on_callback_query(filters.regex(r"^help_back$"))
async def help_back(client, cq):
    kbs = [
        [InlineKeyboardButton(x[0], callback_data=f"help_{x[1]}")] for x in HELP_CATEGORIES
    ] + [[InlineKeyboardButton("Cancel", callback_data="help_cancel")]]
    await cq.edit_message_text("Sort-bot Help:\nChoose a category.", reply_markup=InlineKeyboardMarkup(kbs))

@app.on_callback_query(filters.regex(r"^help_cancel$"))
async def help_cancel(client, cq): await cq.message.delete()

# ---------- ADMIN USER/CHANNEL MANAGEMENT -------- #

@app.on_message(filters.command("auser"))
async def auser(client, msg):
    if msg.from_user.id != OWNER_USER_ID:
        return await msg.reply("Only the owner may add admins.")
    if len(msg.command)<2:
        return await msg.reply("Usage: /auser user_id")
    try:
        user_id = int(msg.command[1])
        await add_admin(user_id)
        await msg.reply(f"Added admin: `{user_id}`.")
    except Exception:
        await msg.reply("Invalid user_id.")

@app.on_message(filters.command("ruser"))
async def ruser(client, msg):
    if msg.from_user.id != OWNER_USER_ID:
        return await msg.reply("Only the owner may remove admins.")
    if len(msg.command)<2:
        return await msg.reply("Usage: /ruser user_id")
    try:
        user_id = int(msg.command[1])
        await remove_admin(user_id)
        await msg.reply(f"Removed admin: `{user_id}`.")
    except Exception:
        await msg.reply("Invalid user_id.")

@app.on_message(filters.command("luser"))
async def luser(client, msg):
    admins = await list_admins()
    lines = []
    for idx, uid in enumerate(admins, 1):
        uname = await username_from_id(client, uid)
        lines.append(f"{idx}, {uid} - @{uname}" if uname else f"{idx}, {uid}")
    await msg.reply("Admins:\n" + "\n".join(lines))

@app.on_message(filters.command("acha"))
async def acha(client, msg):
    if not (await is_admin(msg.from_user.id)):
        return await msg.reply("Only admins may add channels.")
    if len(msg.command)<2:
        return await msg.reply("Usage: /acha channel_id")
    try:
        channel_id = int(msg.command[1])
        if not await check_admin_rights(client, channel_id):
            return await msg.reply("Bot must be admin in the channel to add it.")
        await add_channel(msg.from_user.id, channel_id)
        await msg.reply(f"Added channel `{channel_id}`.")
    except Exception:
        await msg.reply("Invalid channel ID or bot not admin.")

@app.on_message(filters.command("rcha"))
async def rcha(client, msg):
    if not (await is_admin(msg.from_user.id)):
        return await msg.reply("Only admins may remove channels.")
    if len(msg.command)<2:
        return await msg.reply("Usage: /rcha channel_id")
    try:
        channel_id = int(msg.command[1])
        await remove_channel(msg.from_user.id, channel_id)
        await msg.reply(f"Removed channel `{channel_id}`.")
    except Exception:
        await msg.reply("Invalid channel ID.")

@app.on_message(filters.command("lcha"))
async def lcha(client, msg):
    chas = await list_channels(msg.from_user.id)
    out = []
    for idx, cha in enumerate(chas, 1):
        nm = await chat_name_from_id(client, cha)
        out.append(f"{idx}, {cha} - {nm}")
    await msg.reply("Channels:\n" + ("\n".join(out) or "(none)"))

# ------------------- SORT FORM ------------------ #
@app.on_message(filters.command("form"))
async def form_cmd(client, msg):
    if not (await is_admin(msg.from_user.id)):
        return await msg.reply("Only admins may set form.")
    if len(msg.command)<2:
        cur = await get_form(msg.from_user.id)
        return await msg.reply(f"Current sort form: `{cur}`\nUsage: /form season-quality-episode")
    form_str = msg.text.split(None, 1)[1].replace(" ", "").lower()
    fields = form_str.split("-")
    if not all(f in ALLOWED_FORM_FIELDS for f in fields) or len(set(fields)) != len(fields) or not fields:
        return await msg.reply(f"Invalid form. Allowed: {', '.join(ALLOWED_FORM_FIELDS)}")
    await set_form(msg.from_user.id, "-".join(fields))
    await msg.reply(f"Sort form set to: `{ '-'.join(fields) }`")

@app.on_message(filters.command("clear"))
async def clear_cmd(client, msg):
    await clear_queue(msg.from_user.id)
    await msg.reply("Queue cleared.")

# ---------------- HEADER/FOOTER SYSTEM ---------- #
@app.on_message(filters.command("sh"))
async def sh_cmd(client, msg):  # Set header
    txt = msg.text.split(None, 1)[1] if len(msg.command) > 1 else None
    if not txt: return await msg.reply("Usage: /sh some text")
    await set_header(msg.from_user.id, "text", txt)
    await msg.reply("Header set.")

@app.on_message(filters.command("sf"))
async def sf_cmd(client, msg):  # Set footer
    txt = msg.text.split(None, 1)[1] if len(msg.command) > 1 else None
    if not txt: return await msg.reply("Usage: /sf some text")
    await set_footer(msg.from_user.id, "text", txt)
    await msg.reply("Footer set.")

@app.on_message(filters.command("rh"))
async def rh_cmd(client, msg):  # Remove header
    await clear_header(msg.from_user.id)
    await msg.reply("Header removed.")

@app.on_message(filters.command("rf"))
async def rf_cmd(client, msg):  # Remove footer
    await clear_footer(msg.from_user.id)
    await msg.reply("Footer removed.")

@app.on_message(filters.command("h"))
async def h_cmd(client, msg):   # View/set header
    if msg.reply_to_message:
        media = msg.reply_to_message
        hid, hval = None, None
        if media.photo:
            hid, hval = "photo", media.photo.file_id
        elif media.sticker:
            hid, hval = "sticker", media.sticker.file_id
        else:
            return await msg.reply("Reply to image or sticker for header.")
        await set_header(msg.from_user.id, hid, hval)
        return await msg.reply("Header set (media).")
    # View header
    htype, hval = await get_header(msg.from_user.id)
    if not htype: return await msg.reply("No header set.")
    if htype == "text": await msg.reply(f"Header: {hval}")
    if htype == "photo": await msg.reply_photo(hval, caption="Header")
    if htype == "sticker": await msg.reply_sticker(hval)

@app.on_message(filters.command("f"))
async def f_cmd(client, msg):   # View/set footer
    if msg.reply_to_message:
        media = msg.reply_to_message
        fid, fval = None, None
        if media.photo:
            fid, fval = "photo", media.photo.file_id
        elif media.sticker:
            fid, fval = "sticker", media.sticker.file_id
        else:
            return await msg.reply("Reply to image or sticker for footer.")
        await set_footer(msg.from_user.id, fid, fval)
        return await msg.reply("Footer set (media).")
    # View footer
    ftype, fval = await get_footer(msg.from_user.id)
    if not ftype: return await msg.reply("No footer set.")
    if ftype == "text": await msg.reply(f"Footer: {fval}")
    if ftype == "photo": await msg.reply_photo(fval, caption="Footer")
    if ftype == "sticker": await msg.reply_sticker(fval)

# ---------------- ACCEPT FILES ------------------ #
@app.on_message(filters.document | filters.video)
async def on_file(client, msg):
    if not (await is_admin(msg.from_user.id)):
        return
    if msg.document:
        fn = msg.document.file_name or ""
        file_type = "document"
        file_id = msg.document.file_id
    elif msg.video:
        fn = msg.video.file_name or ""
        file_type = "video"
        file_id = msg.video.file_id
    else:
        return
    if not valid_video_file(fn):
        return await msg.reply("Only video files: .mp4, .mkv, .avi, .mov etc.")
    parsed = parse_filename(fn)
    await add_to_queue(msg.from_user.id, file_id, msg.chat.id, msg.id, file_type, fn, parsed)
    await msg.reply("Added to queue.")

# -------- SORT QUEUE & SEND TO CHANNELS --------- #
@app.on_message(filters.command("sort"))
async def sort_cmd(client, msg):
    if not (await is_admin(msg.from_user.id)):
        return await msg.reply("Not authorized.")
    queue = await get_queue(msg.from_user.id)
    if not queue:
        return await msg.reply("Queue is empty.")
    chas = await list_channels(msg.from_user.id)
    if not chas:
        return await msg.reply("No dump channels configured! Use /acha channel_id.")
    form = (await get_form(msg.from_user.id)).split("-")
    segs, skipped, used_form = sort_files(queue, form)
    if not segs:
        return await msg.reply("No valid files to sort. All files missing required fields?")
    state = {
        "used_form": used_form, "segments": {str(k): [q['file_id'] for q in files] for k,files in segs.items()},
        "channels": chas
    }
    await set_resend(msg.from_user.id, state)
    await show_channel_picker(client, msg, chas, segs, skipped)

async def show_channel_picker(client, msg, chas, segs, skipped):
    if len(chas)==1:
        await actually_send(client, msg, chas, segs, skipped)
        return
    kbs = [[InlineKeyboardButton(f"{await chat_name_from_id(client, c)}", callback_data=f"pick_{c}")]
        for c in chas] + [[InlineKeyboardButton("Send", callback_data="pick_send")],
                          [InlineKeyboardButton("Cancel", callback_data="pick_cancel")]]
    await msg.reply("Choose channel(s) to send:", reply_markup=InlineKeyboardMarkup(kbs))
    client._pick_state[msg.from_user.id] = {"sel": set(), "chas": chas, "segs": segs, "skipped": skipped, "msg_id": msg.id}
    
app._pick_state = dict()

@app.on_callback_query(filters.regex("^pick_"))
async def pick_cb(client, cq):
    user_id = cq.from_user.id
    state = app._pick_state.get(user_id)
    if not state:
        await cq.answer("Expired.")
        return
    d = cq.data
    if d == "pick_send":
        sel = state["sel"] or state["chas"]
        await actually_send(client, cq.message, list(sel), state["segs"], state["skipped"])
        del app._pick_state[user_id]
        try: await cq.message.delete()
        except: pass
        return
    if d == "pick_cancel":
        await cq.message.edit_text("Sort/copy cancelled.")
        del app._pick_state[user_id]
        return
    if d.startswith("pick_"):
        cid = int(d.split("_",1)[1])
        if cid in state["sel"]:
            state["sel"].remove(cid)
        else:
            state["sel"].add(cid)
        await cq.answer(f"Selected: {', '.join(str(x) for x in state['sel'])}")

async def actually_send(client, msg, chas, segs, skipped):
    # For each segment: send header, files, footer
    user_id = msg.from_user.id
    form = (await get_form(user_id)).split("-")
    # Prepare resend state
    header, hval = await get_header(user_id)
    footer, fval = await get_footer(user_id)
    for segkey, files in segs.items():
        # Parse seg fields
        pf = dict(zip(form[:-1], eval(segkey) if isinstance(segkey,str) else segkey))
        # Header
        if header == "text":
            await msg.reply(render_placeholder(hval, pf))
        elif header == "photo":
            await msg.reply_photo(hval, caption=render_placeholder("", pf))
        elif header == "sticker":
            await msg.reply_sticker(hval)
        # Send files to channels
        for f in files:
            for cha in chas:
                try:
                    if f['file_type']=="document":
                        await client.copy_message(cha, f['chat_id'], f['msg_id'])
                    else:
                        await client.copy_message(cha, f['chat_id'], f['msg_id'])
                except Exception:
                    pass
        # Footer
        if footer == "text":
            await msg.reply(render_placeholder(fval, pf))
        elif footer == "photo":
            await msg.reply_photo(fval, caption=render_placeholder("", pf))
        elif footer == "sticker":
            await msg.reply_sticker(fval)
    # Skipped file report
    if skipped:
        txt = "Skipped / Unsorted Files:\n" + "\n".join(
            f"{i+1}. {f[0]['filename']} → {f[1]}" for i, f in enumerate(skipped)
        )
        await msg.reply(txt)
    # Show Done/Resend buttons
    kbs = [[InlineKeyboardButton("Done", callback_data="done"),
            InlineKeyboardButton("Resend", callback_data="resend")]]
    await msg.reply("Done! Queue is still present until Done is clicked.", reply_markup=InlineKeyboardMarkup(kbs))

@app.on_callback_query(filters.regex(r"^(done|resend)$"))
async def done_resend(client, cq):
    user_id = cq.from_user.id
    if cq.data=="done":
        await clear_queue(user_id)
        await clear_resend(user_id)
        await cq.answer("Queue cleared!")
        await cq.message.delete()
    elif cq.data=="resend":
        state = await get_resend(user_id)
        if not state:
            await cq.answer("Nothing to resend!")
            return
        # Reconstitute
        chas = state["channels"]
        segs = {eval(k): [] for k in state["segments"].keys()}
        qlist = await get_queue(user_id)
        # Match files from state with current queue
        for segstr, fileids in state["segments"].items():
            files = []
            for fid in fileids:
                for f in qlist:
                    if f["file_id"] == fid:
                        files.append(f)
                        break
            segs[eval(segstr)]=files
        await actually_send(client, cq.message, chas, segs, [])
        await cq.answer("Resent!")

# ------------- BOT STARTUP ---------------------- #
async def main():
    await init_db()
    print("Sort-bot is starting...")
    await app.start()
    await idle()

if __name__ == "__main__":
    asyncio.run(main())

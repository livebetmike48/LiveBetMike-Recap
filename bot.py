"""
LiveBetMike Plays Recap bot -- AUTONOMOUS edition, v2 (multi-server).

Reads the picks channel itself, parses each posted play, grades it against
real box scores (MLB StatsAPI / ESPN for WNBA), and posts a nightly recap
at 12:30 AM ET with the day's record, units, YTD and ROI.

Play format it parses (Mike's canonical skeleton; everything after the
market -- book lines, screenshots -- is ignored):

    Risk 1.02 To Win 1U: Tyler Phillips (Marlins) UNDER 14.5 Outs. ...

Brand rule: a play the parser can't read is listed in the recap as
"couldn't grade" -- never guessed at. Units come straight from the play
text (win = +to_win, loss = -risk, push = 0), so no odds math can drift.

WHAT'S NEW IN v2
  * PER-SERVER. Every config, play, ledger and recap is keyed by guild_id,
    so Fun House and LBM run completely separate books of the same sport.
    Existing single-server data is migrated automatically on first boot.
  * /logplay -- manually enter a play, optionally BACKDATED to a past date,
    so a missed play still lands in the ledger and that day's recap.
  * /resetledger -- wipe one sport's plays in THIS server (confirm button).
  * ONE PLAY, ONCE. Same sport + date + player + market + line + side is a
    single play however many messages carry it. Repeats are stored, labeled
    'duplicate', and never counted. The recap says how many it skipped.
  * REAL EASTERN TIME. Dates and the 12:30 AM recap use America/New_York,
    not a hardcoded UTC-4, so the November clock change doesn't move it.
  * PERMISSION PREFLIGHT. The 403 that silently ate a recap now names the
    channel and the exact missing permission, in the log and in /recapnow.

Setup: enable MESSAGE CONTENT INTENT in the Discord dev portal (Bot tab).
Railway vars: DISCORD_TOKEN, DB_PATH (volume path, e.g. /data/recap.db).
"""
import os
import re
import logging
import asyncio
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo

import requests
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "recap_bot.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recap_bot")

ET = ZoneInfo("America/New_York")
MLB_BASE = "https://statsapi.mlb.com/api/v1"
SPORTS = ["MLB", "WNBA", "NBA", "NFL"]
GRADED_SPORTS = {"MLB", "WNBA"}  # graders wired so far

COUNTED = ("win", "loss", "push")  # the only statuses that touch units


# ---------------------------------------------------------------- time
def _et_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(ET)


def _et_date_str(offset_days: int = 0) -> str:
    return (_et_now() + timedelta(days=offset_days)).strftime("%Y-%m-%d")


def _et_day_bounds_utc(date_str: str):
    """UTC window covering the ET calendar day, DST-correct."""
    start_et = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
    end_et = start_et + timedelta(days=1)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def parse_date_arg(raw: str):
    """Accepts YYYY-MM-DD, MM/DD, MM/DD/YY(YY), 'today', 'yesterday'.
    Returns 'YYYY-MM-DD' or None if unreadable."""
    s = (raw or "").strip().lower()
    if not s or s == "today":
        return _et_date_str(0)
    if s == "yesterday":
        return _et_date_str(-1)
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
    else:
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})(?:/(\d{2}|\d{4}))?", s)
        if not m:
            return None
        mo, d = int(m.group(1)), int(m.group(2))
        yr = m.group(3)
        if yr is None:
            y = int(_et_date_str(0)[:4])
        else:
            y = int(yr) if len(yr) == 4 else 2000 + int(yr)
    try:
        return datetime(y, mo, d).strftime("%Y-%m-%d")
    except ValueError:
        return None


# ---------------------------------------------------------------- storage
@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _columns(c, table) -> set:
    try:
        return {r["name"] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def init_db():
    """Creates v2 schema and stages any v1 data for guild backfill on ready."""
    with _conn() as c:
        cfg_cols = _columns(c, "config")
        if cfg_cols and "guild_id" not in cfg_cols:
            # v1 config was (key, value) with no server. Park it for on_ready,
            # where channel IDs can be resolved back to their guild.
            c.execute("DROP TABLE IF EXISTS config_legacy")
            c.execute("ALTER TABLE config RENAME TO config_legacy")
            log.info("v1 config detected -- staged as config_legacy for migration")

        c.execute("""
            CREATE TABLE IF NOT EXISTS config (
                guild_id TEXT, key TEXT, value TEXT,
                PRIMARY KEY (guild_id, key)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS plays (
                message_id TEXT PRIMARY KEY,
                guild_id TEXT,
                sport TEXT, post_date TEXT,
                raw TEXT, player TEXT, team TEXT,
                side TEXT, point REAL, market TEXT,
                risk REAL, to_win REAL,
                status TEXT DEFAULT 'pending',  -- pending/win/loss/push/ungraded/duplicate
                actual REAL, profit REAL DEFAULT 0,
                source TEXT DEFAULT 'channel'   -- channel/manual
            )
        """)
        play_cols = _columns(c, "plays")
        for col, ddl in (("guild_id", "TEXT"), ("source", "TEXT DEFAULT 'channel'")):
            if col not in play_cols:
                c.execute(f"ALTER TABLE plays ADD COLUMN {col} {ddl}")
                log.info("plays: added column %s", col)
        c.execute("CREATE INDEX IF NOT EXISTS idx_plays_lookup "
                  "ON plays (guild_id, sport, post_date)")


async def migrate_legacy_guild(bot):
    """v1 stored no guild. Resolve each legacy channel ID to its guild and
    re-key config + backfill plays. Runs once; leaves data alone on failure."""
    with _conn() as c:
        if not _columns(c, "config_legacy"):
            legacy = []
        else:
            legacy = [dict(r) for r in c.execute("SELECT key, value FROM config_legacy").fetchall()]

    sport_guild = {}
    unresolved = []
    for row in legacy:
        key, value = row["key"], row["value"]
        channel = bot.get_channel(int(value)) if str(value).isdigit() else None
        if channel is None or getattr(channel, "guild", None) is None:
            unresolved.append(key)
            continue
        gid = str(channel.guild.id)
        set_config(gid, key, value)
        m = re.fullmatch(r"(?:picks|recap)_channel_(\w+)", key)
        if m and key.startswith("picks"):
            sport_guild[m.group(1)] = gid

    if legacy and not unresolved:
        with _conn() as c:
            c.execute("DROP TABLE config_legacy")
        log.info("Migrated %d v1 config rows to per-server config", len(legacy))
    elif unresolved:
        log.error("Could not resolve guild for legacy config keys %s -- "
                  "config_legacy kept, will retry next boot", unresolved)

    # Backfill plays that predate the guild column, using each sport's picks channel.
    with _conn() as c:
        orphan_sports = [r["sport"] for r in c.execute(
            "SELECT DISTINCT sport FROM plays WHERE guild_id IS NULL").fetchall()]
    if not orphan_sports:
        return
    for sport in orphan_sports:
        gid = sport_guild.get(sport)
        if gid is None:
            gid = _lookup_any_guild_for_sport(sport)
        if gid is None:
            log.error("Plays exist for %s with no server -- run /setpickschannel "
                      "for %s and they'll be adopted on the next boot", sport, sport)
            continue
        with _conn() as c:
            cur = c.execute("UPDATE plays SET guild_id=? WHERE guild_id IS NULL AND sport=?",
                            (gid, sport))
        log.info("Backfilled %d %s plays into server %s", cur.rowcount, sport, gid)


def _lookup_any_guild_for_sport(sport: str):
    with _conn() as c:
        row = c.execute("SELECT guild_id FROM config WHERE key=?",
                        (f"picks_channel_{sport}",)).fetchone()
    return row["guild_id"] if row else None


def set_config(guild_id, key, value):
    with _conn() as c:
        c.execute("INSERT INTO config (guild_id,key,value) VALUES (?,?,?) "
                  "ON CONFLICT(guild_id,key) DO UPDATE SET value=excluded.value",
                  (str(guild_id), key, str(value)))


def get_config(guild_id, key):
    with _conn() as c:
        row = c.execute("SELECT value FROM config WHERE guild_id=? AND key=?",
                        (str(guild_id), key)).fetchone()
        return row["value"] if row else None


def configured_pairs():
    """Every (guild_id, sport) with a picks channel set."""
    with _conn() as c:
        rows = c.execute("SELECT guild_id, key FROM config "
                         "WHERE key LIKE 'picks_channel_%'").fetchall()
    out = []
    for r in rows:
        sport = r["key"].replace("picks_channel_", "")
        if sport in SPORTS:
            out.append((r["guild_id"], sport))
    return out


# ---------------------------------------------------------------- parsing
# Risk 1.02 To Win 1U: Tyler Phillips (Marlins) UNDER 14.5 Outs. <noise>
PLAY_RE = re.compile(
    r"risk\s+(?P<risk>\d*\.?\d+)\s*u?\s+to\s+win\s+(?P<to_win>\d*\.?\d+)\s*u?\s*:?\s*"
    r"(?P<player>[^()\n]+?)\s*\((?P<team>[^)\n]+)\)\s+"
    r"(?P<side>over|under|o|u)\s*(?P<point>\d+(?:\.\d+)?|\.\d+)\s+"
    r"(?P<tail>[^\n]+)",
    re.IGNORECASE,
)

MARKET_MAP = {
    "outs": ("pitching", "outs"),
    "strikeouts": ("pitching", "strikeOuts"), "ks": ("pitching", "strikeOuts"),
    "k's": ("pitching", "strikeOuts"), "k": ("pitching", "strikeOuts"),
    "hits allowed": ("pitching", "hits"),
    "walks": ("pitching", "baseOnBalls"), "walks allowed": ("pitching", "baseOnBalls"),
    "earned runs": ("pitching", "earnedRuns"), "er": ("pitching", "earnedRuns"),
    "hits": ("batting", "hits"),
    "total bases": ("batting", "totalBases"), "tb": ("batting", "totalBases"),
    "h+r+rbi": ("batting", "hrr"), "hits+runs+rbi": ("batting", "hrr"),
}


def _known_aliases():
    """Longest first, so 'hits allowed' wins over 'hits' and "k's" over 'k'."""
    return sorted(set(MARKET_MAP) | set(WNBA_MARKET_MAP), key=len, reverse=True)


def extract_market(tail: str) -> str:
    """The market is whatever KNOWN market name the text after the line starts
    with -- so trailing book noise ('Outs. FD -102', 'Total Bases DK') is cut
    without needing a period to stop at. Unknown markets keep their words so
    they still display, and grading reports them as couldn't-grade."""
    t = re.sub(r"\s+", " ", (tail or "").strip().lower())
    for alias in _known_aliases():
        if re.match(re.escape(alias) + r"(?![a-z0-9+'])", t):
            return alias
    m = re.match(r"[a-z][a-z+/' ]*", t)
    return re.sub(r"\s+", " ", m.group(0).strip()) if m else ""


def parse_play(text: str):
    """Returns a play dict or None if the message doesn't match the skeleton."""
    m = PLAY_RE.search(text or "")
    if not m:
        return None
    market_raw = extract_market(m.group("tail"))
    if not market_raw:
        return None
    side = m.group("side").lower()
    return {
        "risk": float(m.group("risk")),
        "to_win": float(m.group("to_win")),
        "player": m.group("player").strip(),
        "team": m.group("team").strip(),
        "side": "Over" if side in ("over", "o") else "Under",
        "point": float(m.group("point")),
        "market": market_raw,
    }


def play_fingerprint(play: dict) -> tuple:
    """One play = one sport+date+player+market+line+side, however many
    messages carry it. Risk is deliberately NOT part of the key."""
    return (_norm(play["player"]), play["market"], float(play["point"]), play["side"])


def find_duplicate(guild_id, sport, date_str, play):
    """Returns the existing counted row for this play, or None."""
    fp = play_fingerprint(play)
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM plays WHERE guild_id=? AND sport=? AND post_date=? "
            "AND status != 'duplicate' AND player IS NOT NULL",
            (str(guild_id), sport, date_str)).fetchall()
    for r in rows:
        if (_norm(r["player"]), r["market"], r["point"], r["side"]) == fp:
            return dict(r)
    return None


# ---------------------------------------------------------------- MLB grading
def _norm(name: str) -> str:
    return re.sub(r"[^a-z]", "", (name or "").lower())


def _final_game_pks(date_str: str) -> list:
    r = requests.get(f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str}, timeout=15)
    r.raise_for_status()
    pks = []
    for d in r.json().get("dates", []):
        for g in d.get("games", []):
            if g.get("status", {}).get("abstractGameState") == "Final":
                pks.append(g["gamePk"])
    return pks


def _find_stat_in_boxscore(box: dict, player_name: str, group: str, field: str):
    want = _norm(player_name)
    for team_side in ("home", "away"):
        players = (box.get("teams", {}).get(team_side, {}) or {}).get("players", {})
        for p in players.values():
            full = (p.get("person") or {}).get("fullName", "")
            if _norm(full) != want and want not in _norm(full):
                continue
            stats = (p.get("stats") or {}).get(group) or {}
            if not stats:
                continue
            if field == "hrr":
                return float(stats.get("hits", 0) + stats.get("runs", 0) + stats.get("rbi", 0))
            if field == "outs":
                ip = str(stats.get("inningsPitched", "0.0"))
                whole, _, frac = ip.partition(".")
                return float(int(whole) * 3 + int(frac or 0))
            if field in stats:
                return float(stats[field])
    return None


def grade_mlb_play(play: dict, date_str: str, boxscore_cache: dict):
    """Returns (status, actual) or ('pending', None) if not gradeable yet."""
    mk = MARKET_MAP.get(play["market"])
    if mk is None:
        return "ungraded", None
    group, field = mk
    try:
        if "pks" not in boxscore_cache:
            boxscore_cache["pks"] = _final_game_pks(date_str)
        for pk in boxscore_cache["pks"]:
            if pk not in boxscore_cache:
                r = requests.get(f"{MLB_BASE}/game/{pk}/boxscore", timeout=20)
                r.raise_for_status()
                boxscore_cache[pk] = r.json()
            actual = _find_stat_in_boxscore(boxscore_cache[pk], play["player"], group, field)
            if actual is not None:
                if actual == play["point"]:
                    return "push", actual
                went_over = actual > play["point"]
                won = went_over if play["side"] == "Over" else not went_over
                return ("win" if won else "loss"), actual
    except Exception as e:
        log.error("Grading fetch failed for %s: %s", play["player"], e)
        return "pending", None
    return "pending", None  # player not found in any final box yet


# ---------------------------------------------------------------- WNBA grading
ESPN_WNBA = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba"

WNBA_MARKET_MAP = {
    "points": "PTS", "pts": "PTS",
    "rebounds": "REB", "rebs": "REB", "reb": "REB",
    "assists": "AST", "asts": "AST", "ast": "AST",
    "threes": "3PM", "3s": "3PM", "3pt": "3PM", "3-pointers": "3PM",
    "three pointers": "3PM", "3 pointers": "3PM", "threes made": "3PM",
    "p+r+a": ("PTS", "REB", "AST"), "pra": ("PTS", "REB", "AST"),
    "p+r": ("PTS", "REB"), "p+a": ("PTS", "AST"), "r+a": ("REB", "AST"),
}


def _wnba_completed_events(date_str: str) -> list:
    r = requests.get(f"{ESPN_WNBA}/scoreboard",
                     params={"dates": date_str.replace("-", "")}, timeout=15)
    r.raise_for_status()
    out = []
    for ev in r.json().get("events", []):
        if ((ev.get("status") or {}).get("type") or {}).get("completed"):
            out.append(str(ev["id"]))
    return out


def _wnba_player_stats(summary: dict, player_name: str):
    want = _norm(player_name)
    for team in (summary.get("boxscore") or {}).get("players", []):
        for block in team.get("statistics", []):
            labels = block.get("labels") or block.get("names") or []
            for ath in block.get("athletes", []):
                full = ((ath.get("athlete") or {}).get("displayName", ""))
                if _norm(full) != want and want not in _norm(full):
                    continue
                vals = ath.get("stats") or []
                if not vals:
                    continue
                return dict(zip(labels, vals))
    return None


def _wnba_stat_value(stats: dict, key: str):
    if key == "3PM":
        made = str(stats.get("3PT", "0-0")).split("-")[0]
        return float(made or 0)
    v = stats.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def grade_wnba_play(play: dict, date_str: str, cache: dict):
    mk = WNBA_MARKET_MAP.get(play["market"])
    if mk is None:
        return "ungraded", None
    try:
        if "events" not in cache:
            cache["events"] = _wnba_completed_events(date_str)
        for ev_id in cache["events"]:
            if ev_id not in cache:
                r = requests.get(f"{ESPN_WNBA}/summary", params={"event": ev_id}, timeout=20)
                r.raise_for_status()
                cache[ev_id] = r.json()
            stats = _wnba_player_stats(cache[ev_id], play["player"])
            if stats is None:
                continue
            if isinstance(mk, tuple):
                parts = [_wnba_stat_value(stats, k) for k in mk]
                if any(p is None for p in parts):
                    return "pending", None
                actual = float(sum(parts))
            else:
                actual = _wnba_stat_value(stats, mk)
                if actual is None:
                    return "pending", None
            if actual == play["point"]:
                return "push", actual
            went_over = actual > play["point"]
            won = went_over if play["side"] == "Over" else not went_over
            return ("win" if won else "loss"), actual
    except Exception as e:
        log.error("WNBA grading fetch failed for %s: %s", play["player"], e)
        return "pending", None
    return "pending", None


GRADERS = {"MLB": grade_mlb_play, "WNBA": grade_wnba_play}


# ---------------------------------------------------------------- permissions
READ_PERMS = ("view_channel", "read_message_history")
POST_PERMS = ("view_channel", "send_messages", "embed_links")


def missing_perms(channel, needed) -> list:
    """Returns the human-readable permissions the bot lacks in a channel."""
    guild = getattr(channel, "guild", None)
    me = getattr(guild, "me", None) if guild else None
    if me is None:
        return []
    try:
        perms = channel.permissions_for(me)
    except Exception:
        return []
    return [n.replace("_", " ").title() for n in needed if not getattr(perms, n, False)]


# ---------------------------------------------------------------- bot
intents = discord.Intents.default()
intents.message_content = True  # requires the portal toggle


class ConfirmReset(discord.ui.View):
    """Second tap before anything irreversible happens."""

    def __init__(self, user_id: int, guild_id: str, sport: str, count: int):
        super().__init__(timeout=30)
        self.user_id = user_id
        self.guild_id = str(guild_id)
        self.sport = sport
        self.count = count

    @discord.ui.button(label="Wipe it", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your confirmation.", ephemeral=True)
            return
        with _conn() as c:
            cur = c.execute("DELETE FROM plays WHERE guild_id=? AND sport=?",
                            (self.guild_id, self.sport))
        log.warning("Ledger reset: %d %s plays deleted in guild %s by %s",
                    cur.rowcount, self.sport, self.guild_id, interaction.user)
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=f"🧹 Wiped **{cur.rowcount}** {self.sport} plays from this server. "
                    f"Ledger is back to 0-0, 0.00u.", view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your confirmation.", ephemeral=True)
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Cancelled — nothing deleted.", view=self)
        self.stop()


class RecapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._migrated = False

    async def setup_hook(self):
        init_db()
        sport_choices = [app_commands.Choice(name=s, value=s) for s in SPORTS]

        # ---- /setpickschannel
        @app_commands.choices(sport=sport_choices)
        async def _setpicks(interaction: discord.Interaction,
                            sport: app_commands.Choice[str]):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            set_config(interaction.guild_id, f"picks_channel_{sport.value}", interaction.channel_id)
            gaps = missing_perms(interaction.channel, READ_PERMS)
            warn = f"\n⚠️ I'm missing **{', '.join(gaps)}** here — I can't read the plays until that's fixed." if gaps else ""
            note = ("" if sport.value in GRADED_SPORTS else
                    f" (Note: {sport.value} grading isn't wired yet — plays will be stored and listed pending.)")
            await interaction.response.send_message(
                f"✅ Scanning {interaction.channel.mention} for **{sport.value}** plays in "
                f"**{interaction.guild.name}**.{note}{warn}")

        self.tree.add_command(app_commands.Command(
            name="setpickschannel",
            description="ADMIN: scan this channel's posts as plays for a sport",
            callback=_setpicks))

        # ---- /setrecapchannel
        @app_commands.choices(sport=sport_choices)
        async def _setrecap(interaction: discord.Interaction,
                            sport: app_commands.Choice[str]):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            set_config(interaction.guild_id, f"recap_channel_{sport.value}", interaction.channel_id)
            gaps = missing_perms(interaction.channel, POST_PERMS)
            warn = f"\n⚠️ I'm missing **{', '.join(gaps)}** here — the recap will fail until that's fixed." if gaps else ""
            await interaction.response.send_message(
                f"✅ Nightly **{sport.value}** recap will post in {interaction.channel.mention}.{warn}")

        self.tree.add_command(app_commands.Command(
            name="setrecapchannel",
            description="ADMIN: post a sport's nightly recap in this channel",
            callback=_setrecap))

        # ---- /recapnow [date]
        @app_commands.choices(sport=sport_choices)
        @app_commands.describe(date="Optional: YYYY-MM-DD or MM/DD. Defaults to today.")
        async def _recapnow(interaction: discord.Interaction,
                            sport: app_commands.Choice[str],
                            date: str = None):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            date_str = parse_date_arg(date) if date else _et_date_str(0)
            if date_str is None:
                await interaction.response.send_message(
                    f"Couldn't read the date `{date}`. Use YYYY-MM-DD or MM/DD.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            status = await run_recap(self, interaction.guild_id, sport.value, date_str)
            await interaction.followup.send(
                f"**{sport.value}** recap for {date_str}: {status}", ephemeral=True)

        self.tree.add_command(app_commands.Command(
            name="recapnow",
            description="ADMIN: scan, grade and post a day's recap right now",
            callback=_recapnow))

        # ---- /logplay
        @app_commands.choices(sport=sport_choices)
        @app_commands.describe(
            play="The play as you'd post it: Risk 1 To Win 1u: Player (Team) OVER 5.5 Ks",
            date="Optional: YYYY-MM-DD or MM/DD for a missed day. Defaults to today.")
        async def _logplay(interaction: discord.Interaction,
                           sport: app_commands.Choice[str],
                           play: str,
                           date: str = None):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            date_str = parse_date_arg(date) if date else _et_date_str(0)
            if date_str is None:
                await interaction.response.send_message(
                    f"Couldn't read the date `{date}`. Use YYYY-MM-DD or MM/DD.", ephemeral=True)
                return
            if date_str > _et_date_str(0):
                await interaction.response.send_message(
                    f"{date_str} is in the future — nothing to grade.", ephemeral=True)
                return
            parsed = parse_play(play)
            if parsed is None:
                await interaction.response.send_message(
                    "❌ Couldn't read that play, so I didn't log anything.\n"
                    "Format: `Risk 1.02 To Win 1u: Tyler Phillips (Marlins) UNDER 14.5 Outs`",
                    ephemeral=True)
                return

            dupe = find_duplicate(interaction.guild_id, sport.value, date_str, parsed)
            if dupe:
                await interaction.response.send_message(
                    f"⚠️ Already in the ledger for {date_str}: **{dupe['player']} {dupe['side']} "
                    f"{dupe['point']:g} {dupe['market']}** (status: {dupe['status']}, "
                    f"{(dupe['profit'] or 0):+.2f}u). Nothing added — one play, counted once.",
                    ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            mid = f"manual-{interaction.id}"
            with _conn() as c:
                c.execute(
                    "INSERT OR IGNORE INTO plays (message_id, guild_id, sport, post_date, raw, "
                    "player, team, side, point, market, risk, to_win, source) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'manual')",
                    (mid, str(interaction.guild_id), sport.value, date_str, play[:300],
                     parsed["player"], parsed["team"], parsed["side"], parsed["point"],
                     parsed["market"], parsed["risk"], parsed["to_win"]))

            verdict = "stored as pending"
            if sport.value in GRADED_SPORTS:
                row = dict(parsed)
                status, actual = await asyncio.to_thread(
                    GRADERS[sport.value], row, date_str, {})
                profit = (parsed["to_win"] if status == "win"
                          else (-parsed["risk"] if status == "loss" else 0))
                with _conn() as c:
                    c.execute("UPDATE plays SET status=?, actual=?, profit=? WHERE message_id=?",
                              (status, actual, profit, mid))
                if status in COUNTED:
                    act = f" (actual: {actual:g})" if actual is not None else ""
                    verdict = f"graded **{status}**{act} — {profit:+.2f}u"
                elif status == "ungraded":
                    verdict = "stored, but I don't know that market — it'll show as couldn't-grade"
                else:
                    verdict = "stored as pending (game not final or player not found yet)"

            await interaction.followup.send(
                f"📝 Logged for **{date_str}**: {parsed['player']} ({parsed['team']}) "
                f"{parsed['side']} {parsed['point']:g} {parsed['market']} — "
                f"risk {parsed['risk']:g}u to win {parsed['to_win']:g}u.\n{verdict}\n"
                f"Run `/recapnow {sport.value.lower()} date:{date_str}` to repost that day's recap.",
                ephemeral=True)

        self.tree.add_command(app_commands.Command(
            name="logplay",
            description="ADMIN: manually log a play, optionally for a past date",
            callback=_logplay))

        # ---- /resetledger
        @app_commands.choices(sport=sport_choices)
        async def _resetledger(interaction: discord.Interaction,
                               sport: app_commands.Choice[str]):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            with _conn() as c:
                row = c.execute(
                    "SELECT COUNT(*) n, SUM(profit) p FROM plays WHERE guild_id=? AND sport=?",
                    (str(interaction.guild_id), sport.value)).fetchone()
            n, p = row["n"] or 0, row["p"] or 0
            if n == 0:
                await interaction.response.send_message(
                    f"No {sport.value} plays stored in this server — nothing to reset.",
                    ephemeral=True)
                return
            await interaction.response.send_message(
                f"⚠️ This deletes **all {n} {sport.value} plays** in "
                f"**{interaction.guild.name}** ({p:+.2f}u of record). This cannot be undone. "
                f"Other servers are untouched.",
                view=ConfirmReset(interaction.user.id, interaction.guild_id, sport.value, n),
                ephemeral=True)

        self.tree.add_command(app_commands.Command(
            name="resetledger",
            description="ADMIN: wipe this server's plays for one sport (asks to confirm)",
            callback=_resetledger))

        # ---- /record
        @app_commands.choices(sport=sport_choices)
        async def _record(interaction: discord.Interaction,
                          sport: app_commands.Choice[str]):
            wins, losses, pushes, profit, risked = ledger_totals(interaction.guild_id, sport.value)
            roi = f"{profit / risked * 100:+.2f}%" if risked else "—"
            await interaction.response.send_message(
                f"**{sport.value} YTD — {interaction.guild.name}**: {wins}-{losses}"
                + (f"-{pushes}" if pushes else "")
                + f" | {profit:+.2f}u | ROI {roi}")

        self.tree.add_command(app_commands.Command(
            name="record",
            description="Show this server's season record, units and ROI for a sport",
            callback=_record))

        try:
            synced = await self.tree.sync()
            log.info("Synced %d slash commands", len(synced))
        except Exception as e:
            log.error("Slash command sync failed: %s", e)

    async def on_ready(self):
        log.info("Logged in as %s", self.user)
        if not self._migrated:
            try:
                await migrate_legacy_guild(self)
            except Exception as e:
                log.error("Legacy migration failed (data left untouched): %s", e)
            self._migrated = True
        for gid, sport in configured_pairs():
            guild = self.get_guild(int(gid))
            log.info("Watching %s in %s", sport, guild.name if guild else f"guild {gid}")
        if not nightly_recap.is_running():
            nightly_recap.start(self)


def ledger_totals(guild_id, sport):
    with _conn() as c:
        rows = c.execute(
            "SELECT status, COUNT(*) n, SUM(profit) p, SUM(risk) r FROM plays "
            "WHERE guild_id=? AND sport=? AND status IN ('win','loss','push') GROUP BY status",
            (str(guild_id), sport)).fetchall()
    wins = sum(r["n"] for r in rows if r["status"] == "win")
    losses = sum(r["n"] for r in rows if r["status"] == "loss")
    pushes = sum(r["n"] for r in rows if r["status"] == "push")
    profit = sum(r["p"] or 0 for r in rows)
    risked = sum(r["r"] or 0 for r in rows)
    return wins, losses, pushes, profit, risked


# ---------------------------------------------------------------- scanning
async def scan_channel_for_plays(bot, guild_id, sport: str, date_str: str):
    """Stores new parseable plays posted on date_str (ET) in this server's
    picks channel. Returns (new_count, duplicate_count, permission_error)."""
    channel_id = get_config(guild_id, f"picks_channel_{sport}")
    if not channel_id:
        return 0, 0, None
    channel = bot.get_channel(int(channel_id))
    if channel is None:
        msg = f"picks channel {channel_id} not visible to the bot"
        log.error("%s scan: %s", sport, msg)
        return 0, 0, msg

    gaps = missing_perms(channel, READ_PERMS)
    if gaps:
        msg = f"missing {', '.join(gaps)} in #{getattr(channel, 'name', channel_id)}"
        log.error("%s scan blocked: %s", sport, msg)
        return 0, 0, msg

    start_utc, end_utc = _et_day_bounds_utc(date_str)
    new = dupes = 0
    try:
        async for msg_obj in channel.history(after=start_utc, before=end_utc, limit=500):
            if msg_obj.author.bot:
                continue
            content = msg_obj.content or ""
            play = parse_play(content)
            if play is None:
                # Keep a visible trace of anything unreadable, including a
                # screenshot posted with no readable line -- never silently dropped.
                has_media = bool(getattr(msg_obj, "attachments", None) or
                                 getattr(msg_obj, "embeds", None))
                if content.strip() or has_media:
                    raw = content[:300] if content.strip() else "[image/attachment, no text]"
                    with _conn() as c:
                        c.execute(
                            "INSERT OR IGNORE INTO plays (message_id, guild_id, sport, post_date, "
                            "raw, status) VALUES (?,?,?,?,?, 'ungraded')",
                            (str(msg_obj.id), str(guild_id), sport, date_str, raw))
                continue

            existing = find_duplicate(guild_id, sport, date_str, play)
            status = "duplicate" if (existing and str(existing["message_id"]) != str(msg_obj.id)) else "pending"
            with _conn() as c:
                cur = c.execute(
                    "INSERT OR IGNORE INTO plays (message_id, guild_id, sport, post_date, raw, "
                    "player, team, side, point, market, risk, to_win, status) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (str(msg_obj.id), str(guild_id), sport, date_str, content[:300],
                     play["player"], play["team"], play["side"], play["point"],
                     play["market"], play["risk"], play["to_win"], status))
            if cur.rowcount:
                if status == "duplicate":
                    dupes += 1
                    log.info("%s: repeat of %s %s %g %s — stored but not counted",
                             sport, play["player"], play["side"], play["point"], play["market"])
                else:
                    new += 1
    except discord.Forbidden as e:
        msg = f"Discord refused history in #{getattr(channel, 'name', channel_id)} ({e})"
        log.error("%s scan blocked: %s", sport, msg)
        return new, dupes, msg
    return new, dupes, None


def _fit_lines(lines, limit=1024):
    """Whole lines only, with an honest overflow count."""
    out, used = [], 0
    for i, line in enumerate(lines):
        if used + len(line) + 1 > limit - 20 and out:
            out.append(f"…+{len(lines) - i} more")
            break
        out.append(line)
        used += len(line) + 1
    return "\n".join(out) if out else "—"


async def run_recap(bot, guild_id, sport: str, date_str: str) -> str:
    guild_id = str(guild_id)
    _, dupes, scan_error = await scan_channel_for_plays(bot, guild_id, sport, date_str)

    with _conn() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT * FROM plays WHERE guild_id=? AND sport=? AND post_date=?",
            (guild_id, sport, date_str)).fetchall()]
    if not rows:
        return (f"no plays found for {date_str}"
                + (f" — SCAN BLOCKED: {scan_error}" if scan_error else ""))

    if sport in GRADED_SPORTS:
        grader = GRADERS[sport]
        cache = {}
        for r in rows:
            if r["status"] != "pending" or not r["player"]:
                continue
            status, actual = await asyncio.to_thread(grader, r, date_str, cache)
            profit = r["to_win"] if status == "win" else (-r["risk"] if status == "loss" else 0)
            with _conn() as c:
                c.execute("UPDATE plays SET status=?, actual=?, profit=? WHERE message_id=?",
                          (status, actual, profit, r["message_id"]))
            r.update(status=status, actual=actual, profit=profit)

    graded = [r for r in rows if r["status"] in COUNTED]
    pending = [r for r in rows if r["status"] == "pending"]
    ungraded = [r for r in rows if r["status"] == "ungraded"]
    repeats = [r for r in rows if r["status"] == "duplicate"]
    wins = sum(1 for r in graded if r["status"] == "win")
    losses = sum(1 for r in graded if r["status"] == "loss")
    day_profit = sum(r["profit"] or 0 for r in graded)

    _, _, _, ytd, risked = ledger_totals(guild_id, sport)
    roi = f"{ytd / risked * 100:+.2f}%" if risked else "—"

    mm, dd, yy = date_str[5:7].lstrip("0"), date_str[8:10].lstrip("0"), date_str[2:4]
    embed = discord.Embed(
        title=f"{sport} Recap — {mm}/{dd}/{yy} | {day_profit:+.2f}u",
        description=f"**{wins}-{losses} today** ({len(graded)} bet{'s' if len(graded) != 1 else ''})",
        color=discord.Color.green() if day_profit >= 0 else discord.Color.red(),
    )
    lines = []
    for r in graded:
        mark = {"win": "✅", "loss": "❌", "push": "➖"}[r["status"]]
        actual = f" (actual: {r['actual']:g})" if r["actual"] is not None else ""
        manual = " 📝" if r.get("source") == "manual" else ""
        lines.append(f"{mark} {r['player']} {r['side']} {r['point']:g} "
                     f"{r['market']}{actual} — {r['profit']:+.2f}u{manual}")
    if lines:
        embed.add_field(name="Plays", value=_fit_lines(lines), inline=False)
    if pending:
        embed.add_field(name="⏳ Still pending (game not final / player not found)",
                        value=_fit_lines([f"{r['player']} {r['side']} {r['point']:g} {r['market']}"
                                          for r in pending]), inline=False)
    if ungraded:
        embed.add_field(name="🔎 Couldn't grade — check manually",
                        value=_fit_lines([(r["raw"] or "?")[:80] for r in ungraded]), inline=False)
    if repeats:
        embed.add_field(name="🔁 Repeat posts (counted once)",
                        value=_fit_lines([f"{r['player']} {r['side']} {r['point']:g} {r['market']}"
                                          for r in repeats]), inline=False)
    embed.add_field(name="💰 Units YTD", value=f"{ytd:+.2f}u", inline=True)
    embed.add_field(name="📈 ROI", value=roi, inline=True)
    if scan_error:
        embed.add_field(name="⚠️ Couldn't read the picks channel",
                        value=f"{scan_error} — plays posted today may be missing.", inline=False)

    # ---- delivery, with the permission failure named instead of swallowed
    recap_id = get_config(guild_id, f"recap_channel_{sport}")
    picks_id = get_config(guild_id, f"picks_channel_{sport}")
    attempts, problems = [], []
    for cid, label in ((recap_id, "recap channel"), (picks_id, "picks channel")):
        if cid and cid not in [a[0] for a in attempts]:
            attempts.append((cid, label))
    if not attempts:
        return "no channel configured for this server"

    for cid, label in attempts:
        channel = bot.get_channel(int(cid))
        if channel is None:
            problems.append(f"{label} {cid} not visible to the bot")
            continue
        gaps = missing_perms(channel, POST_PERMS)
        if gaps:
            problems.append(f"#{getattr(channel, 'name', cid)} missing {', '.join(gaps)}")
            continue
        if problems:
            # We're only here because the intended channel wouldn't take it.
            embed.add_field(name="⚠️ Posted here as a fallback",
                            value="; ".join(problems), inline=False)
        try:
            await channel.send(embed=embed)
        except discord.Forbidden as e:
            problems.append(f"#{getattr(channel, 'name', cid)} refused the post ({e})")
            continue
        extra = f", {len(repeats)} repeat" if repeats else ""
        fallback = f" — FELL BACK from the recap channel: {'; '.join(problems)}" if problems else ""
        if problems:
            log.error("%s recap for %s: %s", sport, date_str, "; ".join(problems))
        return (f"posted in #{getattr(channel, 'name', cid)} — {wins}-{losses}, "
                f"{day_profit:+.2f}u, {len(pending)} pending, "
                f"{len(ungraded)} unreadable{extra}{fallback}")

    log.error("%s recap for %s could not be delivered: %s", sport, date_str, "; ".join(problems))
    return "GRADED BUT NOT POSTED — " + "; ".join(problems)


# 12:30 AM Eastern, year-round -- recaps the ET day that just ended
@tasks.loop(time=dtime(hour=0, minute=30, tzinfo=ET))
async def nightly_recap(bot):
    date_str = _et_date_str(-1)
    pairs = configured_pairs()
    if not pairs:
        log.info("Nightly recap: nothing configured yet")
        return
    for guild_id, sport in pairs:
        guild = bot.get_guild(int(guild_id))
        name = guild.name if guild else f"guild {guild_id}"
        try:
            status = await run_recap(bot, guild_id, sport, date_str)
            log.info("[%s] %s nightly recap: %s", name, sport, status)
        except Exception as e:
            log.error("[%s] %s nightly recap failed: %s", name, sport, e)


@nightly_recap.before_loop
async def _before():
    await client.wait_until_ready()


client = RecapBot()

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_TOKEN")
    client.run(TOKEN)
